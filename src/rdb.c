/**
 * RDB持久化
 *
 *   bgsave命令执行[会创建子进程去执行,阻塞只发生在创建子进程的过程中]
 *
 *     自动触发
 *
 *        在配置文件中配置save操作,例如save m n[在m秒内触发了n次修改操作则进行RDB更新]
 *
 *        主从复制,如果从节点进行全量复制会强制父节点执行RDB持久化生成RDB文件传送给从节点
 *
 *        执行debug reload命令重新加载Redis
 *
 *        未配置AOF情况下执行shutdown命令时会自动触发bgsave
 *
 *   主要流程
 *
 *      1、fork()[返回两次,子进程返回pid=0,父进程返回pid>0]创建子进程[此阶段会阻塞redis服务器],创建失败父进程直接返回错误,否则父进程继续处理其它请求
 *
 *      2、子进程执行持久化,通过遍历整个redis数据库获取相应的key和value等信息写入文件中进行调用fsync[同步I/O]进行落盘
 *
 *      3、父进程等待子进程执行完成[在serverCron时间事件中等待,子进程完成会通知父进程],父进程记录此次RDB子进程执行情况更新统计信息
 *
 * RDB是一个紧凑压缩的二进制文件,代表Redis在某个时间点上的数据快照,非常适用于备份,全量复制等场景
 *
 * Redis加载RDB恢复数据远远快于AOF的方式
 *
 * RDB方式数据没办法做到实时持久化/秒级持久化,因为bgsave每次运行都要执行fork操作创建子进程,属于重量级操作,频繁执行成本过高
 *
 * RDB文件使用特定二进制格式保存,Redis版本演进过程中有多个格式的RDB版本,存在老版本Redis服务无法兼容新版RDB格式的问题
 *
 * redis-check-dump
 *
 * @param c 客户端
 */

#include "server.h"
#include "lzf.h"
#include "zipmap.h"
#include "endianconv.h"

#include <math.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <sys/param.h>

#define rdbExitReportCorruptRDB(...) rdbCheckThenExit(__LINE__,__VA_ARGS__)

extern int rdbCheckMode;

void rdbCheckError(
        const char *fmt, ...);

void rdbCheckSetError(
        const char *fmt, ...);

void rdbCheckThenExit(
        int linenum,
        char *reason, ...) {

    va_list ap;
    char msg[1024];
    int len;

    len = snprintf(msg, sizeof(msg),
                   "Internal error in RDB reading function at rdb.c:%d -> ", linenum);
    va_start(ap, reason);
    vsnprintf(msg + len, sizeof(msg) - len, reason, ap);
    va_end(ap);

    if (!rdbCheckMode) {
        serverLog(LL_WARNING, "%s", msg);
        char *argv[2] = {"", server.rdb_filename};
        redis_check_rdb_main(2, argv, NULL);
    } else {
        rdbCheckError("%s", msg);
    }
    exit(1);
}

static int rdbWriteRaw(
        rio *rdb,
        void *p,
        size_t len) {

    if (rdb && rioWrite(rdb, p, len) == 0)
        return -1;
    return len;
}

int rdbSaveType(
        rio *rdb,
        unsigned char type) {

    return rdbWriteRaw(rdb, &type, 1);
}

int rdbLoadType(
        rio *rdb) {

    unsigned char type;
    if (rioRead(rdb, &type, 1) == 0) return -1;
    return type;
}

time_t rdbLoadTime(
        rio *rdb) {

    int32_t t32;
    if (rioRead(rdb, &t32, 4) == 0) return -1;
    return (time_t) t32;
}

int rdbSaveMillisecondTime(
        rio *rdb,
        long long t) {

    int64_t t64 = (int64_t) t;
    return rdbWriteRaw(rdb, &t64, 8);
}

long long rdbLoadMillisecondTime(
        rio *rdb) {

    int64_t t64;
    if (rioRead(rdb, &t64, 8) == 0) return -1;
    return (long long) t64;
}

int rdbSaveLen(
        rio *rdb,
        uint64_t len) {

    unsigned char buf[2];
    size_t nwritten;

    if (len < (1 << 6)) {
        /* Save a 6 bit len */
        buf[0] = (len & 0xFF) | (RDB_6BITLEN << 6);
        if (rdbWriteRaw(rdb, buf, 1) == -1) return -1;
        nwritten = 1;
    } else if (len < (1 << 14)) {
        /* Save a 14 bit len */
        buf[0] = ((len >> 8) & 0xFF) | (RDB_14BITLEN << 6);
        buf[1] = len & 0xFF;
        if (rdbWriteRaw(rdb, buf, 2) == -1) return -1;
        nwritten = 2;
    } else if (len <= UINT32_MAX) {
        /* Save a 32 bit len */
        buf[0] = RDB_32BITLEN;
        if (rdbWriteRaw(rdb, buf, 1) == -1) return -1;
        uint32_t len32 = htonl(len);
        if (rdbWriteRaw(rdb, &len32, 4) == -1) return -1;
        nwritten = 1 + 4;
    } else {
        /* Save a 64 bit len */
        buf[0] = RDB_64BITLEN;
        if (rdbWriteRaw(rdb, buf, 1) == -1) return -1;
        len = htonu64(len);
        if (rdbWriteRaw(rdb, &len, 8) == -1) return -1;
        nwritten = 1 + 8;
    }
    return nwritten;
}

int rdbLoadLenByRef(
        rio *rdb,
        int *isencoded,
        uint64_t *lenptr) {

    unsigned char buf[2];
    int type;

    if (isencoded) *isencoded = 0;
    if (rioRead(rdb, buf, 1) == 0) return -1;
    type = (buf[0] & 0xC0) >> 6;
    if (type == RDB_ENCVAL) {
        /* Read a 6 bit encoding type. */
        if (isencoded) *isencoded = 1;
        *lenptr = buf[0] & 0x3F;
    } else if (type == RDB_6BITLEN) {
        /* Read a 6 bit len. */
        *lenptr = buf[0] & 0x3F;
    } else if (type == RDB_14BITLEN) {
        /* Read a 14 bit len. */
        if (rioRead(rdb, buf + 1, 1) == 0) return -1;
        *lenptr = ((buf[0] & 0x3F) << 8) | buf[1];
    } else if (buf[0] == RDB_32BITLEN) {
        /* Read a 32 bit len. */
        uint32_t len;
        if (rioRead(rdb, &len, 4) == 0) return -1;
        *lenptr = ntohl(len);
    } else if (buf[0] == RDB_64BITLEN) {
        /* Read a 64 bit len. */
        uint64_t len;
        if (rioRead(rdb, &len, 8) == 0) return -1;
        *lenptr = ntohu64(len);
    } else {
        rdbExitReportCorruptRDB(
                "Unknown length encoding %d in rdbLoadLen()", type);
        return -1; /* Never reached. */
    }
    return 0;
}

uint64_t rdbLoadLen(
        rio *rdb,
        int *isencoded) {
    uint64_t len;

    if (rdbLoadLenByRef(rdb, isencoded, &len) == -1) return RDB_LENERR;
    return len;
}

int rdbEncodeInteger(
        long long value,
        unsigned char *enc) {

    if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT8;
        enc[1] = value & 0xFF;
        return 2;
    } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT16;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        return 3;
    } else if (value >= -((long long) 1 << 31) && value <= ((long long) 1 << 31) - 1) {
        enc[0] = (RDB_ENCVAL << 6) | RDB_ENC_INT32;
        enc[1] = value & 0xFF;
        enc[2] = (value >> 8) & 0xFF;
        enc[3] = (value >> 16) & 0xFF;
        enc[4] = (value >> 24) & 0xFF;
        return 5;
    } else {
        return 0;
    }
}

void *rdbLoadIntegerObject(
        rio *rdb,
        int enctype,
        int flags,
        size_t *lenptr) {

    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int encode = flags & RDB_LOAD_ENC;
    unsigned char enc[4];
    long long val;

    if (enctype == RDB_ENC_INT8) {
        if (rioRead(rdb, enc, 1) == 0) return NULL;
        val = (signed char) enc[0];
    } else if (enctype == RDB_ENC_INT16) {
        uint16_t v;
        if (rioRead(rdb, enc, 2) == 0) return NULL;
        v = enc[0] | (enc[1] << 8);
        val = (int16_t) v;
    } else if (enctype == RDB_ENC_INT32) {
        uint32_t v;
        if (rioRead(rdb, enc, 4) == 0) return NULL;
        v = enc[0] | (enc[1] << 8) | (enc[2] << 16) | (enc[3] << 24);
        val = (int32_t) v;
    } else {
        val = 0; /* anti-warning */
        rdbExitReportCorruptRDB("Unknown RDB integer encoding type %d", enctype);
    }
    if (plain || sds) {
        char buf[LONG_STR_SIZE], *p;
        int len = ll2string(buf, sizeof(buf), val);
        if (lenptr) *lenptr = len;
        p = plain ? zmalloc(len) : sdsnewlen(NULL, len);
        memcpy(p, buf, len);
        return p;
    } else if (encode) {
        return createStringObjectFromLongLong(val);
    } else {
        return createObject(OBJ_STRING, sdsfromlonglong(val));
    }
}

int rdbTryIntegerEncoding(
        char *s,
        size_t len,
        unsigned char *enc) {

    long long value;
    char *endptr, buf[32];

    /* Check if it's possible to encode this value as a number */
    value = strtoll(s, &endptr, 10);
    if (endptr[0] != '\0') return 0;
    ll2string(buf, 32, value);

    /* If the number converted back into a string is not identical
     * then it's not possible to encode the string as integer */
    if (strlen(buf) != len || memcmp(buf, s, len)) return 0;

    return rdbEncodeInteger(value, enc);
}

ssize_t rdbSaveLzfBlob(
        rio *rdb,
        void *data,
        size_t compress_len,
        size_t original_len) {

    unsigned char byte;
    ssize_t n, nwritten = 0;

    /* Data compressed! Let's save it on disk */
    byte = (RDB_ENCVAL << 6) | RDB_ENC_LZF;
    if ((n = rdbWriteRaw(rdb, &byte, 1)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb, compress_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbSaveLen(rdb, original_len)) == -1) goto writeerr;
    nwritten += n;

    if ((n = rdbWriteRaw(rdb, data, compress_len)) == -1) goto writeerr;
    nwritten += n;

    return nwritten;

    writeerr:
    return -1;
}

ssize_t rdbSaveLzfStringObject(
        rio *rdb,
        unsigned char *s,
        size_t len) {

    size_t comprlen, outlen;
    void *out;

    /* We require at least four bytes compression for this to be worth it */
    if (len <= 4) return 0;
    outlen = len - 4;
    if ((out = zmalloc(outlen + 1)) == NULL) return 0;
    comprlen = lzf_compress(s, len, out, outlen);
    if (comprlen == 0) {
        zfree(out);
        return 0;
    }
    ssize_t nwritten = rdbSaveLzfBlob(rdb, out, comprlen, len);
    zfree(out);
    return nwritten;
}

void *rdbLoadLzfStringObject(
        rio *rdb,
        int flags,
        size_t *lenptr) {

    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    uint64_t len, clen;
    unsigned char *c = NULL;
    char *val = NULL;

    if ((clen = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;
    if ((len = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;
    if ((c = zmalloc(clen)) == NULL) goto err;

    /* Allocate our target according to the uncompressed size. */
    if (plain) {
        val = zmalloc(len);
        if (lenptr) *lenptr = len;
    } else {
        val = sdsnewlen(NULL, len);
    }

    /* Load the compressed representation and uncompress it to target. */
    if (rioRead(rdb, c, clen) == 0) goto err;
    if (lzf_decompress(c, clen, val, len) == 0) {
        if (rdbCheckMode) rdbCheckSetError("Invalid LZF compressed string");
        goto err;
    }
    zfree(c);

    if (plain || sds) {
        return val;
    } else {
        return createObject(OBJ_STRING, val);
    }
    err:
    zfree(c);
    if (plain)
        zfree(val);
    else
        sdsfree(val);
    return NULL;
}

ssize_t rdbSaveRawString(
        rio *rdb,
        unsigned char *s,
        size_t len) {

    int enclen;
    ssize_t n, nwritten = 0;

    /* Try integer encoding */
    if (len <= 11) {
        unsigned char buf[5];
        if ((enclen = rdbTryIntegerEncoding((char *) s, len, buf)) > 0) {
            if (rdbWriteRaw(rdb, buf, enclen) == -1) return -1;
            return enclen;
        }
    }

    /* Try LZF compression - under 20 bytes it's unable to compress even
     * aaaaaaaaaaaaaaaaaa so skip it */
    if (server.rdb_compression && len > 20) {
        n = rdbSaveLzfStringObject(rdb, s, len);
        if (n == -1) return -1;
        if (n > 0) return n;
        /* Return value of 0 means data can't be compressed, save the old way */
    }

    /* Store verbatim */
    if ((n = rdbSaveLen(rdb, len)) == -1) return -1;
    nwritten += n;
    if (len > 0) {
        if (rdbWriteRaw(rdb, s, len) == -1) return -1;
        nwritten += len;
    }
    return nwritten;
}

ssize_t rdbSaveLongLongAsStringObject(
        rio *rdb,
        long long value) {

    unsigned char buf[32];
    ssize_t n, nwritten = 0;
    int enclen = rdbEncodeInteger(value, buf);
    if (enclen > 0) {
        return rdbWriteRaw(rdb, buf, enclen);
    } else {
        /* Encode as string */
        enclen = ll2string((char *) buf, 32, value);
        serverAssert(enclen < 32);
        if ((n = rdbSaveLen(rdb, enclen)) == -1) return -1;
        nwritten += n;
        if ((n = rdbWriteRaw(rdb, buf, enclen)) == -1) return -1;
        nwritten += n;
    }
    return nwritten;
}

int rdbSaveStringObject(
        rio *rdb,
        robj *obj) {

    if (obj->encoding == OBJ_ENCODING_INT) {
        return rdbSaveLongLongAsStringObject(rdb, (long) obj->ptr);
    } else {
        serverAssertWithInfo(NULL, obj, sdsEncodedObject(obj));
        return rdbSaveRawString(rdb, obj->ptr, sdslen(obj->ptr));
    }
}

void *rdbGenericLoadStringObject(
        rio *rdb,
        int flags,
        size_t *lenptr) {

    int encode = flags & RDB_LOAD_ENC;
    int plain = flags & RDB_LOAD_PLAIN;
    int sds = flags & RDB_LOAD_SDS;
    int isencoded;
    uint64_t len;

    len = rdbLoadLen(rdb, &isencoded);
    if (isencoded) {
        switch (len) {
            case RDB_ENC_INT8:
            case RDB_ENC_INT16:
            case RDB_ENC_INT32:
                return rdbLoadIntegerObject(rdb, len, flags, lenptr);
            case RDB_ENC_LZF:
                return rdbLoadLzfStringObject(rdb, flags, lenptr);
            default:
                rdbExitReportCorruptRDB("Unknown RDB string encoding type %d", len);
        }
    }

    if (len == RDB_LENERR) return NULL;
    if (plain || sds) {
        void *buf = plain ? zmalloc(len) : sdsnewlen(NULL, len);
        if (lenptr) *lenptr = len;
        if (len && rioRead(rdb, buf, len) == 0) {
            if (plain)
                zfree(buf);
            else
                sdsfree(buf);
            return NULL;
        }
        return buf;
    } else {
        robj *o = encode ? createStringObject(NULL, len) :
                  createRawStringObject(NULL, len);
        if (len && rioRead(rdb, o->ptr, len) == 0) {
            decrRefCount(o);
            return NULL;
        }
        return o;
    }
}

robj *rdbLoadStringObject(
        rio *rdb) {

    return rdbGenericLoadStringObject(rdb, RDB_LOAD_NONE, NULL);
}

robj *rdbLoadEncodedStringObject(
        rio *rdb) {

    return rdbGenericLoadStringObject(rdb, RDB_LOAD_ENC, NULL);
}

int rdbLoadDoubleValue(
        rio *rdb,
        double *val) {

    char buf[256];
    unsigned char len;

    if (rioRead(rdb, &len, 1) == 0) return -1;
    switch (len) {
        case 255:
            *val = R_NegInf;
            return 0;
        case 254:
            *val = R_PosInf;
            return 0;
        case 253:
            *val = R_Nan;
            return 0;
        default:
            if (rioRead(rdb, buf, len) == 0) return -1;
            buf[len] = '\0';
            sscanf(buf, "%lg", val);
            return 0;
    }
}

int rdbSaveBinaryDoubleValue(
        rio *rdb,
        double val) {

    memrev64ifbe(&val);
    return rdbWriteRaw(rdb, &val, sizeof(val));
}

int rdbLoadBinaryDoubleValue(
        rio *rdb,
        double *val) {

    if (rioRead(rdb, val, sizeof(*val)) == 0) return -1;
    memrev64ifbe(val);
    return 0;
}

int rdbSaveBinaryFloatValue(
        rio *rdb,
        float val) {

    memrev32ifbe(&val);
    return rdbWriteRaw(rdb, &val, sizeof(val));
}

int rdbLoadBinaryFloatValue(
        rio *rdb,
        float *val) {

    if (rioRead(rdb, val, sizeof(*val)) == 0) return -1;
    memrev32ifbe(val);
    return 0;
}

int rdbSaveObjectType(
        rio *rdb,
        robj *o) {

    switch (o->type) {
        case OBJ_STRING:
            return rdbSaveType(rdb, RDB_TYPE_STRING);
        case OBJ_LIST:
            if (o->encoding == OBJ_ENCODING_QUICKLIST)
                return rdbSaveType(rdb, RDB_TYPE_LIST_QUICKLIST);
            else
                serverPanic("Unknown list encoding");
        case OBJ_SET:
            if (o->encoding == OBJ_ENCODING_INTSET)
                return rdbSaveType(rdb, RDB_TYPE_SET_INTSET);
            else if (o->encoding == OBJ_ENCODING_HT)
                return rdbSaveType(rdb, RDB_TYPE_SET);
            else
                serverPanic("Unknown set encoding");
        case OBJ_ZSET:
            if (o->encoding == OBJ_ENCODING_ZIPLIST)
                return rdbSaveType(rdb, RDB_TYPE_ZSET_ZIPLIST);
            else if (o->encoding == OBJ_ENCODING_SKIPLIST)
                return rdbSaveType(rdb, RDB_TYPE_ZSET_2);
            else
                serverPanic("Unknown sorted set encoding");
        case OBJ_HASH:
            if (o->encoding == OBJ_ENCODING_ZIPLIST)
                return rdbSaveType(rdb, RDB_TYPE_HASH_ZIPLIST);
            else if (o->encoding == OBJ_ENCODING_HT)
                return rdbSaveType(rdb, RDB_TYPE_HASH);
            else
                serverPanic("Unknown hash encoding");
        case OBJ_MODULE:
            return rdbSaveType(rdb, RDB_TYPE_MODULE_2);
        default:
            serverPanic("Unknown object type");
    }
    return -1; /* avoid warning */
}

int rdbLoadObjectType(
        rio *rdb) {

    int type;
    if ((type = rdbLoadType(rdb)) == -1) return -1;
    if (!rdbIsObjectType(type)) return -1;
    return type;
}

ssize_t rdbSaveObject(
        rio *rdb,
        robj *o) {

    ssize_t n = 0, nwritten = 0;

    if (o->type == OBJ_STRING) {
        /* Save a string value */
        if ((n = rdbSaveStringObject(rdb, o)) == -1) return -1;
        nwritten += n;
    } else if (o->type == OBJ_LIST) {
        /* Save a list value */
        if (o->encoding == OBJ_ENCODING_QUICKLIST) {
            quicklist *ql = o->ptr;
            quicklistNode *node = ql->head;

            if ((n = rdbSaveLen(rdb, ql->len)) == -1) return -1;
            nwritten += n;

            do {
                if (quicklistNodeIsCompressed(node)) {
                    void *data;
                    size_t compress_len = quicklistGetLzf(node, &data);
                    if ((n = rdbSaveLzfBlob(rdb, data, compress_len, node->sz)) == -1) return -1;
                    nwritten += n;
                } else {
                    if ((n = rdbSaveRawString(rdb, node->zl, node->sz)) == -1) return -1;
                    nwritten += n;
                }
            } while ((node = node->next));
        } else {
            serverPanic("Unknown list encoding");
        }
    } else if (o->type == OBJ_SET) {
        /* Save a set value */
        if (o->encoding == OBJ_ENCODING_HT) {
            dict *set = o->ptr;
            dictIterator *di = dictGetIterator(set);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb, dictSize(set))) == -1) return -1;
            nwritten += n;

            while ((de = dictNext(di)) != NULL) {
                sds ele = dictGetKey(de);
                if ((n = rdbSaveRawString(rdb, (unsigned char *) ele, sdslen(ele)))
                    == -1)
                    return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else if (o->encoding == OBJ_ENCODING_INTSET) {
            size_t l = intsetBlobLen((intset *) o->ptr);

            if ((n = rdbSaveRawString(rdb, o->ptr, l)) == -1) return -1;
            nwritten += n;
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (o->type == OBJ_ZSET) {
        /* Save a sorted set value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char *) o->ptr);

            if ((n = rdbSaveRawString(rdb, o->ptr, l)) == -1) return -1;
            nwritten += n;
        } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = o->ptr;
            zskiplist *zsl = zs->zsl;

            if ((n = rdbSaveLen(rdb, zsl->length)) == -1) return -1;
            nwritten += n;

            /* We save the skiplist elements from the greatest to the smallest
             * (that's trivial since the elements are already ordered in the
             * skiplist): this improves the load process, since the next loaded
             * element will always be the smaller, so adding to the skiplist
             * will always immediately stop at the head, making the insertion
             * O(1) instead of O(log(N)). */
            zskiplistNode *zn = zsl->tail;
            while (zn != NULL) {
                if ((n = rdbSaveRawString(rdb,
                                          (unsigned char *) zn->ele, sdslen(zn->ele))) == -1) {
                    return -1;
                }
                nwritten += n;
                if ((n = rdbSaveBinaryDoubleValue(rdb, zn->score)) == -1)
                    return -1;
                nwritten += n;
                zn = zn->backward;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else if (o->type == OBJ_HASH) {
        /* Save a hash value */
        if (o->encoding == OBJ_ENCODING_ZIPLIST) {
            size_t l = ziplistBlobLen((unsigned char *) o->ptr);

            if ((n = rdbSaveRawString(rdb, o->ptr, l)) == -1) return -1;
            nwritten += n;

        } else if (o->encoding == OBJ_ENCODING_HT) {
            dictIterator *di = dictGetIterator(o->ptr);
            dictEntry *de;

            if ((n = rdbSaveLen(rdb, dictSize((dict *) o->ptr))) == -1) return -1;
            nwritten += n;

            while ((de = dictNext(di)) != NULL) {
                sds field = dictGetKey(de);
                sds value = dictGetVal(de);

                if ((n = rdbSaveRawString(rdb, (unsigned char *) field,
                                          sdslen(field))) == -1)
                    return -1;
                nwritten += n;
                if ((n = rdbSaveRawString(rdb, (unsigned char *) value,
                                          sdslen(value))) == -1)
                    return -1;
                nwritten += n;
            }
            dictReleaseIterator(di);
        } else {
            serverPanic("Unknown hash encoding");
        }

    } else if (o->type == OBJ_MODULE) {
        /* Save a module-specific value. */
        RedisModuleIO io;
        moduleValue *mv = o->ptr;
        moduleType *mt = mv->type;
        moduleInitIOContext(io, mt, rdb);

        /* Write the "module" identifier as prefix, so that we'll be able
         * to call the right module during loading. */
        int retval = rdbSaveLen(rdb, mt->id);
        if (retval == -1) return -1;
        io.bytes += retval;

        /* Then write the module-specific representation + EOF marker. */
        mt->rdb_save(&io, mv->value);
        retval = rdbSaveLen(rdb, RDB_MODULE_OPCODE_EOF);
        if (retval == -1) return -1;
        io.bytes += retval;

        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }
        return io.error ? -1 : (ssize_t) io.bytes;
    } else {
        serverPanic("Unknown object type");
    }
    return nwritten;
}

size_t rdbSavedObjectLen(
        robj *o) {

    ssize_t len = rdbSaveObject(NULL, o);
    serverAssertWithInfo(NULL, o, len != -1);
    return len;
}

/**
 * 保存一个键-值对,包括过期时间、类型、键和值
 *
 * @param rdb 数据库
 * @param key 键
 * @param val 值
 * @param expiretime 过期时间
 * @param now 当前时间
 */
int rdbSaveKeyValuePair(
        rio *rdb,
        robj *key,
        robj *val,
        long long expiretime,
        long long now) {

    // 保存expire信息
    if (expiretime != -1) {
        // 如果过期了直接跳过
        if (expiretime < now) return 0;
        if (rdbSaveType(rdb, RDB_OPCODE_EXPIRETIME_MS) == -1) return -1;
        if (rdbSaveMillisecondTime(rdb, expiretime) == -1) return -1;
    }
    // 保存key和value信息
    if (rdbSaveObjectType(rdb, val) == -1) return -1;
    if (rdbSaveStringObject(rdb, key) == -1) return -1;
    if (rdbSaveObject(rdb, val) == -1) return -1;
    return 1;
}

int rdbSaveAuxField(
        rio *rdb,
        void *key,
        size_t keylen,
        void *val,
        size_t vallen) {

    if (rdbSaveType(rdb, RDB_OPCODE_AUX) == -1) return -1;
    if (rdbSaveRawString(rdb, key, keylen) == -1) return -1;
    if (rdbSaveRawString(rdb, val, vallen) == -1) return -1;
    return 1;
}

int rdbSaveAuxFieldStrStr(
        rio *rdb,
        char *key,
        char *val) {

    return rdbSaveAuxField(rdb, key, strlen(key), val, strlen(val));
}

int rdbSaveAuxFieldStrInt(
        rio *rdb,
        char *key,
        long long val) {

    char buf[LONG_STR_SIZE];
    int vlen = ll2string(buf, sizeof(buf), val);
    return rdbSaveAuxField(rdb, key, strlen(key), buf, vlen);
}

int rdbSaveInfoAuxFields(
        rio *rdb,
        int flags,
        rdbSaveInfo *rsi) {

    int redis_bits = (sizeof(void *) == 8) ? 64 : 32;
    int aof_preamble = (flags & RDB_SAVE_AOF_PREAMBLE) != 0;

    /* Add a few fields about the state when the RDB was created. */
    if (rdbSaveAuxFieldStrStr(rdb, "redis-ver", REDIS_VERSION) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb, "redis-bits", redis_bits) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb, "ctime", time(NULL)) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb, "used-mem", zmalloc_used_memory()) == -1) return -1;

    /* Handle saving options that generate aux fields. */
    if (rsi) {
        if (rsi->repl_stream_db &&
            rdbSaveAuxFieldStrInt(rdb, "repl-stream-db", rsi->repl_stream_db)
            == -1) {
            return -1;
        }
    }
    if (rdbSaveAuxFieldStrInt(rdb, "aof-preamble", aof_preamble) == -1) return -1;
    if (rdbSaveAuxFieldStrStr(rdb, "repl-id", server.replid) == -1) return -1;
    if (rdbSaveAuxFieldStrInt(rdb, "repl-offset", server.master_repl_offset) == -1) return -1;
    return 1;
}

/**
 * 生成RDB格式的数据库转储,将其发送到指定的Redis I/O通道
 *
 * @param rdb 数据库信息
 * @param error 错误信息
 * @param flags 标签
 * @param rsi 需要保存的信息
 */
int rdbSaveRio(
        rio *rdb,
        int *error,
        int flags,
        rdbSaveInfo *rsi) {

    dictIterator *di = NULL;
    dictEntry *de;
    char magic[10];
    int j;
    long long now = mstime();
    uint64_t cksum;
    size_t processed = 0;
    // 数据校验
    if (server.rdb_checksum)
    {
        rdb->update_cksum = rioGenericUpdateChecksum;
    }
    snprintf(magic, sizeof(magic), "REDIS%04d", RDB_VERSION);
    if (rdbWriteRaw(rdb, magic, 9) == -1) {
        goto werr;
    }
    if (rdbSaveInfoAuxFields(rdb, flags, rsi) == -1) {
        goto werr;
    }

    // 遍历数据库
    for (j = 0; j < server.dbnum; j++) {
        // 数据库号
        redisDb *db = server.db + j;
        dict *d = db->dict;
        // 代表数据库没数据
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);
        if (!di) {
            return C_ERR;
        }
        if (rdbSaveType(rdb, RDB_OPCODE_SELECTDB) == -1) {
            goto werr;
        }
        if (rdbSaveLen(rdb, j) == -1) {
            goto werr;
        }
        uint32_t db_size, expires_size;
        db_size = (dictSize(db->dict) <= UINT32_MAX) ? dictSize(db->dict) : UINT32_MAX;
        expires_size = (dictSize(db->expires) <= UINT32_MAX) ? dictSize(db->expires) : UINT32_MAX;
        if (rdbSaveType(rdb, RDB_OPCODE_RESIZEDB) == -1) {
            goto werr;
        }
        if (rdbSaveLen(rdb, db_size) == -1) {
            goto werr;
        }
        if (rdbSaveLen(rdb, expires_size) == -1) {
            goto werr;
        }
        // 遍历字典
        while ((de = dictNext(di)) != NULL) {
            // key
            sds keystr = dictGetKey(de);
            // 值
            robj key, *o = dictGetVal(de);
            long long expire;
            initStaticStringObject(key, keystr);
            // 过期信息
            expire = getExpire(db, &key);
            // 保存命令
            if (rdbSaveKeyValuePair(rdb, &key, o, expire, now) == -1) {
                goto werr;
            }

            if (flags & RDB_SAVE_AOF_PREAMBLE &&
                rdb->processed_bytes > processed + AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = rdb->processed_bytes;
                aofReadDiffFromParent();
            }
        }
        dictReleaseIterator(di);
    }
    di = NULL;

    if (rdbSaveType(rdb, RDB_OPCODE_EOF) == -1) goto werr;

    // 数据校验
    cksum = rdb->cksum;
    // 将数据校验写入文件
    if (rioWrite(rdb, &cksum, 8) == 0) goto werr;
    return C_OK;

    werr:
    if (error) *error = errno;
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/**
 * 生成RDB文件并保存到磁盘上
 *
 * @param filename 文件名
 * @param rsi 数据库信息
 */
int rdbSave(
        char *filename,
        rdbSaveInfo *rsi) {

    char tmpfile[256];
    char cwd[MAXPATHLEN];
    FILE *fp;
    rio rdb;
    int error = 0;
    snprintf(tmpfile, 256, "temp-%d.rdb", (int) getpid());
    // 开启临时文件[w可写]
    fp = fopen(tmpfile, "w");
    if (!fp) {
        char *cwdp = getcwd(cwd, MAXPATHLEN);
        serverLog(LL_WARNING, "Failed opening the RDB file %s (in server root dir %s) ""for saving: %s", filename,
                  cwdp ? cwdp : "unknown", strerror(errno));
        return C_ERR;
    }
    // 根据文件初始化IO
    rioInitWithFile(&rdb, fp);
    // 保存信息写入RDB文件[核心操作]
    if (rdbSaveRio(&rdb, &error, RDB_SAVE_NONE, rsi) == C_ERR) {
        errno = error;
        goto werr;
    }
    if (fflush(fp) == EOF) {
        goto werr;
    }
    // 强制刷盘
    if (fsync(fileno(fp)) == -1) {
        goto werr;
    }
    if (fclose(fp) == EOF) {
        goto werr;
    }
    if (rename(tmpfile, filename) == -1) {
        char *cwdp = getcwd(cwd, MAXPATHLEN);
        serverLog(LL_WARNING, "Error moving temp DB file %s on the final ""destination %s (in server root dir %s): %s",
                  tmpfile, filename, cwdp ? cwdp : "unknown", strerror(errno));
        unlink(tmpfile);
        return C_ERR;
    }
    serverLog(LL_NOTICE, "DB saved on disk");
    server.dirty = 0;
    server.lastsave = time(NULL);
    server.lastbgsave_status = C_OK;
    return C_OK;

    werr:
    serverLog(LL_WARNING, "Write error saving DB on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
}

/**
 * 真正生成RDB文件的地方
 *
 * @param filename 文件名
 * @param rsi rdb需要保存的信息
 */
int rdbSaveBackground(
        char *filename,
        rdbSaveInfo *rsi) {

    pid_t childpid;
    long long start;
    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        return C_ERR;
    }
    // 计数器
    server.dirty_before_bgsave = server.dirty;
    // 最后一次执行bgsave的时间
    server.lastbgsave_try = time(NULL);
    // 开启子进程信息通道
    openChildInfoPipe();
    start = ustime();
    // 通过fork操作创建子进程去执行,如果是子进程则会返回0
    if ((childpid = fork()) == 0) {
        int retval;
        closeListeningSockets(0);
        redisSetProcTitle("redis-rdb-bgsave");
        // 执行RDB持久化
        retval = rdbSave(filename, rsi);
        if (retval == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty(-1);
            if (private_dirty) {
                serverLog(LL_NOTICE, "RDB: %zu MB of memory used by copy-on-write", private_dirty / (1024 * 1024));
            }
            server.child_info_data.cow_size = private_dirty;
            sendChildInfo(CHILD_INFO_TYPE_RDB);
        }
        // 退出子进程
        exitFromChild((retval == C_OK) ? 0 : 1);
    } else {
        server.stat_fork_time = ustime() - start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time / (1024 * 1024 * 1024);
        latencyAddSampleIfNeeded("fork", server.stat_fork_time / 1000);
        if (childpid == -1) {
            closeChildInfoPipe();
            server.lastbgsave_status = C_ERR;
            serverLog(LL_WARNING, "Can't save in background: fork: %s", strerror(errno));
            return C_ERR;
        }
        serverLog(LL_NOTICE, "Background saving started by pid %d", childpid);
        server.rdb_save_time_start = time(NULL);
        server.rdb_child_pid = childpid;
        server.rdb_child_type = RDB_CHILD_TYPE_DISK;
        updateDictResizePolicy();
        return C_OK;
    }
    return C_OK;
}

/**
 * 删除临时文件
 *
 * @param childpid
 */
void rdbRemoveTempFile(
        pid_t childpid) {

    char tmpfile[256];

    snprintf(tmpfile, sizeof(tmpfile), "temp-%d.rdb", (int) childpid);
    unlink(tmpfile);
}

robj *rdbLoadCheckModuleValue(
        rio *rdb,
        char *modulename) {

    uint64_t opcode;
    while ((opcode = rdbLoadLen(rdb, NULL)) != RDB_MODULE_OPCODE_EOF) {
        if (opcode == RDB_MODULE_OPCODE_SINT ||
            opcode == RDB_MODULE_OPCODE_UINT) {
            uint64_t len;
            if (rdbLoadLenByRef(rdb, NULL, &len) == -1) {
                rdbExitReportCorruptRDB(
                        "Error reading integer from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_STRING) {
            robj *o = rdbGenericLoadStringObject(rdb, RDB_LOAD_NONE, NULL);
            if (o == NULL) {
                rdbExitReportCorruptRDB(
                        "Error reading string from module %s value", modulename);
            }
            decrRefCount(o);
        } else if (opcode == RDB_MODULE_OPCODE_FLOAT) {
            float val;
            if (rdbLoadBinaryFloatValue(rdb, &val) == -1) {
                rdbExitReportCorruptRDB(
                        "Error reading float from module %s value", modulename);
            }
        } else if (opcode == RDB_MODULE_OPCODE_DOUBLE) {
            double val;
            if (rdbLoadBinaryDoubleValue(rdb, &val) == -1) {
                rdbExitReportCorruptRDB(
                        "Error reading double from module %s value", modulename);
            }
        }
    }
    return createStringObject("module-dummy-value", 18);
}

robj *rdbLoadObject(
        int rdbtype,
        rio *rdb) {

    robj *o = NULL, *ele, *dec;
    uint64_t len;
    unsigned int i;

    if (rdbtype == RDB_TYPE_STRING) {
        /* Read string value */
        if ((o = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
        o = tryObjectEncoding(o);
    } else if (rdbtype == RDB_TYPE_LIST) {
        /* Read list value */
        if ((len = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;

        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);

        /* Load every single element of the list */
        while (len--) {
            if ((ele = rdbLoadEncodedStringObject(rdb)) == NULL) return NULL;
            dec = getDecodedObject(ele);
            size_t len = sdslen(dec->ptr);
            quicklistPushTail(o->ptr, dec->ptr, len);
            decrRefCount(dec);
            decrRefCount(ele);
        }
    } else if (rdbtype == RDB_TYPE_SET) {
        /* Read Set value */
        if ((len = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;

        /* Use a regular set when there are too many entries. */
        if (len > server.set_max_intset_entries) {
            o = createSetObject();
            /* It's faster to expand the dict to the right size asap in order
             * to avoid rehashing */
            if (len > DICT_HT_INITIAL_SIZE)
                dictExpand(o->ptr, len);
        } else {
            o = createIntsetObject();
        }

        /* Load every single element of the set */
        for (i = 0; i < len; i++) {
            long long llval;
            sds sdsele;

            if ((sdsele = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;

            if (o->encoding == OBJ_ENCODING_INTSET) {
                /* Fetch integer value from element. */
                if (isSdsRepresentableAsLongLong(sdsele, &llval) == C_OK) {
                    o->ptr = intsetAdd(o->ptr, llval, NULL);
                } else {
                    setTypeConvert(o, OBJ_ENCODING_HT);
                    dictExpand(o->ptr, len);
                }
            }

            /* This will also be called when the set was just converted
             * to a regular hash table encoded set. */
            if (o->encoding == OBJ_ENCODING_HT) {
                dictAdd((dict *) o->ptr, sdsele, NULL);
            } else {
                sdsfree(sdsele);
            }
        }
    } else if (rdbtype == RDB_TYPE_ZSET_2 || rdbtype == RDB_TYPE_ZSET) {
        /* Read list/set value. */
        uint64_t zsetlen;
        size_t maxelelen = 0;
        zset *zs;

        if ((zsetlen = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;
        o = createZsetObject();
        zs = o->ptr;

        /* Load every single element of the sorted set. */
        while (zsetlen--) {
            sds sdsele;
            double score;
            zskiplistNode *znode;

            if ((sdsele = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;

            if (rdbtype == RDB_TYPE_ZSET_2) {
                if (rdbLoadBinaryDoubleValue(rdb, &score) == -1) return NULL;
            } else {
                if (rdbLoadDoubleValue(rdb, &score) == -1) return NULL;
            }

            /* Don't care about integer-encoded strings. */
            if (sdslen(sdsele) > maxelelen) maxelelen = sdslen(sdsele);

            znode = zslInsert(zs->zsl, score, sdsele);
            dictAdd(zs->dict, sdsele, &znode->score);
        }

        /* Convert *after* loading, since sorted sets are not stored ordered. */
        if (zsetLength(o) <= server.zset_max_ziplist_entries &&
            maxelelen <= server.zset_max_ziplist_value)
            zsetConvert(o, OBJ_ENCODING_ZIPLIST);
    } else if (rdbtype == RDB_TYPE_HASH) {
        uint64_t len;
        int ret;
        sds field, value;

        len = rdbLoadLen(rdb, NULL);
        if (len == RDB_LENERR) return NULL;

        o = createHashObject();

        /* Too many entries? Use a hash table. */
        if (len > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);

        /* Load every field and value into the ziplist */
        while (o->encoding == OBJ_ENCODING_ZIPLIST && len > 0) {
            len--;
            /* Load raw strings */
            if ((field = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;
            if ((value = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;

            /* Add pair to ziplist */
            o->ptr = ziplistPush(o->ptr, (unsigned char *) field,
                                 sdslen(field), ZIPLIST_TAIL);
            o->ptr = ziplistPush(o->ptr, (unsigned char *) value,
                                 sdslen(value), ZIPLIST_TAIL);

            /* Convert to hash table if size threshold is exceeded */
            if (sdslen(field) > server.hash_max_ziplist_value ||
                sdslen(value) > server.hash_max_ziplist_value) {
                sdsfree(field);
                sdsfree(value);
                hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            }
            sdsfree(field);
            sdsfree(value);
        }

        /* Load remaining fields and values into the hash table */
        while (o->encoding == OBJ_ENCODING_HT && len > 0) {
            len--;
            /* Load encoded strings */
            if ((field = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;
            if ((value = rdbGenericLoadStringObject(rdb, RDB_LOAD_SDS, NULL))
                == NULL)
                return NULL;

            /* Add pair to hash table */
            ret = dictAdd((dict *) o->ptr, field, value);
            if (ret == DICT_ERR) {
                rdbExitReportCorruptRDB("Duplicate keys detected");
            }
        }

        /* All pairs should be read by now */
        serverAssert(len == 0);
    } else if (rdbtype == RDB_TYPE_LIST_QUICKLIST) {
        if ((len = rdbLoadLen(rdb, NULL)) == RDB_LENERR) return NULL;
        o = createQuicklistObject();
        quicklistSetOptions(o->ptr, server.list_max_ziplist_size,
                            server.list_compress_depth);

        while (len--) {
            unsigned char *zl =
                    rdbGenericLoadStringObject(rdb, RDB_LOAD_PLAIN, NULL);
            if (zl == NULL) return NULL;
            quicklistAppendZiplist(o->ptr, zl);
        }
    } else if (rdbtype == RDB_TYPE_HASH_ZIPMAP ||
               rdbtype == RDB_TYPE_LIST_ZIPLIST ||
               rdbtype == RDB_TYPE_SET_INTSET ||
               rdbtype == RDB_TYPE_ZSET_ZIPLIST ||
               rdbtype == RDB_TYPE_HASH_ZIPLIST) {
        unsigned char *encoded =
                rdbGenericLoadStringObject(rdb, RDB_LOAD_PLAIN, NULL);
        if (encoded == NULL) return NULL;
        o = createObject(OBJ_STRING, encoded); /* Obj type fixed below. */

        /* Fix the object encoding, and make sure to convert the encoded
         * data type into the base type if accordingly to the current
         * configuration there are too many elements in the encoded data
         * type. Note that we only check the length and not max element
         * size as this is an O(N) scan. Eventually everything will get
         * converted. */
        switch (rdbtype) {
            case RDB_TYPE_HASH_ZIPMAP:
                /* Convert to ziplist encoded hash. This must be deprecated
                 * when loading dumps created by Redis 2.4 gets deprecated. */
            {
                unsigned char *zl = ziplistNew();
                unsigned char *zi = zipmapRewind(o->ptr);
                unsigned char *fstr, *vstr;
                unsigned int flen, vlen;
                unsigned int maxlen = 0;

                while ((zi = zipmapNext(zi, &fstr, &flen, &vstr, &vlen)) != NULL) {
                    if (flen > maxlen) maxlen = flen;
                    if (vlen > maxlen) maxlen = vlen;
                    zl = ziplistPush(zl, fstr, flen, ZIPLIST_TAIL);
                    zl = ziplistPush(zl, vstr, vlen, ZIPLIST_TAIL);
                }

                zfree(o->ptr);
                o->ptr = zl;
                o->type = OBJ_HASH;
                o->encoding = OBJ_ENCODING_ZIPLIST;

                if (hashTypeLength(o) > server.hash_max_ziplist_entries ||
                    maxlen > server.hash_max_ziplist_value) {
                    hashTypeConvert(o, OBJ_ENCODING_HT);
                }
            }
                break;
            case RDB_TYPE_LIST_ZIPLIST:
                o->type = OBJ_LIST;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                listTypeConvert(o, OBJ_ENCODING_QUICKLIST);
                break;
            case RDB_TYPE_SET_INTSET:
                o->type = OBJ_SET;
                o->encoding = OBJ_ENCODING_INTSET;
                if (intsetLen(o->ptr) > server.set_max_intset_entries)
                    setTypeConvert(o, OBJ_ENCODING_HT);
                break;
            case RDB_TYPE_ZSET_ZIPLIST:
                o->type = OBJ_ZSET;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (zsetLength(o) > server.zset_max_ziplist_entries)
                    zsetConvert(o, OBJ_ENCODING_SKIPLIST);
                break;
            case RDB_TYPE_HASH_ZIPLIST:
                o->type = OBJ_HASH;
                o->encoding = OBJ_ENCODING_ZIPLIST;
                if (hashTypeLength(o) > server.hash_max_ziplist_entries)
                    hashTypeConvert(o, OBJ_ENCODING_HT);
                break;
            default:
                rdbExitReportCorruptRDB("Unknown RDB encoding type %d", rdbtype);
                break;
        }
    } else if (rdbtype == RDB_TYPE_MODULE || rdbtype == RDB_TYPE_MODULE_2) {
        uint64_t moduleid = rdbLoadLen(rdb, NULL);
        moduleType *mt = moduleTypeLookupModuleByID(moduleid);
        char name[10];

        if (rdbCheckMode && rdbtype == RDB_TYPE_MODULE_2)
            return rdbLoadCheckModuleValue(rdb, name);

        if (mt == NULL) {
            moduleTypeNameByID(name, moduleid);
            serverLog(LL_WARNING, "The RDB file contains module data I can't load: no matching module '%s'", name);
            exit(1);
        }
        RedisModuleIO io;
        moduleInitIOContext(io, mt, rdb);
        io.ver = (rdbtype == RDB_TYPE_MODULE) ? 1 : 2;
        /* Call the rdb_load method of the module providing the 10 bit
         * encoding version in the lower 10 bits of the module ID. */
        void *ptr = mt->rdb_load(&io, moduleid & 1023);
        if (io.ctx) {
            moduleFreeContext(io.ctx);
            zfree(io.ctx);
        }

        /* Module v2 serialization has an EOF mark at the end. */
        if (io.ver == 2) {
            uint64_t eof = rdbLoadLen(rdb, NULL);
            if (eof != RDB_MODULE_OPCODE_EOF) {
                serverLog(LL_WARNING,
                          "The RDB file contains module data for the module '%s' that is not terminated by the proper module value EOF marker",
                          name);
                exit(1);
            }
        }

        if (ptr == NULL) {
            moduleTypeNameByID(name, moduleid);
            serverLog(LL_WARNING,
                      "The RDB file contains module data for the module type '%s', that the responsible module is not able to load. Check for modules log above for additional clues.",
                      name);
            exit(1);
        }
        o = createModuleObject(mt, ptr);
    } else {
        rdbExitReportCorruptRDB("Unknown RDB encoding type %d", rdbtype);
    }
    return o;
}

/**
 * 开始加载RDB文件
 *
 * @param fp
 */
void startLoading(
        FILE *fp) {

    struct stat sb;
    server.loading = 1;
    server.loading_start_time = time(NULL);
    server.loading_loaded_bytes = 0;
    if (fstat(fileno(fp), &sb) == -1) {
        server.loading_total_bytes = 0;
    } else {
        server.loading_total_bytes = sb.st_size;
    }
}

/**
 * 刷新加载信息
 *
 * @param pos
 */
void loadingProgress(
        off_t pos) {

    server.loading_loaded_bytes = pos;
    if (server.stat_peak_memory < zmalloc_used_memory())
        server.stat_peak_memory = zmalloc_used_memory();
}

void stopLoading(
        void) {

    server.loading = 0;
}

void rdbLoadProgressCallback(
        rio *r,
        const void *buf,
        size_t len) {

    if (server.rdb_checksum)
        rioGenericUpdateChecksum(r, buf, len);
    if (server.loading_process_events_interval_bytes &&
        (r->processed_bytes + len) / server.loading_process_events_interval_bytes >
        r->processed_bytes / server.loading_process_events_interval_bytes) {
        /* The DB can take some non trivial amount of time to load. Update
         * our cached time since it is used to create and update the last
         * interaction time with clients and for other important things. */
        updateCachedTime();
        if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER)
            replicationSendNewlineToMaster();
        loadingProgress(r->processed_bytes);
        processEventsWhileBlocked();
    }
}

/**
 * 加载RDB文件
 *
 * @param rdb
 * @param rsi
 */
int rdbLoadRio(
        rio *rdb,
        rdbSaveInfo *rsi) {

    uint64_t dbid;
    int type, rdbver;
    redisDb *db = server.db + 0;
    char buf[1024];
    long long expiretime, now = mstime();
    rdb->update_cksum = rdbLoadProgressCallback;
    rdb->max_processing_chunk = server.loading_process_events_interval_bytes;
    if (rioRead(rdb, buf, 9) == 0) goto eoferr;
    buf[9] = '\0';
    if (memcmp(buf, "REDIS", 5) != 0) {
        serverLog(LL_WARNING, "Wrong signature trying to load DB from file");
        errno = EINVAL;
        return C_ERR;
    }
    rdbver = atoi(buf + 5);
    if (rdbver < 1 || rdbver > RDB_VERSION) {
        serverLog(LL_WARNING, "Can't handle RDB format version %d", rdbver);
        errno = EINVAL;
        return C_ERR;
    }

    while (1) {
        robj *key, *val;
        expiretime = -1;

        if ((type = rdbLoadType(rdb)) == -1) goto eoferr;

        if (type == RDB_OPCODE_EXPIRETIME) {
            if ((expiretime = rdbLoadTime(rdb)) == -1) goto eoferr;
            if ((type = rdbLoadType(rdb)) == -1) goto eoferr;
            expiretime *= 1000;
        } else if (type == RDB_OPCODE_EXPIRETIME_MS) {
            if ((expiretime = rdbLoadMillisecondTime(rdb)) == -1) goto eoferr;
            if ((type = rdbLoadType(rdb)) == -1) goto eoferr;
        } else if (type == RDB_OPCODE_EOF) {
            break;
        } else if (type == RDB_OPCODE_SELECTDB) {
            if ((dbid = rdbLoadLen(rdb, NULL)) == RDB_LENERR)
                goto eoferr;
            if (dbid >= (unsigned) server.dbnum) {
                serverLog(LL_WARNING,
                          "FATAL: Data file was created with a Redis "
                          "server configured to handle more than %d "
                          "databases. Exiting\n", server.dbnum);
                exit(1);
            }
            db = server.db + dbid;
            continue;
        } else if (type == RDB_OPCODE_RESIZEDB) {
            uint64_t db_size, expires_size;
            if ((db_size = rdbLoadLen(rdb, NULL)) == RDB_LENERR)
                goto eoferr;
            if ((expires_size = rdbLoadLen(rdb, NULL)) == RDB_LENERR)
                goto eoferr;
            dictExpand(db->dict, db_size);
            dictExpand(db->expires, expires_size);
            continue; /* Read type again. */
        } else if (type == RDB_OPCODE_AUX) {
            robj *auxkey, *auxval;
            if ((auxkey = rdbLoadStringObject(rdb)) == NULL) goto eoferr;
            if ((auxval = rdbLoadStringObject(rdb)) == NULL) goto eoferr;

            if (((char *) auxkey->ptr)[0] == '%') {
                serverLog(LL_NOTICE, "RDB '%s': %s",
                          (char *) auxkey->ptr,
                          (char *) auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr, "repl-stream-db")) {
                if (rsi) rsi->repl_stream_db = atoi(auxval->ptr);
            } else if (!strcasecmp(auxkey->ptr, "repl-id")) {
                if (rsi && sdslen(auxval->ptr) == CONFIG_RUN_ID_SIZE) {
                    memcpy(rsi->repl_id, auxval->ptr, CONFIG_RUN_ID_SIZE + 1);
                    rsi->repl_id_is_set = 1;
                }
            } else if (!strcasecmp(auxkey->ptr, "repl-offset")) {
                if (rsi) rsi->repl_offset = strtoll(auxval->ptr, NULL, 10);
            } else {
                serverLog(LL_DEBUG, "Unrecognized RDB AUX field: '%s'",
                          (char *) auxkey->ptr);
            }

            decrRefCount(auxkey);
            decrRefCount(auxval);
            continue;
        }

        if ((key = rdbLoadStringObject(rdb)) == NULL) goto eoferr;
        if ((val = rdbLoadObject(type, rdb)) == NULL) goto eoferr;
        if (server.masterhost == NULL && expiretime != -1 && expiretime < now) {
            decrRefCount(key);
            decrRefCount(val);
            continue;
        }
        dbAdd(db, key, val);
        if (expiretime != -1) setExpire(NULL, db, key, expiretime);
        decrRefCount(key);
    }
    if (rdbver >= 5 && server.rdb_checksum) {
        uint64_t cksum, expected = rdb->cksum;

        if (rioRead(rdb, &cksum, 8) == 0) goto eoferr;
        if (cksum == 0) {
            serverLog(LL_WARNING, "RDB file was saved with checksum disabled: no check performed.");
        } else if (cksum != expected) {
            serverLog(LL_WARNING, "Wrong RDB checksum. Aborting now.");
            rdbExitReportCorruptRDB("RDB CRC error");
        }
    }
    return C_OK;

    eoferr: /* unexpected end of file is handled here with a fatal exit */
    serverLog(LL_WARNING, "Short read or OOM loading DB. Unrecoverable error, aborting now.");
    rdbExitReportCorruptRDB("Unexpected EOF reading RDB file");
    return C_ERR; /* Just to avoid warning */
}

/**
 * 加载loadRDB文件
 *
 * @param filename
 * @param rsi
 */
int rdbLoad(
        char *filename,
        rdbSaveInfo *rsi) {

    FILE *fp;
    rio rdb;
    int retval;
    if ((fp = fopen(filename, "r")) == NULL) return C_ERR;
    startLoading(fp);
    // 初始化I/O
    rioInitWithFile(&rdb, fp);
    // 真正加载RDB的地方
    retval = rdbLoadRio(&rdb, rsi);
    fclose(fp);
    stopLoading();
    return retval;
}

/**
 * 子进程执行完,父进程执行,持久化到磁盘再传输
 *
 * @param exitcode 退出标记
 * @param bysignal 唤醒标记
 */
void backgroundSaveDoneHandlerDisk(
        int exitcode,
        int bysignal) {

    // 子进程成功执行完成退出
    if (!bysignal && exitcode == 0) {
        // 成功退出
        serverLog(LL_NOTICE, "Background saving terminated with success");
        server.dirty = server.dirty - server.dirty_before_bgsave;
        server.lastsave = time(NULL);
        // 代表ok
        server.lastbgsave_status = C_OK;
    } else if (!bysignal && exitcode != 0) {
        serverLog(LL_WARNING, "Background saving error");
        server.lastbgsave_status = C_ERR;
    } else {
        mstime_t latency;
        serverLog(LL_WARNING, "Background saving terminated by signal %d", bysignal);
        latencyStartMonitor(latency);
        rdbRemoveTempFile(server.rdb_child_pid);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("rdb-unlink-temp-file", latency);
        if (bysignal != SIGUSR1)
            server.lastbgsave_status = C_ERR;
    }
    // 消除子进程
    server.rdb_child_pid = -1;
    server.rdb_child_type = RDB_CHILD_TYPE_NONE;
    server.rdb_save_time_last = time(NULL) - server.rdb_save_time_start;
    server.rdb_save_time_start = -1;
    // 将文件更新给从节点
    updateSlavesWaitingBgsave((!bysignal && exitcode == 0) ? C_OK : C_ERR, RDB_CHILD_TYPE_DISK);
}

/**
 * 父进程等待RDB子进程完成后执行
 *
 * @param exitcode
 * @param bysignal
 */
void backgroundSaveDoneHandler(
        int exitcode,
        int bysignal) {

    switch (server.rdb_child_type) {
        // 刷盘方式
        case RDB_CHILD_TYPE_DISK:
            backgroundSaveDoneHandlerDisk(exitcode, bysignal);
            break;
        default:
            serverPanic("Unknown RDB child type.");
            break;
    }
}

void bgsaveCommand(
        client *c) {

    int schedule = 0;
    if (c->argc > 1) {
        if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr, "schedule")) {
            schedule = 1;
        } else {
            addReply(c, shared.syntaxerr);
            return;
        }
    }
    // 有子进程在执行RDB持久化
    if (server.rdb_child_pid != -1) {
        addReplyError(c, "Background save already in progress");
    } else if (server.aof_child_pid != -1) {
        if (schedule) {
            // 开启调度
            server.rdb_bgsave_scheduled = 1;
            addReplyStatus(c, "Background saving scheduled");
        } else {
            addReplyError(c,
                          "An AOF log rewriting in progress: can't BGSAVE right now. ""Use BGSAVE SCHEDULE in order to schedule a BGSAVE whenever ""possible.");
        }
        // 执行RDB持久化
    } else if (rdbSaveBackground(server.rdb_filename, NULL) == C_OK) {
        addReplyStatus(c, "Background saving started");
    } else {
        addReply(c, shared.err);
    }
}