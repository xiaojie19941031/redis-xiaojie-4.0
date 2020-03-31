/**
 * AOF持久化
 *
 *   主要流程
 *
 *      1、命令追加(feedAppendOnlyFile),服务器执行写入或修改命令后将此命令追加到aof_buf[AOF缓冲区](如果子进程在执行AOF重写,父进程同时会将命令写入AOF_REWRITE_BUF[AOF重写缓冲区],并将这些内容写入aof_pipe_write_data_to_child[父子进程之间的通道])
 *
 *      2、文件写入同步(flushAppendOnlyFile)[serverCron(时间事件)中每秒执行,beforeSleep(进入下一次事件循环前[第一次会执行])中执行],将aof_bug缓冲区中的数据写入AOF文件中,然后根据同步策略进行文件落盘
 *
 *            有子线程在等待AOF持久化
 *
 *               AOF未延迟执行,更新AOF延迟执行时间为当前时间直接返回[此操作后时间事件中的flushAppendFile才会执行],不会阻塞主线程
 *
 *               AOF延迟执行,并且此时距离上一次持久化时间未超过2秒直接返回,不会阻塞主线程
 *
 *               AOF延迟执行,并且此时距离上一次持久化时间超过2秒[此情况是最坏的情况,磁盘可能出问题了,这时候主线程会继续往下走,调用write时会因为在执行fsync函数而阻塞,redis阻塞原因]
 *
 *            无子线程执行AOF持久化
 *
 *               直接将aof_buf中的数据写入AOF文件根据刷盘策略进行刷盘即可
 *
 *            刷盘策略
 *
 *               always:每次write操作后直接主线程调用fsync方法进行同步,同步期间阻塞redis[效率低下]
 *
 *               everysec:每次write操作后主线程直接返回,fsync同步由子线程进行调用[不阻塞主线程],最多丢失2秒的数据
 *
 *               no:每次write后不调用fsync进行同步,具体调用fsync同步时间由操作系统决定[丢失数据最多]
 *
 *      3、AOF文件重写[缩小AOF文件]
 *
 *          手动触发
 *
 *             bgrewriteaof
 *
 *          自动触发
 *
 *             配置文件配置AOF重写规则,当AOF文件满足条件时会自动进行AOF文件重写[在serverCron时间事件中进行判断是否要进行重写]
 *
 *          主要流程[此过程会通过fork()创建子进程]
 *
 *             父进程开启父子进程进行数据传输通道,通过fork()创建子进程[阻塞]执行AOF重写,创建成功父进程记录重写信息直接返回,否则返回错误信息
 *
 *             子进程开启临时文件,通过遍历redis数据库将数据以最简的命令将内容写入文件,并通过管道读取少量父进程传输的新数据,如果超过1秒或者20次没有收到数据直接要求父进程停止传输数据[AOF重写缓冲区中的数据]
 *
 *             子进程发送停止传输新数据的信号给父进程,父进程停止传输新数据同时子进程再将差异通道中的数据全部读取写入AOF文件进行刷盘[防止未读取到差异]
 *
 *             父进程等待子进程完成AOF重写后[在serverCron时间事件中,子进程完成会通知父进程],将父进程中重写缓冲区剩余的内容写入AOF文件中,根据刷盘策略进行刷盘并替换老的AOF文件
 *
 * AOF为什么把命令追加到aof_buf中？Redis使用单线程响应命令,如果每次写AOF文件命令都直接追加到硬盘,那么性能完全取决于当前硬盘负载,先写入缓冲区aof_buf中,还有另一个好处,Redis可以提供多种缓冲区同步硬盘的策略,在性能和安全性方面做出平衡
 *
 * AOF持久化开启且存在AOF文件时,优先加载AOF文件,AOF关闭或者AOF文件不存在时,加载RDB文件
 *
 * redis-check-aof--fix[修复有问题的AOF文件]
 */

#include "server.h"
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>

void aofUpdateCurrentSize(void);

void aofClosePipes(void);

#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)

typedef struct aofrwblock {
    unsigned long used, free;
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

void aofRewriteBufferReset(
        void) {

    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks, zfree);
}

unsigned long aofRewriteBufferSize(
        void) {

    listNode *ln;
    listIter li;
    unsigned long size = 0;

    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        size += block->used;
    }
    return size;
}

/**
 * 将父进行重写缓冲区数据发送给子进程事件处理程序
 *
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void aofChildWriteDiffData(
        aeEventLoop *el,
        int fd,
        void *privdata,
        int mask) {

    listNode *ln;
    aofrwblock *block;
    ssize_t nwritten;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);
    while (1) {
        ln = listFirst(server.aof_rewrite_buf_blocks);
        block = ln ? ln->value : NULL;
        // 子进程要求停止发送
        if (server.aof_stop_sending_diff || !block) {
            // 删除写事件
            aeDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE);
            return;
        }
        if (block->used > 0) {
            // 将差异写进子进程通道
            nwritten = write(server.aof_pipe_write_data_to_child, block->buf, block->used);
            if (nwritten <= 0) return;
            // 更改内容将已经写个子进程的内容删除掉
            memmove(block->buf, block->buf + nwritten, block->used - nwritten);
            block->used -= nwritten;
            block->free += nwritten;
        }
        // 如果没有数据了,代表写完了直接删除节点
        if (block->used == 0) listDelNode(server.aof_rewrite_buf_blocks, ln);
    }
}

/**
 * 将数据追加到AOF重写缓冲区
 *
 * @param s
 * @param len
 */
void aofRewriteBufferAppend(
        unsigned char *s,
        unsigned long len) {

    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;
    while (len) {
        // 存在block
        if (block) {
            // 空闲的大小
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {
                // 内存拷贝
                memcpy(block->buf + block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }
        if (len) {
            int numblocks;
            block = zmalloc(sizeof(*block));
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            listAddNodeTail(server.aof_rewrite_buf_blocks, block);
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks + 1) % 10) == 0) {
                int level = ((numblocks + 1) % 100) == 0 ? LL_WARNING : LL_NOTICE;
                serverLog(level, "Background AOF buffer size: %lu MB", aofRewriteBufferSize() / (1024 * 1024));
            }
        }
    }

    // 获取文件事件失败也就是不存在这个事件[像子进程写数据通道]
    if (aeGetFileEvents(server.el, server.aof_pipe_write_data_to_child) == 0) {
        aeCreateFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE, aofChildWriteDiffData, NULL);
    }
}

ssize_t aofRewriteBufferWrite(
        int fd) {

    listNode *ln;
    listIter li;
    ssize_t count = 0;

    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {
            nwritten = write(fd, block->buf, block->used);
            if (nwritten != (ssize_t) block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }
            count += nwritten;
        }
    }
    return count;
}

/**
 * 启动后台线程进行文件同步
 *
 * @param fd
 */
void aof_background_fsync(
        int fd) {

    bioCreateBackgroundJob(BIO_AOF_FSYNC, (void *) (long) fd, NULL, NULL);
}

void stopAppendOnly(
        void) {

    serverAssert(server.aof_state != AOF_OFF);
    flushAppendOnlyFile(1);
    aof_fsync(server.aof_fd);
    close(server.aof_fd);

    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = AOF_OFF;
    if (server.aof_child_pid != -1) {
        int statloc;

        serverLog(LL_NOTICE, "Killing running AOF rewrite child: %ld",
                  (long) server.aof_child_pid);
        if (kill(server.aof_child_pid, SIGUSR1) != -1) {
            while (wait3(&statloc, 0, NULL) != server.aof_child_pid);
        }
        /* reset the buffer accumulating changes while the child saves */
        aofRewriteBufferReset();
        aofRemoveTempFile(server.aof_child_pid);
        server.aof_child_pid = -1;
        server.aof_rewrite_time_start = -1;
        /* close pipes used for IPC between the two processes. */
        aofClosePipes();
    }
}

/**
 * 启动AOF
 */
int startAppendOnly(
        void) {

    char cwd[MAXPATHLEN];
    // 最后一次aof操作时间
    server.aof_last_fsync = server.unixtime;
    // AOF文件
    server.aof_fd = open(server.aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    serverAssert(server.aof_state == AOF_OFF);
    if (server.aof_fd == -1) {
        char *cwdp = getcwd(cwd, MAXPATHLEN);
        serverLog(LL_WARNING,
                  "Redis needs to enable the AOF but can't open the "
                  "append only file %s (in server root dir %s): %s",
                  server.aof_filename,
                  cwdp ? cwdp : "unknown",
                  strerror(errno));
        return C_ERR;
    }
    if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_WARNING,
                  "AOF was enabled but there is already a child process saving an RDB file on disk. An AOF background was scheduled to start when possible.");
    } else if (rewriteAppendOnlyFileBackground() == C_ERR) {
        close(server.aof_fd);
        serverLog(LL_WARNING,
                  "Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
        return C_ERR;
    }
    // 等待AOF重写
    server.aof_state = AOF_WAIT_REWRITE;
    return C_OK;
}

/**
 * 文件写入同步操作
 */
#define AOF_WRITE_LOG_ERROR_RATE 30

void flushAppendOnlyFile(
        int force) {

    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;
    // 表示没有东西不需要进行写入
    if (sdslen(server.aof_buf) == 0) return;
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        // 返回type类型的job正在等待被执行的个数
        sync_in_progress = bioPendingJobsOfType(BIO_AOF_FSYNC) != 0;
    // 强制执行
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        // 代表有aof线程在执行
        if (sync_in_progress) {
            if (server.aof_flush_postponed_start == 0) {
                server.aof_flush_postponed_start = server.unixtime;
                return;
                // 代表两次fsync任务不超过2秒,直接返回不会阻塞主线程
            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                return;
            }
            // 代表两次fsync之间的间隔超过了2秒,说明磁盘可能有问题
            server.aof_delayed_fsync++;
            serverLog(LL_NOTICE,
                      "Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    // 原子的进行操作
    latencyStartMonitor(latency);
    // 调用write方法写入文件[写入的是缓冲区](如果还在刷盘此操作会被阻塞),因为主进程中也会调用这个方法
    nwritten = write(server.aof_fd, server.aof_buf, sdslen(server.aof_buf));
    latencyEndMonitor(latency);
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync", latency);
    } else if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) {
        latencyAddSampleIfNeeded("aof-write-active-child", latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone", latency);
    }
    latencyAddSampleIfNeeded("aof-write", latency);
    // 判断write是否阻塞
    server.aof_flush_postponed_start = 0;
    // 文件长度不对
    if (nwritten != (signed) sdslen(server.aof_buf)) {
        static time_t last_write_error_log = 0;
        int can_log = 0;
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }
        if (nwritten == -1) {
            if (can_log) {
                serverLog(LL_WARNING, "Error writing to the AOF file: %s",
                          strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                serverLog(LL_WARNING, "Short write while writing to "
                                      "the AOF file: (nwritten=%lld, "
                                      "expected=%lld)",
                          (long long) nwritten,
                          (long long) sdslen(server.aof_buf));
            }

            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    serverLog(LL_WARNING, "Could not remove short write "
                                          "from the append-only file.  Redis may refuse "
                                          "to load the AOF the next time it starts.  "
                                          "ftruncate: %s", strerror(errno));
                }
            } else {
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            serverLog(LL_WARNING,
                      "Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            server.aof_last_write_status = C_ERR;

            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf, nwritten, -1);
            }
            return;
        }
    } else {
        if (server.aof_last_write_status == C_ERR) {
            serverLog(LL_WARNING,
                      "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = C_OK;
        }
    }
    server.aof_current_size += nwritten;
    if ((sdslen(server.aof_buf) + sdsavail(server.aof_buf)) < 4000) {
        sdsclear(server.aof_buf);
    } else {
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }
    // 有在重写或者RDB重写的直接不刷盘[会丢数据]
    if (server.aof_no_fsync_on_rewrite &&
        (server.aof_child_pid != -1 || server.rdb_child_pid != -1))
        return;

    // 处理always配置
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        latencyStartMonitor(latency);
        aof_fsync(server.aof_fd);
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always", latency);
        server.aof_last_fsync = server.unixtime;
        // 处理everysec配置当超过了一秒则执行fsync
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        // 如果没有进程在执行(启子线程执行),不然说明在执行直接等一下次时间事件
        if (!sync_in_progress) aof_background_fsync(server.aof_fd);
        server.aof_last_fsync = server.unixtime;
    }
}

/**
 * 普通命令
 *
 * @param dst
 * @param argc
 * @param argv
 * @return
 */
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    buf[0] = '*';
    len = 1 + ll2string(buf + 1, sizeof(buf) - 1, argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst, buf, len);

    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);
        buf[0] = '$';
        len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst, buf, len);
        dst = sdscatlen(dst, o->ptr, sdslen(o->ptr));
        dst = sdscatlen(dst, "\r\n", 2);
        decrRefCount(o);
    }
    return dst;
}

sds catAppendOnlyExpireAtCommand(
        sds buf,
        struct redisCommand *cmd,
        robj *key,
        robj *seconds) {

    long long when;
    robj *argv[3];

    /* Make sure we can use strtoll */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr, NULL, 10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand) {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        when += mstime();
    }
    decrRefCount(seconds);

    argv[0] = createStringObject("PEXPIREAT", 9);
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    decrRefCount(argv[0]);
    decrRefCount(argv[2]);
    return buf;
}

/**
 *
 * @param cmd redis命令
 * @param dictid
 * @param argv 参数
 * @param argc 参数
 */
void feedAppendOnlyFile(
        struct redisCommand *cmd,
        int dictid,
        robj **argv,
        int argc) {

    sds buf = sdsempty();
    robj *tmpargv[3];
    if (dictid != server.aof_selected_db) {
        char seldb[64];
        snprintf(seldb, sizeof(seldb), "%d", dictid);
        // 追加选择数据库语句
        buf = sdscatprintf(buf, "*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n", (unsigned long) strlen(seldb), seldb);
        server.aof_selected_db = dictid;
    }
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand || cmd->proc == expireatCommand) {
        // 解析特殊标签
        // 追加expire等命令
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
        // 将setex命令等解析
    } else if (cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        tmpargv[0] = createStringObject("SET", 3);
        tmpargv[1] = argv[1];
        tmpargv[2] = argv[3];
        buf = catAppendOnlyGenericCommand(buf, 3, tmpargv);
        decrRefCount(tmpargv[0]);
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);
    } else if (cmd->proc == setCommand && argc > 3) {
        int i;
        robj *exarg = NULL, *pxarg = NULL;
        buf = catAppendOnlyGenericCommand(buf, 3, argv);
        for (i = 3; i < argc; i++) {
            if (!strcasecmp(argv[i]->ptr, "ex")) exarg = argv[i + 1];
            if (!strcasecmp(argv[i]->ptr, "px")) pxarg = argv[i + 1];
        }
        serverAssert(!(exarg && pxarg));
        if (exarg)
            buf = catAppendOnlyExpireAtCommand(buf, server.expireCommand, argv[1],
                                               exarg);
        if (pxarg)
            buf = catAppendOnlyExpireAtCommand(buf, server.pexpireCommand, argv[1],
                                               pxarg);
    } else {
        buf = catAppendOnlyGenericCommand(buf, argc, argv);
    }
    // 开启AOF情况下进行追加的操作
    if (server.aof_state == AOF_ON)
        // 追加到AOF缓冲区中
        server.aof_buf = sdscatlen(server.aof_buf, buf, sdslen(buf));

    // 代表在AOF重写过程中
    if (server.aof_child_pid != -1)
        aofRewriteBufferAppend((unsigned char *) buf, sdslen(buf));

    // 清空buf内存
    sdsfree(buf);
}

struct client *createFakeClient(void) {
    struct client *c = zmalloc(sizeof(*c));

    selectDb(c, 0);
    c->fd = -1;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->bufpos = 0;
    c->flags = 0;
    c->btype = BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    listSetFreeMethod(c->reply, decrRefCountVoid);
    listSetDupMethod(c->reply, dupClientReplyValue);
    initClientMultiState(c);
    return c;
}

void freeFakeClientArgv(struct client *c) {
    int j;

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
}

void freeFakeClient(struct client *c) {
    sdsfree(c->querybuf);
    listRelease(c->reply);
    listRelease(c->watched_keys);
    freeClientMultiState(c);
    zfree(c);
}

/**
 * 加载AOF文件
 *
 * @param filename
 */
int loadAppendOnlyFile(
        char *filename) {

    struct client *fakeClient;
    FILE *fp = fopen(filename, "r");
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */

    if (fp == NULL) {
        serverLog(LL_WARNING, "Fatal error: can't open the append log file for reading: %s", strerror(errno));
        exit(1);
    }

    if (fp && redis_fstat(fileno(fp), &sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        fclose(fp);
        return C_ERR;
    }

    server.aof_state = AOF_OFF;

    fakeClient = createFakeClient();
    startLoading(fp);

    char sig[5];
    if (fread(sig, 1, 5, fp) != 5 || memcmp(sig, "REDIS", 5) != 0) {
        /* No RDB preamble, seek back at 0 offset. */
        if (fseek(fp, 0, SEEK_SET) == -1) goto readerr;
    } else {
        /* RDB preamble. Pass loading the RDB functions. */
        rio rdb;

        serverLog(LL_NOTICE, "Reading RDB preamble from AOF file...");
        if (fseek(fp, 0, SEEK_SET) == -1) goto readerr;
        rioInitWithFile(&rdb, fp);
        if (rdbLoadRio(&rdb, NULL) != C_OK) {
            serverLog(LL_WARNING, "Error reading the RDB preamble of the AOF file, AOF loading aborted");
            goto readerr;
        } else {
            serverLog(LL_NOTICE, "Reading the remaining AOF tail...");
        }
    }

    /* Read the actual AOF file, in REPL format, command by command. */
    while (1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
        }

        if (fgets(buf, sizeof(buf), fp) == NULL) {
            if (feof(fp))
                break;
            else
                goto readerr;
        }
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        argc = atoi(buf + 1);
        if (argc < 1) goto fmterr;

        argv = zmalloc(sizeof(robj *) * argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            if (fgets(buf, sizeof(buf), fp) == NULL) {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            if (buf[0] != '$') goto fmterr;
            len = strtol(buf + 1, NULL, 10);
            argsds = sdsnewlen(NULL, len);
            if (len && fread(argsds, len, 1, fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            argv[j] = createObject(OBJ_STRING, argsds);
            if (fread(buf, 2, 1, fp) == 0) {
                fakeClient->argc = j + 1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr; /* discard CRLF */
            }
        }

        /* Command lookup */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING, "Unknown command '%s' reading the append only file", (char *) argv[0]->ptr);
            exit(1);
        }

        /* Run the command in the context of a fake client */
        fakeClient->cmd = cmd;
        cmd->proc(fakeClient);

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 && listLength(fakeClient->reply) == 0);
        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, log error and quit. */
    if (fakeClient->flags & CLIENT_MULTI) goto uxeof;

    loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */
    fclose(fp);
    freeFakeClient(fakeClient);
    server.aof_state = old_aof_state;
    stopLoading();
    aofUpdateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    return C_OK;

    readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        serverLog(LL_WARNING, "Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

    uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING, "!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING, "!!! Truncating the AOF at offset %llu !!!",
                  (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename, valid_up_to) == -1) {
            if (valid_up_to == -1) {
                serverLog(LL_WARNING, "Last valid command offset is invalid");
            } else {
                serverLog(LL_WARNING, "Error truncating the AOF file: %s",
                          strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd, 0, SEEK_END) == -1) {
                serverLog(LL_WARNING, "Can't seek the end of the AOF file: %s",
                          strerror(errno));
            } else {
                serverLog(LL_WARNING,
                          "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,
              "Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

    fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    serverLog(LL_WARNING,
              "Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rioWriteBulkLongLong(r, (long) obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r, obj->ptr, sdslen(obj->ptr));
    } else {
        serverPanic("Unknown string encoding");
    }
}

int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *list = o->ptr;
        quicklistIter *li = quicklistGetIterator(list, AL_START_HEAD);
        quicklistEntry entry;

        while (quicklistNext(li, &entry)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;
                if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
                if (rioWriteBulkString(r, "RPUSH", 5) == 0) return 0;
                if (rioWriteBulkObject(r, key) == 0) return 0;
            }

            if (entry.value) {
                if (rioWriteBulkString(r, (char *) entry.value, entry.sz) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r, entry.longval) == 0) return 0;
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        quicklistReleaseIterator(li);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 1;
}

int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    if (o->encoding == OBJ_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while (intsetGet(o->ptr, ii++, &llval)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
                if (rioWriteBulkString(r, "SADD", 4) == 0) return 0;
                if (rioWriteBulkObject(r, key) == 0) return 0;
            }
            if (rioWriteBulkLongLong(r, llval) == 0) return 0;
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r, '*', 2 + cmd_items) == 0) return 0;
                if (rioWriteBulkString(r, "SADD", 4) == 0) return 0;
                if (rioWriteBulkObject(r, key) == 0) return 0;
            }
            if (rioWriteBulkString(r, ele, sdslen(ele)) == 0) return 0;
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown set encoding");
    }
    return 1;
}

int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl, 0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        while (eptr != NULL) {
            serverAssert(ziplistGet(eptr, &vstr, &vlen, &vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
                if (rioWriteBulkString(r, "ZADD", 4) == 0) return 0;
                if (rioWriteBulkObject(r, key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r, score) == 0) return 0;
            if (vstr != NULL) {
                if (rioWriteBulkString(r, (char *) vstr, vlen) == 0) return 0;
            } else {
                if (rioWriteBulkLongLong(r, vll) == 0) return 0;
            }
            zzlNext(zl, &eptr, &sptr);
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
                if (rioWriteBulkString(r, "ZADD", 4) == 0) return 0;
                if (rioWriteBulkObject(r, key) == 0) return 0;
            }
            if (rioWriteBulkDouble(r, *score) == 0) return 0;
            if (rioWriteBulkString(r, ele, sdslen(ele)) == 0) return 0;
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted zset encoding");
    }
    return 1;
}

static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            return rioWriteBulkString(r, (char *) vstr, vlen);
        else
            return rioWriteBulkLongLong(r, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        return rioWriteBulkString(r, value, sdslen(value));
    }

    serverPanic("Unknown hash encoding");
    return 0;
}

int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        if (count == 0) {
            int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                            AOF_REWRITE_ITEMS_PER_CMD : items;

            if (rioWriteBulkCount(r, '*', 2 + cmd_items * 2) == 0) return 0;
            if (rioWriteBulkString(r, "HMSET", 5) == 0) return 0;
            if (rioWriteBulkObject(r, key) == 0) return 0;
        }

        if (rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY) == 0) return 0;
        if (rioWriteHashIteratorCursor(r, hi, OBJ_HASH_VALUE) == 0) return 0;
        if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

int rewriteModuleObject(rio *r, robj *key, robj *o) {
    RedisModuleIO io;
    moduleValue *mv = o->ptr;
    moduleType *mt = mv->type;
    moduleInitIOContext(io, mt, r);
    mt->aof_rewrite(&io, key, mv->value);
    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    return io.error ? 0 : 1;
}

/**
 * AOF重写读取差异
 */
ssize_t aofReadDiffFromParent(
        void) {

    char buf[65536];
    ssize_t nread, total = 0;

    // 读取父进程通道中的数据
    while ((nread = read(server.aof_pipe_read_data_from_parent, buf, sizeof(buf))) > 0) {
        server.aof_child_diff = sdscatlen(server.aof_child_diff, buf, nread);
        total += nread;
    }
    return total;
}

/**
 * 写入AOF文件
 *
 * @param aof
 */
int rewriteAppendOnlyFileRio(
        rio *aof) {

    dictIterator *di = NULL;
    dictEntry *de;
    size_t processed = 0;
    long long now = mstime();
    int j;

    // 遍历数据库
    for (j = 0; j < server.dbnum; j++) {
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db + j;
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        di = dictGetSafeIterator(d);

        // 切换到新的DB
        if (rioWrite(aof, selectcmd, sizeof(selectcmd) - 1) == 0) goto werr;
        if (rioWriteBulkLongLong(aof, j) == 0) goto werr;

        // 查找此库中的key和value
        while ((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            keystr = dictGetKey(de);
            o = dictGetVal(de);
            initStaticStringObject(key, keystr);

            expiretime = getExpire(db, &key);

            // 过期的直接忽略
            if (expiretime != -1 && expiretime < now) continue;

            // 重写这些命令
            /* Save the key and associated value */
            if (o->type == OBJ_STRING) {
                /* Emit a SET command */
                char cmd[] = "*3\r\n$3\r\nSET\r\n";
                if (rioWrite(aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
                /* Key and value */
                if (rioWriteBulkObject(aof, &key) == 0) goto werr;
                if (rioWriteBulkObject(aof, o) == 0) goto werr;
            } else if (o->type == OBJ_LIST) {
                if (rewriteListObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_SET) {
                if (rewriteSetObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_ZSET) {
                if (rewriteSortedSetObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_HASH) {
                if (rewriteHashObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_MODULE) {
                if (rewriteModuleObject(aof, &key, o) == 0) goto werr;
            } else {
                serverPanic("Unknown object type");
            }
            /* Save the expire time */
            if (expiretime != -1) {
                char cmd[] = "*3\r\n$9\r\nPEXPIREAT\r\n";
                if (rioWrite(aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
                if (rioWriteBulkObject(aof, &key) == 0) goto werr;
                if (rioWriteBulkLongLong(aof, expiretime) == 0) goto werr;
            }
            // 实时读取父进程传入的新数据
            if (aof->processed_bytes > processed + AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = aof->processed_bytes;
                aofReadDiffFromParent();
            }
        }
        dictReleaseIterator(di);
        di = NULL;
    }
    return C_OK;

    werr:
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/**
 * 重写AOF文件
 *
 * @param filename 文件名
 */
int rewriteAppendOnlyFile(
        char *filename) {

    rio aof;
    FILE *fp;
    char tmpfile[256];
    char byte;

    snprintf(tmpfile, 256, "temp-rewriteaof-%d.aof", (int) getpid());
    fp = fopen(tmpfile, "w");
    if (!fp) {
        serverLog(LL_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return C_ERR;
    }
    server.aof_child_diff = sdsempty();
    // 根据文件名初始化I/O
    rioInitWithFile(&aof, fp);
    if (server.aof_rewrite_incremental_fsync)
        rioSetAutoSync(&aof, AOF_AUTOSYNC_BYTES);

    // RDB和AOF混合模式
    if (server.aof_use_rdb_preamble) {
        int error;
        if (rdbSaveRio(&aof, &error, RDB_SAVE_AOF_PREAMBLE, NULL) == C_ERR) {
            errno = error;
            goto werr;
        }
    } else {
        if (rewriteAppendOnlyFileRio(&aof) == C_ERR) goto werr;
    }

    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;

    int nodata = 0;
    mstime_t start = mstime();
    // 超过1秒并且节点小于20等待
    while (mstime() - start < 1000 && nodata < 20) {
        // 在1ms之内,查看从父进程读数据的fd是否变成可读的,若不可读则aeWait()函数返回0,一共最多等待20ms
        if (aeWait(server.aof_pipe_read_data_from_parent, AE_READABLE, 1) <= 0) {
            nodata++;
            continue;
        }
        nodata = 0;
        // 读取父节点传进来的数据
        aofReadDiffFromParent();
    }

    // 发送命令给父节点要求停止发送差异
    if (write(server.aof_pipe_write_ack_to_parent, "!", 1) != 1) goto werr;
    if (anetNonBlock(NULL, server.aof_pipe_read_ack_from_parent) != ANET_OK)
        goto werr;
    if (syncRead(server.aof_pipe_read_ack_from_parent, &byte, 1, 5000) != 1 ||
        byte != '!')
        goto werr;
    serverLog(LL_NOTICE, "Parent agreed to stop sending diffs. Finalizing AOF...");

    // 读取不同
    aofReadDiffFromParent();
    serverLog(LL_NOTICE, "Concatenating %.2f MB of AOF diff received from parent.",
              (double) sdslen(server.aof_child_diff) / (1024 * 1024));
    // 将差异写进重写后的AOF文件
    if (rioWrite(&aof, server.aof_child_diff, sdslen(server.aof_child_diff)) == 0)
        goto werr;
    // 刷文件
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    if (fclose(fp) == EOF) goto werr;

    // 重新命名文件
    if (rename(tmpfile, filename) == -1) {
        serverLog(LL_WARNING, "Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        return C_ERR;
    }
    serverLog(LL_NOTICE, "SYNC append only file rewrite performed");
    return C_OK;

    werr:
    serverLog(LL_WARNING, "Write error writing append only file on disk: %s", strerror(errno));
    fclose(fp);
    unlink(tmpfile);
    return C_ERR;
}

/**
 * 子进程可读通道[父进程执行]
 *
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void aofChildPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask) {
    char byte;
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    if (read(fd, &byte, 1) == 1 && byte == '!') {
        serverLog(LL_NOTICE, "AOF rewrite child asks to stop sending diffs.");
        server.aof_stop_sending_diff = 1;
        if (write(server.aof_pipe_write_ack_to_child, "!", 1) != 1) {
            /* If we can't send the ack, inform the user, but don't try again
             * since in the other side the children will use a timeout if the
             * kernel can't buffer our write, or, the children was
             * terminated. */
            serverLog(LL_WARNING, "Can't send ACK to AOF child: %s", strerror(errno));
        }
    }
    // 删除可读事件,这样就无法读取
    aeDeleteFileEvent(server.el, server.aof_pipe_read_ack_from_child, AE_READABLE);
}

/**
 * 开启父进程与子进程之间的通道
 */
int aofCreatePipes(void) {
    int fds[6] = {-1, -1, -1, -1, -1, -1};
    int j;

    if (pipe(fds) == -1) goto error; /* parent -> children data. */
    if (pipe(fds + 2) == -1) goto error; /* children -> parent ack. */
    if (pipe(fds + 4) == -1) goto error; /* children -> parent ack. */
    if (anetNonBlock(NULL, fds[0]) != ANET_OK) goto error;
    if (anetNonBlock(NULL, fds[1]) != ANET_OK) goto error;
    if (aeCreateFileEvent(server.el, fds[2], AE_READABLE, aofChildPipeReadable, NULL) == AE_ERR) goto error;

    server.aof_pipe_write_data_to_child = fds[1];
    server.aof_pipe_read_data_from_parent = fds[0];
    server.aof_pipe_write_ack_to_parent = fds[3];
    server.aof_pipe_read_ack_from_child = fds[2];
    server.aof_pipe_write_ack_to_child = fds[5];
    server.aof_pipe_read_ack_from_parent = fds[4];
    server.aof_stop_sending_diff = 0;
    return C_OK;

    error:
    serverLog(LL_WARNING, "Error opening /setting AOF rewrite IPC pipes: %s",
              strerror(errno));
    for (j = 0; j < 6; j++) if (fds[j] != -1) close(fds[j]);
    return C_ERR;
}

void aofClosePipes(void) {
    aeDeleteFileEvent(server.el, server.aof_pipe_read_ack_from_child, AE_READABLE);
    aeDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE);
    close(server.aof_pipe_write_data_to_child);
    close(server.aof_pipe_read_data_from_parent);
    close(server.aof_pipe_write_ack_to_parent);
    close(server.aof_pipe_read_ack_from_child);
    close(server.aof_pipe_write_ack_to_child);
    close(server.aof_pipe_read_ack_from_parent);
}

/**
 * 重写AOF文件
 */
int rewriteAppendOnlyFileBackground(
        void) {

    pid_t childpid;
    long long start;
    // 有aof或者rdb子进程执行直接返回
    if (server.aof_child_pid != -1 || server.rdb_child_pid != -1) return C_ERR;
    // 创建子进程与父进程之间的通道[管道技术]
    if (aofCreatePipes() != C_OK) return C_ERR;
    openChildInfoPipe();
    start = ustime();
    // 创建子进程
    if ((childpid = fork()) == 0) {
        // 临时文件
        char tmpfile[256];
        closeListeningSockets(0);
        redisSetProcTitle("redis-aof-rewrite");
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int) getpid());
        // 重写AOF文件
        if (rewriteAppendOnlyFile(tmpfile) == C_OK) {
            size_t private_dirty = zmalloc_get_private_dirty(-1);
            if (private_dirty) {
                serverLog(LL_NOTICE,
                          "AOF rewrite: %zu MB of memory used by copy-on-write",
                          private_dirty / (1024 * 1024));
            }

            server.child_info_data.cow_size = private_dirty;
            // 发送子进程AOF
            sendChildInfo(CHILD_INFO_TYPE_AOF);
            exitFromChild(0);
        } else {
            exitFromChild(1);
        }
    } else {
        server.stat_fork_time = ustime() - start;
        server.stat_fork_rate = (double) zmalloc_used_memory() * 1000000 / server.stat_fork_time /
                                (1024 * 1024 * 1024); /* GB per second. */
        latencyAddSampleIfNeeded("fork", server.stat_fork_time / 1000);
        if (childpid == -1) {
            closeChildInfoPipe();
            serverLog(LL_WARNING,
                      "Can't rewrite append only file in background: fork: %s",
                      strerror(errno));
            aofClosePipes();
            return C_ERR;
        }
        serverLog(LL_NOTICE,
                  "Background append only file rewriting started by pid %d", childpid);
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);
        server.aof_child_pid = childpid;
        updateDictResizePolicy();
        server.aof_selected_db = -1;
        replicationScriptCacheFlush();
        return C_OK;
    }
    return C_OK; /* unreached */
}

/**
 * AOF文件重写命令
 *
 * @param c 客户端
 */
void bgrewriteaofCommand(
        client *c) {

    // 正在执行
    if (server.aof_child_pid != -1) {
        addReplyError(c, "Background append only file rewriting already in progress");
        // 有RDB在执行
    } else if (server.rdb_child_pid != -1) {
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c, "Background append only file rewriting scheduled");
    } else if (rewriteAppendOnlyFileBackground() == C_OK) {
        addReplyStatus(c, "Background append only file rewriting started");
    } else {
        addReply(c, shared.err);
    }
}

/**
 * 删除临时文件
 *
 * @param childpid
 */
void aofRemoveTempFile(
        pid_t childpid) {

    char tmpfile[256];

    snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int) childpid);
    unlink(tmpfile);
}

void aofUpdateCurrentSize(
        void) {

    struct redis_stat sb;
    mstime_t latency;

    latencyStartMonitor(latency);
    if (redis_fstat(server.aof_fd, &sb) == -1) {
        serverLog(LL_WARNING, "Unable to obtain the AOF file length. stat: %s",
                  strerror(errno));
    } else {
        server.aof_current_size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat", latency);
}

/**
 * 根据子进程是否正常退出以及信号
 *
 * @param exitcode
 * @param bysignal
 */
void backgroundRewriteDoneHandler(
        int exitcode,
        int bysignal) {

    if (!bysignal && exitcode == 0) {
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();
        mstime_t latency;
        serverLog(LL_NOTICE, "Background AOF rewrite terminated with success");
        latencyStartMonitor(latency);
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int) server.aof_child_pid);
        newfd = open(tmpfile, O_WRONLY | O_APPEND);
        if (newfd == -1) {
            serverLog(LL_WARNING, "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }
        // 将AOF缓冲区中剩余的东西写入文件中
        if (aofRewriteBufferWrite(newfd) == -1) {
            serverLog(LL_WARNING, "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rewrite-diff-write", latency);
        serverLog(LL_NOTICE, "Residual parent diff successfully flushed to the rewritten AOF (%.2f MB)",
                  (double) aofRewriteBufferSize() / (1024 * 1024));
        // aof不可用
        if (server.aof_fd == -1) {
            oldfd = open(server.aof_filename, O_RDONLY | O_NONBLOCK);
        } else {
            oldfd = -1;
        }
        latencyStartMonitor(latency);
        if (rename(tmpfile, server.aof_filename) == -1) {
            serverLog(LL_WARNING, "Error trying to rename the temporary AOF file %s into %s: %s", tmpfile,
                      server.aof_filename, strerror(errno));
            close(newfd);
            if (oldfd != -1) close(oldfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rename", latency);
        if (server.aof_fd == -1) {
            close(newfd);
        } else {
            // 替换文件
            oldfd = server.aof_fd;
            server.aof_fd = newfd;
            if (server.aof_fsync == AOF_FSYNC_ALWAYS)
                aof_fsync(newfd);
                // 使用子线程
            else if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
                aof_background_fsync(newfd);
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */
            aofUpdateCurrentSize();
            server.aof_rewrite_base_size = server.aof_current_size;

            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = C_OK;

        serverLog(LL_NOTICE, "Background AOF rewrite finished successfully");
        if (server.aof_state == AOF_WAIT_REWRITE)
            server.aof_state = AOF_ON;

        // 创建子线程删除老文件
        if (oldfd != -1) bioCreateBackgroundJob(BIO_CLOSE_FILE, (void *) (long) oldfd, NULL, NULL);

        serverLog(LL_VERBOSE,
                  "Background AOF rewrite signal handler took %lldus", ustime() - now);
    } else if (!bysignal && exitcode != 0) {
        if (bysignal != SIGUSR1)
            server.aof_lastbgrewrite_status = C_ERR;
        serverLog(LL_WARNING,
                  "Background AOF rewrite terminated with error");
    } else {
        server.aof_lastbgrewrite_status = C_ERR;

        serverLog(LL_WARNING,
                  "Background AOF rewrite terminated by signal %d", bysignal);
    }

    cleanup:
    aofClosePipes();
    aofRewriteBufferReset();
    aofRemoveTempFile(server.aof_child_pid);
    server.aof_child_pid = -1;
    server.aof_rewrite_time_last = time(NULL) - server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    if (server.aof_state == AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}