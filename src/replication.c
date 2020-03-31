/**
 * 主从复制
 *
 *    主动触发[从服务器执行]
 *
 *       slaveof masterIp masterPort
 *
 *       salveof no one断开连接的命令[在主节点宕机时,从节点要先断开连接,主节点才能进行重启]
 *
 *    配置文件
 *
 *       在配置文件中加入slaveOf masterIp masterPort
 *
 *    流程
 *
 *       slaveof[异步,从节点保存主节点的IP地址以及端口号并清除自己之前任何相关的复制信息并将当前服务器状态变成REPL_STATE_CONNECT(重新连接主节点的状态)]
 *
 *       主从复制具体操作在时间事件中执行[每秒调用replicationCron函数]
 *
 *          1、如果从节点服务器状态为REPL_STATE_CONNECT(重新连接主节点的状态),主从建立非阻塞网络连接,同时创建文件可写可读事件处理连接[由syncWithMaster函数处理],并将从节点服务器状态变更为REPL_STATE_CONNECTING(连接中状态)
 *
 *          2、如果从节点服务器状态为REPL_STATE_CONNECTING(连接中状态),发送ping命令,接收主节点返回的pong回应,并将从节点服务器的状态变更为REPL_STATE_SEND_AUTH(权限认证状态)
 *
 *                检测主从节点之间的网络是否可用
 *
 *                检查主从节点当前是否接受处理命令
 *
 *          3、如果从节点服务器状态为REPL_STATE_SEND_AUTH(权限认证状态),根据配置文件中是否配置了需要权限认证,配置了发送权限认证命令并接收主节点响应[没配置直接更改服务器状态为发送端口状态],并将从节点服务器更改为REPL_STATE_SEND_PORT(发送端口状态)
 *
 *          4、如果从节点服务器状态为REPL_STATE_SEND_PORT(发送端口状态),发送端口号并接收主节点响应,并将从节点服务器状态设置为REPL_STATE_SEND_IP(发送IP地址状态)
 *
 *          5、如果从节点服务器状态为REPL_STATE_SEND_IP[发送IP地址状态],发送IP地址并接收主节点响应,并将从节点服务器状态设置为REPL_STATE_SEND_CAPA(发送能力状态)
 *
 *          6、如果从节点服务器状态为REPL_STATE_SEND_CAPA(发送能力状态),发送CAPA命令并接收主节点响应,并将从节点服务器状态设置为REPL_STATE_SEND_PSYNC(发送PSYNC同步命令)
 *
 *          7、如果从节点服务器状态为REPL_STATE_SEND_PSYNC(发送PSYNC同步命令),发送PSYNC命令,主节点通过syncCommand方法解析[主从复制核心部分]决定是进行全量复制还是部分复制
 *
 *                从节点拼接PSYNC命令发送给主节点,等待主节点的回复
 *
 *                   主从节点之间第一次进行连接[PSYNC ? -1]
 *
 *                      无任何主节点信息信息,此时命令变成PSYNC ? -1,代表不知道主服务器运行期ID以及偏移量,组装成命令发送给主节点
 *
 *                   主从节点之间进行过连接[PSYNC 主节点运行期ID 需要复制的起始偏移量]
 *
 *                      获取之前保存的主节点运行期ID和需要进行复制的偏移量起始位组装成命令发送给主节点
 *
 *                主节点接收处理从节点传来的PSYNC命令,通过syncCommand命令进行解析[老版本没有PSYNC命令只存在SYNC命令(全量复制)]
 *
 *                   PSYNC ? -1或者运行期ID匹配不上或者复制偏移量比积压缓冲区中最小值还小根据当前主服务器RDB情况返回FULLRESYNC[全量复制]
 *
 *                     1、主服务器在执行RDB并且是刷盘的方式,轮训当前的从服务器,判断当前是否有从服务器的状态为等待BGSAVE结束,存在代表此时主服务器执行的RDB可以共用[共用输出缓冲区中的数据](通过内存BUF拷贝实现),并且将从服务器的状态从等待RDB开始变更为等待RDB结束并且发送主节点运行ID以及复制偏移量,否则需要等待此次RDB执行完再此执行RDB
 *
 *                     2、主服务器不在执行RDB并且也不在执行AOF,则主服务器直接进行RDB操作,并且将从服务器的状态从等待RDB开始变更为等待RDB结束并且发送主节点运行ID以及复制偏移量
 *
 *                     3、从服务器读取到主节点返回的FULLRESYNC(全量同步)以及相关的运行时ID和偏移量进行保存,开启临时文件并且创建一个读监听器事件[用于读取主节点发送的文件信息,事件由readSyncBulkPayload处理]并且从节点服务器状态为REPL_STATE_TRANSFER[接收RDB文件状态]
 *
 *                           readSyncBulkPayload主要读取到主节点传送的数据并清空当前的数据,加载主节点传入的数据[RDB文件]
 *
 *                     4、主服务器执行RDB文件,执行完成后只读打开RDB文件,并设置从节点服务器状态为SLAVE_STATE_SEND_BULK,注册写事件[将RDB文件写给从服务器,事件由sendBulkToSlave执行],此时RDB文件已经发送出去
 *
 *                           sendBulkToSlave主要将主服务器生成的RDB文件写入从服务器客户端资源符中并且将从服务器状态变更为SLAVE_STATE_ONLINE
 *
 *                     5、主服务器最后将client输出缓冲区的以回复的方式写入从服务器保证主从服务器之间的最终同步(从服务器执行这部分命令)[这部分数据在执行命令时会添加到复制输出缓冲区(为了保证主从最终同步)以及积压缓冲区中(默认1M用于判断是否部分复制)]
 *
 *                   PSYNC masterRunningId offset运行期ID匹配,同时偏移量在复制积压缓冲区中也能匹配到,此时主服务器会返回给从服务器CONTINUE[部分复制]
 *
 *                     将积压缓冲区中的数据写入以回复的方式客户端输出缓冲区中返回给子节点,子节点读取执行即可
 *
 *            8、主从复制完成后,每次master执行完命令将命令传输给从服务器
 *
 * 主从节点之间维护心跳和偏移量检查机制,保证主从节点通信正常和数据一致,通过对比主从节点的复制偏移量,可以判断主从节点数据是否一致
 */

#include "server.h"

#include <sys/time.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/stat.h>

void replicationDiscardCachedMaster(void);

void replicationResurrectCachedMaster(int newfd);

void replicationSendAck(void);

void putSlaveOnline(client *slave);

int cancelReplicationHandshake(void);

char *replicationGetSlaveName(client *c) {
    static char buf[NET_PEER_ID_LEN];
    char ip[NET_IP_STR_LEN];

    ip[0] = '\0';
    buf[0] = '\0';
    if (c->slave_ip[0] != '\0' ||
        anetPeerToString(c->fd, ip, sizeof(ip), NULL) != -1) {
        /* Note that the 'ip' buffer is always larger than 'c->slave_ip' */
        if (c->slave_ip[0] != '\0') memcpy(ip, c->slave_ip, sizeof(c->slave_ip));

        if (c->slave_listening_port)
            anetFormatAddr(buf, sizeof(buf), ip, c->slave_listening_port);
        else
            snprintf(buf, sizeof(buf), "%s:<unknown-slave-port>", ip);
    } else {
        snprintf(buf, sizeof(buf), "client id #%llu",
                 (unsigned long long) c->id);
    }
    return buf;
}

void createReplicationBacklog(void) {
    serverAssert(server.repl_backlog == NULL);
    server.repl_backlog = zmalloc(server.repl_backlog_size);
    server.repl_backlog_histlen = 0;
    server.repl_backlog_idx = 0;

    /* We don't have any data inside our buffer, but virtually the first
     * byte we have is the next byte that will be generated for the
     * replication stream. */
    server.repl_backlog_off = server.master_repl_offset + 1;
}

/* This function is called when the user modifies the replication backlog
 * size at runtime. It is up to the function to both update the
 * server.repl_backlog_size and to resize the buffer and setup it so that
 * it contains the same data as the previous one (possibly less data, but
 * the most recent bytes, or the same data and more free space in case the
 * buffer is enlarged). */
void resizeReplicationBacklog(long long newsize) {
    if (newsize < CONFIG_REPL_BACKLOG_MIN_SIZE)
        newsize = CONFIG_REPL_BACKLOG_MIN_SIZE;
    if (server.repl_backlog_size == newsize) return;

    server.repl_backlog_size = newsize;
    if (server.repl_backlog != NULL) {
        /* What we actually do is to flush the old buffer and realloc a new
         * empty one. It will refill with new data incrementally.
         * The reason is that copying a few gigabytes adds latency and even
         * worse often we need to alloc additional space before freeing the
         * old buffer. */
        zfree(server.repl_backlog);
        server.repl_backlog = zmalloc(server.repl_backlog_size);
        server.repl_backlog_histlen = 0;
        server.repl_backlog_idx = 0;
        /* Next byte we have is... the next since the buffer is empty. */
        server.repl_backlog_off = server.master_repl_offset + 1;
    }
}

void freeReplicationBacklog(
        void) {

    serverAssert(listLength(server.slaves) == 0);
    zfree(server.repl_backlog);
    server.repl_backlog = NULL;
}

/**
 * 添加数据进复制积压缓冲区
 *
 * @param ptr
 * @param len
 */
void feedReplicationBacklog(
        void *ptr,
        size_t len) {

    unsigned char *p = ptr;

    server.master_repl_offset += len;
    while (len) {
        size_t thislen = server.repl_backlog_size - server.repl_backlog_idx;
        if (thislen > len) thislen = len;
        memcpy(server.repl_backlog + server.repl_backlog_idx, p, thislen);
        server.repl_backlog_idx += thislen;
        if (server.repl_backlog_idx == server.repl_backlog_size)
            server.repl_backlog_idx = 0;
        len -= thislen;
        p += thislen;
        server.repl_backlog_histlen += thislen;
    }
    if (server.repl_backlog_histlen > server.repl_backlog_size)
        server.repl_backlog_histlen = server.repl_backlog_size;
    /* Set the offset of the first byte we have in the backlog. */
    server.repl_backlog_off = server.master_repl_offset -
                              server.repl_backlog_histlen + 1;
}

void feedReplicationBacklogWithObject(
        robj *o) {

    char llstr[LONG_STR_SIZE];
    void *p;
    size_t len;

    if (o->encoding == OBJ_ENCODING_INT) {
        len = ll2string(llstr, sizeof(llstr), (long) o->ptr);
        p = llstr;
    } else {
        len = sdslen(o->ptr);
        p = o->ptr;
    }
    feedReplicationBacklog(p, len);
}

/**
 * 每次执行命令都会将命令传输给子节点
 *
 * @param slaves
 * @param dictid
 * @param argv
 * @param argc
 */
void replicationFeedSlaves(
        list *slaves,
        int dictid,
        robj **argv,
        int argc) {

    listNode *ln;
    listIter li;
    int j, len;
    char llstr[LONG_STR_SIZE];

    // 不是最顶层的master
    if (server.masterhost != NULL) return;
    if (server.repl_backlog == NULL && listLength(slaves) == 0) return;
    serverAssert(!(listLength(slaves) != 0 && server.repl_backlog == NULL));
    // 发送命令给每一个从节点
    if (server.slaveseldb != dictid) {
        robj *selectcmd;
        // 0 <= id < 10,可以使用共享的select命令对象
        if (dictid >= 0 && dictid < PROTO_SHARED_SELECT_CMDS) {
            selectcmd = shared.select[dictid];
        } else {
            int dictid_len;
            dictid_len = ll2string(llstr, sizeof(llstr), dictid);
            // 选择数据库
            selectcmd = createObject(OBJ_STRING,
                                     sdscatprintf(sdsempty(), "*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n", dictid_len,
                                                  llstr));
        }
        // 将select命令添加到backlog中
        if (server.repl_backlog) feedReplicationBacklogWithObject(selectcmd);
        listRewind(slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
            // 添加select命令到当前从节点的回复中
            addReply(slave, selectcmd);
        }
        if (dictid < 0 || dictid >= PROTO_SHARED_SELECT_CMDS)
            decrRefCount(selectcmd);
    }
    server.slaveseldb = dictid;
    // 写入到积压缓冲区中
    if (server.repl_backlog) {
        char aux[LONG_STR_SIZE + 3];
        aux[0] = '*';
        len = ll2string(aux + 1, sizeof(aux) - 1, argc);
        aux[len + 1] = '\r';
        aux[len + 2] = '\n';
        feedReplicationBacklog(aux, len + 3);
        for (j = 0; j < argc; j++) {
            long objlen = stringObjectLen(argv[j]);
            aux[0] = '$';
            len = ll2string(aux + 1, sizeof(aux) - 1, objlen);
            aux[len + 1] = '\r';
            aux[len + 2] = '\n';
            feedReplicationBacklog(aux, len + 3);
            feedReplicationBacklogWithObject(argv[j]);
            feedReplicationBacklog(aux + len + 1, 2);
        }
    }

    // 将命令写到每一个从节点中
    listRewind(slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
        addReplyMultiBulkLen(slave, argc);
        for (j = 0; j < argc; j++)
            // 将所有的参数列表添加到从节点的输出缓冲区
            addReplyBulk(slave, argv[j]);
    }
}

/* This function is used in order to proxy what we receive from our master
 * to our sub-slaves. */
#include <ctype.h>

void replicationFeedSlavesFromMasterStream(list *slaves, char *buf, size_t buflen) {
    listNode *ln;
    listIter li;

    /* Debugging: this is handy to see the stream sent from master
     * to slaves. Disabled with if(0). */
    if (0) {
        printf("%zu:", buflen);
        for (size_t j = 0; j < buflen; j++) {
            printf("%c", isprint(buf[j]) ? buf[j] : '.');
        }
        printf("\n");
    }

    if (server.repl_backlog) feedReplicationBacklog(buf, buflen);
    listRewind(slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        /* Don't feed slaves that are still waiting for BGSAVE to start */
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) continue;
        addReplyString(slave, buf, buflen);
    }
}

void replicationFeedMonitors(client *c, list *monitors, int dictid, robj **argv, int argc) {
    listNode *ln;
    listIter li;
    int j;
    sds cmdrepr = sdsnew("+");
    robj *cmdobj;
    struct timeval tv;

    gettimeofday(&tv, NULL);
    cmdrepr = sdscatprintf(cmdrepr, "%ld.%06ld ", (long) tv.tv_sec, (long) tv.tv_usec);
    if (c->flags & CLIENT_LUA) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d lua] ", dictid);
    } else if (c->flags & CLIENT_UNIX_SOCKET) {
        cmdrepr = sdscatprintf(cmdrepr, "[%d unix:%s] ", dictid, server.unixsocket);
    } else {
        cmdrepr = sdscatprintf(cmdrepr, "[%d %s] ", dictid, getClientPeerId(c));
    }

    for (j = 0; j < argc; j++) {
        if (argv[j]->encoding == OBJ_ENCODING_INT) {
            cmdrepr = sdscatprintf(cmdrepr, "\"%ld\"", (long) argv[j]->ptr);
        } else {
            cmdrepr = sdscatrepr(cmdrepr, (char *) argv[j]->ptr,
                                 sdslen(argv[j]->ptr));
        }
        if (j != argc - 1)
            cmdrepr = sdscatlen(cmdrepr, " ", 1);
    }
    cmdrepr = sdscatlen(cmdrepr, "\r\n", 2);
    cmdobj = createObject(OBJ_STRING, cmdrepr);

    listRewind(monitors, &li);
    while ((ln = listNext(&li))) {
        client *monitor = ln->value;
        addReply(monitor, cmdobj);
    }
    decrRefCount(cmdobj);
}

/**
 * 将复制积压区中的东西写入从节点
 *
 * @param c
 * @param offset
 */
long long addReplyReplicationBacklog(
        client *c,
        long long offset) {

    long long j, skip, len;
    serverLog(LL_DEBUG, "[PSYNC] Slave request offset: %lld", offset);
    if (server.repl_backlog_histlen == 0) {
        serverLog(LL_DEBUG, "[PSYNC] Backlog history len is zero");
        return 0;
    }
    serverLog(LL_DEBUG, "[PSYNC] Backlog size: %lld", server.repl_backlog_size);
    serverLog(LL_DEBUG, "[PSYNC] First byte: %lld", server.repl_backlog_off);
    serverLog(LL_DEBUG, "[PSYNC] History len: %lld", server.repl_backlog_histlen);
    serverLog(LL_DEBUG, "[PSYNC] Current index: %lld", server.repl_backlog_idx);
    // 计算从哪开始写起
    skip = offset - server.repl_backlog_off;
    serverLog(LL_DEBUG, "[PSYNC] Skipping: %lld", skip);
    j = (server.repl_backlog_idx + (server.repl_backlog_size - server.repl_backlog_histlen)) % server.repl_backlog_size;
    serverLog(LL_DEBUG, "[PSYNC] Index of first byte: %lld", j);
    j = (j + skip) % server.repl_backlog_size;
    len = server.repl_backlog_histlen - skip;
    serverLog(LL_DEBUG, "[PSYNC] Reply total length: %lld", len);
    while (len) {
        long long thislen = ((server.repl_backlog_size - j) < len) ? (server.repl_backlog_size - j) : len;
        serverLog(LL_DEBUG, "[PSYNC] addReply() length: %lld", thislen);
        addReplySds(c, sdsnewlen(server.repl_backlog + j, thislen));
        len -= thislen;
        j = 0;
    }
    return server.repl_backlog_histlen - skip;
}

/* Return the offset to provide as reply to the PSYNC command received
 * from the slave. The returned value is only valid immediately after
 * the BGSAVE process started and before executing any other command
 * from clients. */
long long getPsyncInitialOffset(void) {
    return server.master_repl_offset;
}

/**
 * 返回全量同步响应
 *
 * @param slave
 * @param offset
 * @return
 */
int replicationSetupSlaveForFullResync(
        client *slave,
        long long offset) {

    char buf[128];
    int buflen;
    // 偏移量
    slave->psync_initial_offset = offset;
    // 等待bgsave结束
    slave->replstate = SLAVE_STATE_WAIT_BGSAVE_END;
    server.slaveseldb = -1;
    if (!(slave->flags & CLIENT_PRE_PSYNC)) {
        // 拼接返回全量同步
        buflen = snprintf(buf, sizeof(buf), "+FULLRESYNC %s %lld\r\n", server.replid, offset);
        if (write(slave->fd, buf, buflen) != buflen) {
            freeClientAsync(slave);
            return C_ERR;
        }
    }
    return C_OK;
}

/**
 * 尝试进行部分同步
 *
 * @param c
 */
int masterTryPartialResynchronization(
        client *c) {

    long long psync_offset, psync_len;
    char *master_replid = c->argv[1]->ptr;
    char buf[128];
    int buflen;

    // 解析从服务器要求的复制偏移量
    if (getLongLongFromObjectOrReply(c, c->argv[2], &psync_offset, NULL) != C_OK)
        goto need_full_resync;

    // 主节点的运行ID是否和从节点执行PSYNC的参数提供的运行ID相同
    // 如果运行ID发生了改变,则主节点是一个不同的实例,那么就不能进行继续执行原有的复制进程,转换成全量复制
    if (strcasecmp(master_replid, server.replid) &&
        (strcasecmp(master_replid, server.replid2) || psync_offset > server.second_replid_offset)) {
        if (master_replid[0] != '?') {
            if (strcasecmp(master_replid, server.replid) && strcasecmp(master_replid, server.replid2)) {
                serverLog(LL_NOTICE,
                          "Partial resynchronization not accepted: ""Replication ID mismatch (Slave asked for '%s', my ""replication IDs are '%s' and '%s')",
                          master_replid, server.replid, server.replid2);
            } else {
                serverLog(LL_NOTICE, "Partial resynchronization not accepted: "
                                     "Requested offset for second ID was %lld, but I can reply "
                                     "up to %lld", psync_offset, server.second_replid_offset);
            }
        } else {
            serverLog(LL_NOTICE, "Full resync requested by slave %s",
                      replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    // 存在主节点缓存数据
    // 如果psync_offset小于repl_backlog_off,说明backlog所备份的数据的已经太新了,有一些数据被覆盖,则需要进行全量复制
    // 如果psync_offset大于(server.repl_backlog_off + server.repl_backlog_histlen),表示当前backlog的数据不够全,则需要进行全量复制
    if (!server.repl_backlog || psync_offset < server.repl_backlog_off ||
        psync_offset > (server.repl_backlog_off + server.repl_backlog_histlen)) {
        serverLog(LL_NOTICE, "Unable to partial resync with slave %s for lack of backlog (Slave request was: %lld).",
                  replicationGetSlaveName(c), psync_offset);
        if (psync_offset > server.master_repl_offset) {
            serverLog(LL_WARNING,
                      "Warning: slave %s tried to PSYNC with an offset that is greater than the master replication offset.",
                      replicationGetSlaveName(c));
        }
        goto need_full_resync;
    }

    // slave,执行部分同步
    c->flags |= CLIENT_SLAVE;
    c->replstate = SLAVE_STATE_ONLINE;
    c->repl_ack_time = server.unixtime;
    c->repl_put_online_on_ack = 0;
    listAddNodeTail(server.slaves, c);
    if (c->slave_capa & SLAVE_CAPA_PSYNC2) {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE %s\r\n", server.replid);
    } else {
        buflen = snprintf(buf, sizeof(buf), "+CONTINUE\r\n");
    }
    if (write(c->fd, buf, buflen) != buflen) {
        freeClientAsync(c);
        return C_OK;
    }
    psync_len = addReplyReplicationBacklog(c, psync_offset);
    serverLog(LL_NOTICE,
              "Partial resynchronization request from %s accepted. Sending %lld bytes of backlog starting from offset %lld.",
              replicationGetSlaveName(c), psync_len, psync_offset);
    refreshGoodSlavesCount();
    return C_OK;

    need_full_resync:
    return C_ERR;
}

/**
 * 进行RDB复制并进行同步
 *
 * @param mincapa
 */
int startBgsaveForReplication(
        int mincapa) {

    int retval;
    // 是否直接写到socket
    int socket_target = server.repl_diskless_sync && (mincapa & SLAVE_CAPA_EOF);
    listIter li;
    listNode *ln;
    serverLog(LL_NOTICE, "Starting BGSAVE for SYNC with target: %s", socket_target ? "slaves sockets" : "disk");
    rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
    if (server.master) rsi.repl_stream_db = server.master->db->id;
    // 否则后台进行RDB持久化BGSAVE操作,保存到磁盘上
    retval = rdbSaveBackground(server.rdb_filename, &rsi);
    if (retval == C_ERR) {
        serverLog(LL_WARNING, "BGSAVE for replication failed");
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                slave->flags &= ~CLIENT_SLAVE;
                listDelNode(server.slaves, ln);
                addReplyError(slave, "BGSAVE failed, replication can't continue");
                slave->flags |= CLIENT_CLOSE_AFTER_REPLY;
            }
        }
        return retval;
    }
    if (!socket_target) {
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            // 从服务器状态为SLAVE_STATE_WAIT_BGSAVE_START
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                // 设置要执行全量重同步从节点的状态
                replicationSetupSlaveForFullResync(slave, getPsyncInitialOffset());
            }
        }
    }

    if (retval == C_OK) replicationScriptCacheFlush();
    return retval;
}

/**
 * 处理sync和psync命令
 */
void syncCommand(
        client *c) {

    if (c->flags & CLIENT_SLAVE) return;

    // 未连接[连接不成功状态]
    if (server.masterhost && server.repl_state != REPL_STATE_CONNECTED) {
        addReplySds(c, sdsnew("-NOMASTERLINK Can't SYNC while not connected with my master\r\n"));
        return;
    }

    if (clientHasPendingReplies(c)) {
        addReplyError(c, "SYNC and PSYNC are invalid with pending output");
        return;
    }

    serverLog(LL_NOTICE, "Slave %s asks for synchronization", replicationGetSlaveName(c));

    // psvnc命令
    if (!strcasecmp(c->argv[0]->ptr, "psync")) {
        // 尝试部分复制
        if (masterTryPartialResynchronization(c) == C_OK) {
            // 部分复制成功
            server.stat_sync_partial_ok++;
            return;
        } else {
            char *master_replid = c->argv[1]->ptr;
            // 从节点以强制全量同步为目的,所以不能执行部分重同步,因此增加PSYNC命令失败的次数
            if (master_replid[0] != '?') server.stat_sync_partial_err++;
        }
    } else {
        // 设置标识,执行SYNC命令
        c->flags |= CLIENT_PRE_PSYNC;
    }

    // 全量同步计数自增
    server.stat_sync_full++;
    // 设置client状态为:从服务器节点等待BGSAVE节点的开始
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    // 只要有命令就传输给从节点
    if (server.repl_disable_tcp_nodelay)
        anetDisableTcpNoDelay(NULL, c->fd);
    c->repldbfd = -1;
    c->flags |= CLIENT_SLAVE;
    listAddNodeTail(server.slaves, c);
    // 如果只存在一个slave,并且复制缓冲区不存在,创建一个新的复制缓冲区
    if (listLength(server.slaves) == 1 && server.repl_backlog == NULL) {
        changeReplicationId();
        clearReplicationId2();
        createReplicationBacklog();
    }

    // 第一种情况rdb在执行并且是落盘的[相当于复用缓冲区]
    if (server.rdb_child_pid != -1 && server.rdb_child_type == RDB_CHILD_TYPE_DISK) {

        client *slave;
        listNode *ln;
        listIter li;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            slave = ln->value;
            // 如果有从节点的状态为等待BGSAVE结束直接退出
            // 如果有从节点已经创建子进程执行写RDB操作,等待完成,那么退出循环
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) break;
        }
        // 检查此从节点是否有触发当前BGSAVE的能力
        if (ln && ((c->slave_capa & slave->slave_capa) == slave->slave_capa)) {
            // 将slave的输出缓冲区所有内容拷贝给c的所有输出缓冲区中
            copyClientOutputBuffer(c, slave);
            // 设置全量重同步从节点的状态,设置部分重同步的偏移量
            replicationSetupSlaveForFullResync(c, slave->psync_initial_offset);
            serverLog(LL_NOTICE, "Waiting for end of BGSAVE for SYNC");
        } else {
            serverLog(LL_NOTICE, "Can't attach the slave to the current BGSAVE. Waiting for next BGSAVE for SYNC");
        }

        // 第二种是以socket类型的RDB文件,无盘同步,直接写入SOCKET
    } else {
        // 后台没有AOF重写执行
        if (server.aof_child_pid == -1) {
            // 开启RDB后台复制
            startBgsaveForReplication(c->slave_capa);
        } else {
            serverLog(LL_NOTICE,
                      "No BGSAVE in progress, but an AOF rewrite is active. ""BGSAVE for replication delayed");
        }
    }
    return;
}

/**
 * 处理从节点发送的相关命令
 *
 * @param c
 */
void replconfCommand(
        client *c) {

    int j;

    if ((c->argc % 2) == 0) {
        addReply(c, shared.syntaxerr);
        return;
    }
    for (j = 1; j < c->argc; j += 2) {
        // 端口
        if (!strcasecmp(c->argv[j]->ptr, "listening-port")) {
            long port;
            if ((getLongFromObjectOrReply(c, c->argv[j + 1], &port, NULL) != C_OK))
                return;
            c->slave_listening_port = port;
        } else if (!strcasecmp(c->argv[j]->ptr, "ip-address")) {
            sds ip = c->argv[j + 1]->ptr;
            if (sdslen(ip) < sizeof(c->slave_ip)) {
                memcpy(c->slave_ip, ip, sdslen(ip) + 1);
            } else {
                addReplyErrorFormat(c, "REPLCONF ip-address provided by "
                                       "slave instance is too long: %zd bytes", sdslen(ip));
                return;
            }
        } else if (!strcasecmp(c->argv[j]->ptr, "capa")) {
            /* Ignore capabilities not understood by this master. */
            if (!strcasecmp(c->argv[j + 1]->ptr, "eof"))
                c->slave_capa |= SLAVE_CAPA_EOF;
            else if (!strcasecmp(c->argv[j + 1]->ptr, "psync2"))
                c->slave_capa |= SLAVE_CAPA_PSYNC2;
        } else if (!strcasecmp(c->argv[j]->ptr, "ack")) {
            /* REPLCONF ACK is used by slave to inform the master the amount
             * of replication stream that it processed so far. It is an
             * internal only command that normal clients should never use. */
            long long offset;

            if (!(c->flags & CLIENT_SLAVE)) return;
            if ((getLongLongFromObject(c->argv[j + 1], &offset) != C_OK))
                return;
            if (offset > c->repl_ack_off)
                c->repl_ack_off = offset;
            c->repl_ack_time = server.unixtime;
            /* If this was a diskless replication, we need to really put
             * the slave online when the first ACK is received (which
             * confirms slave is online and ready to get more data). */
            if (c->repl_put_online_on_ack && c->replstate == SLAVE_STATE_ONLINE)
                putSlaveOnline(c);
            /* Note: this command does not reply anything! */
            return;
        } else if (!strcasecmp(c->argv[j]->ptr, "getack")) {
            /* REPLCONF GETACK is used in order to request an ACK ASAP
             * to the slave. */
            if (server.masterhost && server.master) replicationSendAck();
            return;
        } else {
            addReplyErrorFormat(c, "Unrecognized REPLCONF option: %s",
                                (char *) c->argv[j]->ptr);
            return;
        }
    }
    addReply(c, shared.ok);
}

/**
 * 将节点状态变更为传输RDB结束状态
 *
 * @param slave 从节点
 */
void putSlaveOnline(
        client *slave) {

    // 表示RDB发送完成
    slave->replstate = SLAVE_STATE_ONLINE;
    slave->repl_put_online_on_ack = 0;
    slave->repl_ack_time = server.unixtime;
    // 创建从节点回复主节点写事件sendReplyToClient
    if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendReplyToClient, slave) == AE_ERR) {
        serverLog(LL_WARNING, "Unable to register writable event for slave bulk transfer: %s", strerror(errno));
        freeClient(slave);
        return;
    }
    refreshGoodSlavesCount();
    serverLog(LL_NOTICE, "Synchronization with slave %s succeeded", replicationGetSlaveName(slave));
}

/**
 * 将RDB发送给从节点事件处理器
 *
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void sendBulkToSlave(
        aeEventLoop *el,
        int fd,
        void *privdata,
        int mask) {

    client *slave = privdata;
    UNUSED(el);
    UNUSED(mask);
    char buf[PROTO_IOBUF_LEN];
    ssize_t nwritten, buflen;

    // 先写选择数据库之类的
    if (slave->replpreamble) {
        nwritten = write(fd, slave->replpreamble, sdslen(slave->replpreamble));
        if (nwritten == -1) {
            serverLog(LL_VERBOSE, "Write error sending RDB preamble to slave: %s", strerror(errno));
            freeClient(slave);
            return;
        }
        server.stat_net_output_bytes += nwritten;
        sdsrange(slave->replpreamble, nwritten, -1);
        if (sdslen(slave->replpreamble) == 0) {
            sdsfree(slave->replpreamble);
            slave->replpreamble = NULL;
        } else {
            return;
        }
    }
    // 设置为偏移量为0
    lseek(slave->repldbfd, slave->repldboff, SEEK_SET);
    // 从repldbfd读取RDB文件
    buflen = read(slave->repldbfd, buf, PROTO_IOBUF_LEN);
    if (buflen <= 0) {
        serverLog(LL_WARNING, "Read error sending DB to slave: %s", (buflen == 0) ? "premature EOF" : strerror(errno));
        freeClient(slave);
        return;
    }
    // 将读取的内容写入客户端资源符
    if ((nwritten = write(fd, buf, buflen)) == -1) {
        if (errno != EAGAIN) {
            serverLog(LL_WARNING, "Write error sending DB to slave: %s", strerror(errno));
            freeClient(slave);
        }
        return;
    }
    // 更新RDB文件偏移量
    slave->repldboff += nwritten;
    // 代表网络传输字节
    server.stat_net_output_bytes += nwritten;
    // 代表文件全部写入
    if (slave->repldboff == slave->repldbsize) {
        close(slave->repldbfd);
        slave->repldbfd = -1;
        // 删除可写事件
        aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
        // 并且让从节点线上状态
        putSlaveOnline(slave);
    }
}

/**
 * bgsave执行完更新从节点
 *
 * @param bgsaveerr RDB错误
 * @param type 类型
 */
void updateSlavesWaitingBgsave(
        int bgsaveerr,
        int type) {

    listNode *ln;
    int startbgsave = 0;
    int mincapa = -1;
    listIter li;
    listRewind(server.slaves, &li);
    // 遍历从节点
    while ((ln = listNext(&li))) {
        client *slave = ln->value;
        if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
            startbgsave = 1;
            mincapa = (mincapa == -1) ? slave->slave_capa : (mincapa & slave->slave_capa);
            // 等待BGSAVE结束
        } else if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END) {
            struct redis_stat buf;
            if (bgsaveerr != C_OK) {
                freeClient(slave);
                serverLog(LL_WARNING, "SYNC failed. BGSAVE child returned an error");
                continue;
            }
            // 打开描述符[RDB文件,只读]从节点与父节点之间的资源描述符
            if ((slave->repldbfd = open(server.rdb_filename, O_RDONLY)) == -1 ||
                redis_fstat(slave->repldbfd, &buf) == -1) {
                freeClient(slave);
                serverLog(LL_WARNING, "SYNC failed. Can't open/stat DB after BGSAVE: %s", strerror(errno));
                continue;
            }
            // 偏移量为0
            slave->repldboff = 0;
            // rdb复制初始大小
            slave->repldbsize = buf.st_size;
            // 更改状态为SLAVE_STATE_SEND_BULK
            slave->replstate = SLAVE_STATE_SEND_BULK;
            slave->replpreamble = sdscatprintf(sdsempty(), "$%lld\r\n", (unsigned long long) slave->repldbsize);
            // 清除之前的可写事件
            aeDeleteFileEvent(server.el, slave->fd, AE_WRITABLE);
            // 创建可写事件将RDB发给从节点
            if (aeCreateFileEvent(server.el, slave->fd, AE_WRITABLE, sendBulkToSlave, slave) == AE_ERR) {
                freeClient(slave);
                continue;
            }
        }
    }
    if (startbgsave) startBgsaveForReplication(mincapa);
}

/* Change the current instance replication ID with a new, random one.
 * This will prevent successful PSYNCs between this master and other
 * slaves, so the command should be called when something happens that
 * alters the current story of the dataset. */
void changeReplicationId(void) {
    getRandomHexChars(server.replid, CONFIG_RUN_ID_SIZE);
    server.replid[CONFIG_RUN_ID_SIZE] = '\0';
}

/* Clear (invalidate) the secondary replication ID. This happens, for
 * example, after a full resynchronization, when we start a new replication
 * history. */
void clearReplicationId2(void) {
    memset(server.replid2, '0', sizeof(server.replid));
    server.replid2[CONFIG_RUN_ID_SIZE] = '\0';
    server.second_replid_offset = -1;
}

/* Use the current replication ID / offset as secondary replication
 * ID, and change the current one in order to start a new history.
 * This should be used when an instance is switched from slave to master
 * so that it can serve PSYNC requests performed using the master
 * replication ID. */
void shiftReplicationId(void) {
    memcpy(server.replid2, server.replid, sizeof(server.replid));
    /* We set the second replid offset to the master offset + 1, since
     * the slave will ask for the first byte it has not yet received, so
     * we need to add one to the offset: for example if, as a slave, we are
     * sure we have the same history as the master for 50 bytes, after we
     * are turned into a master, we can accept a PSYNC request with offset
     * 51, since the slave asking has the same history up to the 50th
     * byte, and is asking for the new bytes starting at offset 51. */
    server.second_replid_offset = server.master_repl_offset + 1;
    changeReplicationId();
    serverLog(LL_WARNING, "Setting secondary replication ID to %s, valid up to offset: %lld. New replication ID is %s",
              server.replid2, server.second_replid_offset, server.replid);
}

/* ----------------------------------- SLAVE -------------------------------- */

/**
 * 如果给定的复制状态是握手状态,则返回1
 */
int slaveIsInHandshakeState(
        void) {

    return server.repl_state >= REPL_STATE_RECEIVE_PONG &&
           server.repl_state <= REPL_STATE_RECEIVE_PSYNC;
}

/* Avoid the master to detect the slave is timing out while loading the
 * RDB file in initial synchronization. We send a single newline character
 * that is valid protocol but is guaranteed to either be sent entierly or
 * not, since the byte is indivisible.
 *
 * The function is called in two contexts: while we flush the current
 * data with emptyDb(), and while we load the new data received as an
 * RDB file from the master. */
void replicationSendNewlineToMaster(void) {
    static time_t newline_sent;
    if (time(NULL) != newline_sent) {
        newline_sent = time(NULL);
        if (write(server.repl_transfer_s, "\n", 1) == -1) {
            /* Pinging back in this stage is best-effort. */
        }
    }
}

/* Callback used by emptyDb() while flushing away old data to load
 * the new dataset received by the master. */
void replicationEmptyDbCallback(void *privdata) {
    UNUSED(privdata);
    replicationSendNewlineToMaster();
}

/* Once we have a link with the master and the synchroniziation was
 * performed, this function materializes the master client we store
 * at server.master, starting from the specified file descriptor. */
void replicationCreateMasterClient(int fd, int dbid) {
    server.master = createClient(fd);
    server.master->flags |= CLIENT_MASTER;
    server.master->authenticated = 1;
    server.master->reploff = server.master_initial_offset;
    server.master->read_reploff = server.master->reploff;
    memcpy(server.master->replid, server.master_replid,
           sizeof(server.master_replid));
    /* If master offset is set to -1, this master is old and is not
     * PSYNC capable, so we flag it accordingly. */
    if (server.master->reploff == -1)
        server.master->flags |= CLIENT_PRE_PSYNC;
    if (dbid != -1) selectDb(server.master, dbid);
}

void restartAOF() {
    int retry = 10;
    while (retry-- && startAppendOnly() == C_ERR) {
        serverLog(LL_WARNING,
                  "Failed enabling the AOF after successful master synchronization! Trying it again in one second.");
        sleep(1);
    }
    if (!retry) {
        serverLog(LL_WARNING,
                  "FATAL: this slave instance finished the synchronization with its master, but the AOF can't be turned on. Exiting now.");
        exit(1);
    }
}

/* Asynchronously read the SYNC payload we receive from a master */
#define REPL_MAX_WRITTEN_BEFORE_FSYNC (1024*1024*8) /* 8 MB */

/**
 *
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void readSyncBulkPayload(
        aeEventLoop *el,
        int fd,
        void *privdata,
        int mask) {

    char buf[4096];
    ssize_t nread, readlen;
    off_t left;
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    static char eofmark[CONFIG_RUN_ID_SIZE];
    static char lastbytes[CONFIG_RUN_ID_SIZE];
    static int usemark = 0;

    if (server.repl_transfer_size == -1) {
        if (syncReadLine(fd, buf, 1024, server.repl_syncio_timeout * 1000) == -1) {
            serverLog(LL_WARNING, "I/O error reading bulk count from MASTER: %s", strerror(errno));
            goto error;
        }

        if (buf[0] == '-') {
            serverLog(LL_WARNING, "MASTER aborted replication with an error: %s", buf + 1);
            goto error;
        } else if (buf[0] == '\0') {
            server.repl_transfer_lastio = server.unixtime;
            return;
        } else if (buf[0] != '$') {
            serverLog(LL_WARNING,
                      "Bad protocol from MASTER, the first byte is not '$' (we received '%s'), are you sure the host and port are right?",
                      buf);
            goto error;
        }
        if (strncmp(buf + 1, "EOF:", 4) == 0 && strlen(buf + 5) >= CONFIG_RUN_ID_SIZE) {
            usemark = 1;
            memcpy(eofmark, buf + 5, CONFIG_RUN_ID_SIZE);
            memset(lastbytes, 0, CONFIG_RUN_ID_SIZE);
            server.repl_transfer_size = 0;
            serverLog(LL_NOTICE,
                      "MASTER <-> SLAVE sync: receiving streamed RDB from master");
        } else {
            usemark = 0;
            server.repl_transfer_size = strtol(buf + 1, NULL, 10);
            serverLog(LL_NOTICE,
                      "MASTER <-> SLAVE sync: receiving %lld bytes from master",
                      (long long) server.repl_transfer_size);
        }
        return;
    }

    // 读取文件内容
    if (usemark) {
        readlen = sizeof(buf);
    } else {
        left = server.repl_transfer_size - server.repl_transfer_read;
        readlen = (left < (signed) sizeof(buf)) ? left : (signed) sizeof(buf);
    }
    nread = read(fd, buf, readlen);
    if (nread <= 0) {
        serverLog(LL_WARNING, "I/O error trying to sync with MASTER: %s",
                  (nread == -1) ? strerror(errno) : "connection lost");
        cancelReplicationHandshake();
        return;
    }
    server.stat_net_input_bytes += nread;
    int eof_reached = 0;

    if (usemark) {
        /* Update the last bytes array, and check if it matches our delimiter.*/
        if (nread >= CONFIG_RUN_ID_SIZE) {
            memcpy(lastbytes, buf + nread - CONFIG_RUN_ID_SIZE, CONFIG_RUN_ID_SIZE);
        } else {
            int rem = CONFIG_RUN_ID_SIZE - nread;
            memmove(lastbytes, lastbytes + nread, rem);
            memcpy(lastbytes + rem, buf, nread);
        }
        if (memcmp(lastbytes, eofmark, CONFIG_RUN_ID_SIZE) == 0) eof_reached = 1;
    }

    server.repl_transfer_lastio = server.unixtime;
    // 写入临时文件
    if (write(server.repl_transfer_fd, buf, nread) != nread) {
        serverLog(LL_WARNING,
                  "Write error or short write writing to the DB dump file needed for MASTER <-> SLAVE synchronization: %s",
                  strerror(errno));
        goto error;
    }
    server.repl_transfer_read += nread;
    if (usemark && eof_reached) {
        if (ftruncate(server.repl_transfer_fd, server.repl_transfer_read - CONFIG_RUN_ID_SIZE) == -1) {
            serverLog(LL_WARNING, "Error truncating the RDB file received from the master for SYNC: %s",
                      strerror(errno));
            goto error;
        }
    }

    if (server.repl_transfer_read >= server.repl_transfer_last_fsync_off + REPL_MAX_WRITTEN_BEFORE_FSYNC) {
        off_t sync_size = server.repl_transfer_read -
                          server.repl_transfer_last_fsync_off;
        rdb_fsync_range(server.repl_transfer_fd,
                        server.repl_transfer_last_fsync_off, sync_size);
        server.repl_transfer_last_fsync_off += sync_size;
    }

    /* Check if the transfer is now complete */
    if (!usemark) {
        if (server.repl_transfer_read == server.repl_transfer_size)
            eof_reached = 1;
    }

    if (eof_reached) {
        int aof_is_enabled = server.aof_state != AOF_OFF;
        if (rename(server.repl_transfer_tmpfile, server.rdb_filename) == -1) {
            serverLog(LL_WARNING,
                      "Failed trying to rename the temp DB into dump.rdb in MASTER <-> SLAVE synchronization: %s",
                      strerror(errno));
            cancelReplicationHandshake();
            return;
        }
        serverLog(LL_NOTICE, "MASTER <-> SLAVE sync: Flushing old data");
        if (aof_is_enabled) stopAppendOnly();
        // 通知刷新DB数据
        signalFlushedDb(-1);
        emptyDb(
                -1,
                server.repl_slave_lazy_flush ? EMPTYDB_ASYNC : EMPTYDB_NO_FLAGS,
                replicationEmptyDbCallback);
        aeDeleteFileEvent(server.el, server.repl_transfer_s, AE_READABLE);
        serverLog(LL_NOTICE, "MASTER <-> SLAVE sync: Loading DB in memory");
        rdbSaveInfo rsi = RDB_SAVE_INFO_INIT;
        if (rdbLoad(server.rdb_filename, &rsi) != C_OK) {
            serverLog(LL_WARNING, "Failed trying to load the MASTER synchronization DB from disk");
            cancelReplicationHandshake();
            if (aof_is_enabled) restartAOF();
            return;
        }
        /* Final setup of the connected slave <- master link */
        zfree(server.repl_transfer_tmpfile);
        close(server.repl_transfer_fd);
        replicationCreateMasterClient(server.repl_transfer_s, rsi.repl_stream_db);
        server.repl_state = REPL_STATE_CONNECTED;
        /* After a full resynchroniziation we use the replication ID and
         * offset of the master. The secondary ID / offset are cleared since
         * we are starting a new history. */
        memcpy(server.replid, server.master->replid, sizeof(server.replid));
        server.master_repl_offset = server.master->reploff;
        clearReplicationId2();
        /* Let's create the replication backlog if needed. Slaves need to
         * accumulate the backlog regardless of the fact they have sub-slaves
         * or not, in order to behave correctly if they are promoted to
         * masters after a failover. */
        if (server.repl_backlog == NULL) createReplicationBacklog();

        serverLog(LL_NOTICE, "MASTER <-> SLAVE sync: Finished with success");
        /* Restart the AOF subsystem now that we finished the sync. This
         * will trigger an AOF rewrite, and when done will start appending
         * to the new file. */
        if (aof_is_enabled) restartAOF();
    }
    return;

    error:
    cancelReplicationHandshake();
    return;
}

/* Send a synchronous command to the master. Used to send AUTH and
 * REPLCONF commands before starting the replication with SYNC.
 *
 * The command returns an sds string representing the result of the
 * operation. On error the first byte is a "-".
 */
#define SYNC_CMD_READ (1<<0)
#define SYNC_CMD_WRITE (1<<1)
#define SYNC_CMD_FULL (SYNC_CMD_READ|SYNC_CMD_WRITE)

/**
 * 发送命令
 *
 * @param flags
 * @param fd
 * @param ...
 * @return
 */
char *sendSynchronousCommand(
        int flags,
        int fd, ...) {

    // 创建要发送给主服务器的命令
    if (flags & SYNC_CMD_WRITE) {
        char *arg;
        va_list ap;
        sds cmd = sdsempty();
        va_start(ap, fd);

        while (1) {
            arg = va_arg(ap, char*);
            if (arg == NULL) break;
            if (sdslen(cmd) != 0) cmd = sdscatlen(cmd, " ", 1);
            cmd = sdscat(cmd, arg);
        }
        cmd = sdscatlen(cmd, "\r\n", 2);
        // 将命令发送给父节点(同步写)
        if (syncWrite(fd, cmd, sdslen(cmd), server.repl_syncio_timeout * 1000) == -1) {
            // 释放命令
            sdsfree(cmd);
            return sdscatprintf(sdsempty(), "-Writing to master: %s", strerror(errno));
        }
        sdsfree(cmd);
        va_end(ap);
    }

    // 服务器读命令[读取]
    if (flags & SYNC_CMD_READ) {
        char buf[256];

        // 读取数据
        if (syncReadLine(fd, buf, sizeof(buf), server.repl_syncio_timeout * 1000) == -1) {
            return sdscatprintf(sdsempty(), "-Reading from master: %s", strerror(errno));
        }
        server.repl_transfer_lastio = server.unixtime;
        return sdsnew(buf);
    }
    return NULL;
}

#define PSYNC_WRITE_ERROR 0
#define PSYNC_WAIT_REPLY 1
#define PSYNC_CONTINUE 2
#define PSYNC_FULLRESYNC 3
#define PSYNC_NOT_SUPPORTED 4
#define PSYNC_TRY_LATER 5

/**
 * 尝试部分同步
 *
 * @param fd
 * @param read_reply
 * @return
 */
int slaveTryPartialResynchronization(
        int fd,
        int read_reply) {

    char *psync_replid;
    char psync_offset[32];
    sds reply;

    // 写
    if (!read_reply) {
        // 重置复制master初始偏移量
        server.master_initial_offset = -1;
        // 主节点的缓存不为空,可以尝试进行部分重同步[代表是否主从节点连接过]
        if (server.cached_master) {
            // 复制时运行期ID
            psync_replid = server.cached_master->replid;
            snprintf(psync_offset, sizeof(psync_offset), "%lld", server.cached_master->reploff + 1);
            // 表示偏移量
            serverLog(LL_NOTICE, "Trying a partial resynchronization (request %s:%s).", psync_replid, psync_offset);
        } else {
            // 主节点的缓存为空,发送PSYNC ? -1,请求全量同步
            serverLog(LL_NOTICE, "Partial resynchronization not possible (no cached master)");
            // 运行期ID为？
            psync_replid = "?";
            // 复制偏移量为-1
            memcpy(psync_offset, "-1", 3);
        }

        // 发送PSYNC命令
        reply = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PSYNC", psync_replid, psync_offset, NULL);
        if (reply != NULL) {
            serverLog(LL_WARNING, "Unable to send PSYNC to master: %s", reply);
            sdsfree(reply);
            aeDeleteFileEvent(server.el, fd, AE_READABLE);
            return PSYNC_WRITE_ERROR;
        }
        return PSYNC_WAIT_REPLY;
    }

    // 读回复[保存命令]
    reply = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);
    // 代表没有回复
    if (sdslen(reply) == 0) {
        sdsfree(reply);
        return PSYNC_WAIT_REPLY;
    }

    // 如果读到了一个命令,删除fd的可读事件
    aeDeleteFileEvent(server.el, fd, AE_READABLE);

    // 全量复制返回
    if (!strncmp(reply, "+FULLRESYNC", 11)) {
        char *replid = NULL, *offset = NULL;
        // 全量复制
        replid = strchr(reply, ' ');
        if (replid) {
            replid++;
            offset = strchr(replid, ' ');
            if (offset) offset++;
        }
        if (!replid || !offset || (offset - replid - 1) != CONFIG_RUN_ID_SIZE) {
            serverLog(LL_WARNING, "Master replied with wrong +FULLRESYNC syntax.");
            memset(server.master_replid, 0, CONFIG_RUN_ID_SIZE + 1);
        } else {
            // 设置服务器保存的主节点的运行ID
            memcpy(server.master_replid, replid, offset - replid - 1);
            server.master_replid[CONFIG_RUN_ID_SIZE] = '\0';
            // 主节点的偏移量
            server.master_initial_offset = strtoll(offset, NULL, 10);
            serverLog(LL_NOTICE, "Full resync from master: %s:%lld",
                      server.master_replid,
                      server.master_initial_offset);
        }
        // 清空状态
        replicationDiscardCachedMaster();
        sdsfree(reply);
        return PSYNC_FULLRESYNC;
    }

    // 部分重同步
    if (!strncmp(reply, "+CONTINUE", 9)) {
        serverLog(LL_NOTICE, "Successful partial resynchronization with master.");
        char *start = reply + 10;
        char *end = reply + 9;
        while (end[0] != '\r' && end[0] != '\n' && end[0] != '\0') end++;
        if (end - start == CONFIG_RUN_ID_SIZE) {
            char new[CONFIG_RUN_ID_SIZE + 1];
            memcpy(new, start, CONFIG_RUN_ID_SIZE);
            new[CONFIG_RUN_ID_SIZE] = '\0';
            if (strcmp(new, server.cached_master->replid)) {
                serverLog(LL_WARNING, "Master replication ID changed to %s", new);
                memcpy(server.replid2, server.cached_master->replid, sizeof(server.replid2));
                server.second_replid_offset = server.master_repl_offset + 1;
                memcpy(server.replid, new, sizeof(server.replid));
                memcpy(server.cached_master->replid, new, sizeof(server.replid));
                // 断开所有从节点连接
                disconnectSlaves();
            }
        }
        sdsfree(reply);
        replicationResurrectCachedMaster(fd);
        return PSYNC_CONTINUE;
    }

    if (!strncmp(reply, "-NOMASTERLINK", 13) ||
        !strncmp(reply, "-LOADING", 8)) {
        serverLog(LL_NOTICE,
                  "Master is currently unable to PSYNC "
                  "but should be in the future: %s", reply);
        sdsfree(reply);
        return PSYNC_TRY_LATER;
    }

    if (strncmp(reply, "-ERR", 4)) {
        /* If it's not an error, log the unexpected event. */
        serverLog(LL_WARNING,
                  "Unexpected reply to PSYNC from master: %s", reply);
    } else {
        serverLog(LL_NOTICE,
                  "Master does not support PSYNC or is in "
                  "error state (reply: %s)", reply);
    }
    sdsfree(reply);
    replicationDiscardCachedMaster();
    return PSYNC_NOT_SUPPORTED;
}

/**
 * 监听主从复制连接事件处理器[只有从节点执行]
 *
 * @param el
 * @param fd
 * @param privdata
 * @param mask
 */
void syncWithMaster(
        aeEventLoop *el,
        int fd,
        void *privdata,
        int mask) {

    char tmpfile[256], *err = NULL;
    int dfd = -1, maxtries = 5;
    int sockerr = 0, psync_result;
    socklen_t errlen = sizeof(sockerr);
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    // 无主机状态或断开连接状态
    if (server.repl_state == REPL_STATE_NONE) {
        close(fd);
        return;
    }

    if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &sockerr, &errlen) == -1)
        sockerr = errno;
    if (sockerr) {
        serverLog(LL_WARNING, "Error condition on socket for SYNC: %s", strerror(sockerr));
        goto error;
    }

    // 发送ping命令判断主从之间通信是否正常是否能够接收命令
    if (server.repl_state == REPL_STATE_CONNECTING) {
        serverLog(LL_NOTICE, "Non blocking connect for SYNC fired the event.");
        // 先删除可写事件[方便后续操作成功时注册事件,然后直接触发事件]
        aeDeleteFileEvent(server.el, fd, AE_WRITABLE);
        // 将状态变为接收pong状态
        server.repl_state = REPL_STATE_RECEIVE_PONG;
        // 发送ping命令,写如给主机事件
        err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "PING", NULL);
        if (err) goto write_error;
        return;
    }

    // 接收主节点的pong请求
    if (server.repl_state == REPL_STATE_RECEIVE_PONG) {
        // 读取回复
        err = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);

        // 代表没权限或者错误
        if (err[0] != '+' &&
            strncmp(err, "-NOAUTH", 7) != 0 &&
            strncmp(err, "-ERR operation not permitted", 28) != 0) {
            serverLog(LL_WARNING, "Error reply to PING from master: '%s'", err);
            sdsfree(err);
            goto error;
        } else {
            serverLog(LL_NOTICE, "Master replied to PING, replication can continue...");
        }
        sdsfree(err);
        // 此时变成权限认证阶段阶段
        server.repl_state = REPL_STATE_SEND_AUTH;
    }

    // 发送权限认证命令[此时还不能写,未开启监听写事件]
    if (server.repl_state == REPL_STATE_SEND_AUTH) {
        // 配置了参数需要权限认证
        if (server.masterauth) {
            err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "AUTH", server.masterauth, NULL);
            if (err) goto write_error;
            server.repl_state = REPL_STATE_RECEIVE_AUTH;
            return;
        } else {
            // 否则直接发送端口状态
            server.repl_state = REPL_STATE_SEND_PORT;
        }
    }

    // 接收权限认证结果
    if (server.repl_state == REPL_STATE_RECEIVE_AUTH) {
        err = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);
        if (err[0] == '-') {
            serverLog(LL_WARNING, "Unable to AUTH to MASTER: %s", err);
            sdsfree(err);
            goto error;
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PORT;
    }

    // 发送端口号[此时还不能写,未开启监听写事件]
    if (server.repl_state == REPL_STATE_SEND_PORT) {
        sds port = sdsfromlonglong(server.slave_announce_port ? server.slave_announce_port : server.port);
        err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "REPLCONF", "listening-port", port, NULL);
        sdsfree(port);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_PORT;
        return;
    }

    // 接收端口号
    if (server.repl_state == REPL_STATE_RECEIVE_PORT) {
        err = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);
        // 判断是否端口号主机正常接收
        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand "
                                 "REPLCONF listening-port: %s", err);
        }
        sdsfree(err);
        // 发送IP状态
        server.repl_state = REPL_STATE_SEND_IP;
    }

    if (server.repl_state == REPL_STATE_SEND_IP && server.slave_announce_ip == NULL) {
        server.repl_state = REPL_STATE_SEND_CAPA;
    }

    // 发送IP
    if (server.repl_state == REPL_STATE_SEND_IP) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "REPLCONF", "ip-address", server.slave_announce_ip, NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_IP;
        return;
    }

    // 接收IP
    if (server.repl_state == REPL_STATE_RECEIVE_IP) {
        err = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);

        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand ""REPLCONF ip-address: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_CAPA;
    }

    // 发送CAPA
    if (server.repl_state == REPL_STATE_SEND_CAPA) {
        err = sendSynchronousCommand(SYNC_CMD_WRITE, fd, "REPLCONF", "capa", "eof", "capa", "psync2", NULL);
        if (err) goto write_error;
        sdsfree(err);
        server.repl_state = REPL_STATE_RECEIVE_CAPA;
        return;
    }

    // 接收CAPA的回复
    if (server.repl_state == REPL_STATE_RECEIVE_CAPA) {
        err = sendSynchronousCommand(SYNC_CMD_READ, fd, NULL);
        if (err[0] == '-') {
            serverLog(LL_NOTICE, "(Non critical) Master does not understand ""REPLCONF capa: %s", err);
        }
        sdsfree(err);
        server.repl_state = REPL_STATE_SEND_PSYNC;
    }

    // 发送同步命令
    if (server.repl_state == REPL_STATE_SEND_PSYNC) {
        // 尝试部分同步[0代表写]
        if (slaveTryPartialResynchronization(fd, 0) == PSYNC_WRITE_ERROR) {
            err = sdsnew("Write error sending the PSYNC command.");
            goto write_error;
        }
        server.repl_state = REPL_STATE_RECEIVE_PSYNC;
        // 直接返回
        return;
    }

    if (server.repl_state != REPL_STATE_RECEIVE_PSYNC) {
        serverLog(LL_WARNING, "syncWithMaster(): state machine error, ""state should be RECEIVE_PSYNC but is %d",
                  server.repl_state);
        goto error;
    }

    // 子节点读取主节点返回的PSYNC命令执行的结果[读取内容]
    psync_result = slaveTryPartialResynchronization(fd, 1);
    // 重试
    if (psync_result == PSYNC_WAIT_REPLY) return;

    // 直接返回错误等待下次
    if (psync_result == PSYNC_TRY_LATER) goto error;

    // 部分重同步直接返回
    if (psync_result == PSYNC_CONTINUE) {
        serverLog(LL_NOTICE, "MASTER <-> SLAVE sync: Master accepted a Partial Resynchronization.");
        return;
    }
    // 强制同步
    disconnectSlaves();
    // 释放复制缓冲区
    freeReplicationBacklog();

    if (psync_result == PSYNC_NOT_SUPPORTED) {
        serverLog(LL_NOTICE, "Retrying with SYNC...");
        if (syncWrite(fd, "SYNC\r\n", 6, server.repl_syncio_timeout * 1000) == -1) {
            serverLog(LL_WARNING, "I/O error writing to MASTER: %s", strerror(errno));
            goto error;
        }
    }

    // 循环
    while (maxtries--) {
        snprintf(tmpfile, 256, "temp-%d.%ld.rdb", (int) server.unixtime, (long int) getpid());
        // 开启临时文件
        dfd = open(tmpfile, O_CREAT | O_WRONLY | O_EXCL, 0644);
        if (dfd != -1) break;
        sleep(1);
    }
    if (dfd == -1) {
        serverLog(LL_WARNING, "Opening the temp file needed for MASTER <-> SLAVE synchronization: %s", strerror(errno));
        goto error;
    }

    // 监听一个fd的读事件,并设置该事件的处理程序为readSyncBulkPayload,用来从主节点读数据
    if (aeCreateFileEvent(server.el, fd, AE_READABLE, readSyncBulkPayload, NULL) == AE_ERR) {
        serverLog(LL_WARNING, "Can't create readable event for SYNC: %s (fd=%d)", strerror(errno), fd);
        goto error;
    }
    // 传输状态
    server.repl_state = REPL_STATE_TRANSFER;
    // 传输大小
    server.repl_transfer_size = -1;
    // 已读取内容大小
    server.repl_transfer_read = 0;
    // 最近一个执行fsync的偏移量为0
    server.repl_transfer_last_fsync_off = 0;
    server.repl_transfer_fd = dfd;
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_tmpfile = zstrdup(tmpfile);
    return;

    error:
    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    if (dfd != -1) close(dfd);
    close(fd);
    server.repl_transfer_s = -1;
    server.repl_state = REPL_STATE_CONNECT;
    return;

    write_error: /* Handle sendSynchronousCommand(SYNC_CMD_WRITE) errors. */
    serverLog(LL_WARNING, "Sending command to master in replication handshake: %s", err);
    sdsfree(err);
    goto error;
}

/**
 * 连接master
 */
int connectWithMaster(
        void) {

    int fd;

    // 主从建立连接
    fd = anetTcpNonBlockBestEffortBindConnect(NULL, server.masterhost, server.masterport, NET_FIRST_BIND_ADDR);
    if (fd == -1) {
        serverLog(LL_WARNING, "Unable to connect to MASTER: %s", strerror(errno));
        return C_ERR;
    }

    // 监听可读可写事件并且由syncWithMaster处理[监听后会执行第一次写操作]
    if (aeCreateFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE, syncWithMaster, NULL) ==
        AE_ERR) {
        close(fd);
        serverLog(LL_WARNING, "Can't create readable event for SYNC");
        return C_ERR;
    }
    server.repl_transfer_lastio = server.unixtime;
    server.repl_transfer_s = fd;
    // 将状态变成连接中状态
    server.repl_state = REPL_STATE_CONNECTING;
    return C_OK;
}

/* This function can be called when a non blocking connection is currently
 * in progress to undo it.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void undoConnectWithMaster(void) {
    int fd = server.repl_transfer_s;

    aeDeleteFileEvent(server.el, fd, AE_READABLE | AE_WRITABLE);
    close(fd);
    server.repl_transfer_s = -1;
}

/* Abort the async download of the bulk dataset while SYNC-ing with master.
 * Never call this function directly, use cancelReplicationHandshake() instead.
 */
void replicationAbortSyncTransfer(void) {
    serverAssert(server.repl_state == REPL_STATE_TRANSFER);
    undoConnectWithMaster();
    close(server.repl_transfer_fd);
    unlink(server.repl_transfer_tmpfile);
    zfree(server.repl_transfer_tmpfile);
}

/**
 * 取消复制握手
 */
int cancelReplicationHandshake(
        void) {

    // 交互状态变成需要连接
    if (server.repl_state == REPL_STATE_TRANSFER) {
        replicationAbortSyncTransfer();
        server.repl_state = REPL_STATE_CONNECT;
    } else if (server.repl_state == REPL_STATE_CONNECTING || slaveIsInHandshakeState()) {
        undoConnectWithMaster();
        server.repl_state = REPL_STATE_CONNECT;
    } else {
        return 0;
    }
    return 1;
}

/**
 * 将复制设置为指定的主地址和端口
 *
 * @param ip 地址
 * @param port 端口
 */
void replicationSetMaster(
        char *ip,
        int port) {

    int was_master = server.masterhost == NULL;
    // 释放之前的主节点
    sdsfree(server.masterhost);
    server.masterhost = sdsnew(ip);
    server.masterport = port;
    // 存在主节点释放主节点
    if (server.master) {
        freeClient(server.master);
    }
    // 清除所有客户端的阻塞状态
    disconnectAllBlockedClients();
    // 断开所有的从节点的连接
    disconnectSlaves();
    // 取消复制
    cancelReplicationHandshake();
    if (was_master) replicationCacheMasterUsingMyself();
    // 强制变成待连接状态[重新连接主节点状态]
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = 0;
}

/**
 * 取消复制
 */
void replicationUnsetMaster(
        void) {

    if (server.masterhost == NULL) return;
    sdsfree(server.masterhost);
    server.masterhost = NULL;
    shiftReplicationId();
    if (server.master) freeClient(server.master);
    replicationDiscardCachedMaster();
    cancelReplicationHandshake();
    // 断开所有连接
    disconnectSlaves();
    server.repl_state = REPL_STATE_NONE;
    server.slaveseldb = -1;
}

void replicationHandleMasterDisconnection(
        void) {

    server.master = NULL;
    server.repl_state = REPL_STATE_CONNECT;
    server.repl_down_since = server.unixtime;
}

void slaveofCommand(
        client *c) {

    if (server.cluster_enabled) {
        addReplyError(c, "SLAVEOF not allowed in cluster mode.");
        return;
    }
    // slaveof no one命令,断开与主机的连接,数据不会丢失
    if (!strcasecmp(c->argv[1]->ptr, "no") &&
        !strcasecmp(c->argv[2]->ptr, "one")) {
        if (server.masterhost) {
            replicationUnsetMaster();
            sds client = catClientInfoString(sdsempty(), c);
            serverLog(LL_NOTICE, "MASTER MODE enabled (user request from '%s')", client);
            sdsfree(client);
        }
    } else {
        long port;
        if ((getLongFromObjectOrReply(c, c->argv[2], &port, NULL) != C_OK))
            return;
        // 已经是主从关系
        if (server.masterhost && !strcasecmp(server.masterhost, c->argv[1]->ptr) && server.masterport == port) {
            serverLog(LL_NOTICE,
                      "SLAVE OF would result into synchronization with the master we are already connected with. No operation performed.");
            addReplySds(c, sdsnew("+OK Already connected to specified master\r\n"));
            return;
        }
        // 设置主机的IP和端口
        replicationSetMaster(c->argv[1]->ptr, port);
        sds client = catClientInfoString(sdsempty(), c);
        serverLog(LL_NOTICE, "SLAVE OF %s:%d enabled (user request from '%s')", server.masterhost, server.masterport,
                  client);
        sdsfree(client);
    }
    addReply(c, shared.ok);
}

void roleCommand(
        client *c) {

    if (server.masterhost == NULL) {
        listIter li;
        listNode *ln;
        void *mbcount;
        int slaves = 0;

        addReplyMultiBulkLen(c, 3);
        addReplyBulkCBuffer(c, "master", 6);
        addReplyLongLong(c, server.master_repl_offset);
        mbcount = addDeferredMultiBulkLength(c);
        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            char ip[NET_IP_STR_LEN], *slaveip = slave->slave_ip;

            if (slaveip[0] == '\0') {
                if (anetPeerToString(slave->fd, ip, sizeof(ip), NULL) == -1)
                    continue;
                slaveip = ip;
            }
            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            addReplyMultiBulkLen(c, 3);
            addReplyBulkCString(c, slaveip);
            addReplyBulkLongLong(c, slave->slave_listening_port);
            addReplyBulkLongLong(c, slave->repl_ack_off);
            slaves++;
        }
        setDeferredMultiBulkLength(c, mbcount, slaves);
    } else {
        char *slavestate = NULL;

        addReplyMultiBulkLen(c, 5);
        addReplyBulkCBuffer(c, "slave", 5);
        addReplyBulkCString(c, server.masterhost);
        addReplyLongLong(c, server.masterport);
        if (slaveIsInHandshakeState()) {
            slavestate = "handshake";
        } else {
            switch (server.repl_state) {
                case REPL_STATE_NONE:
                    slavestate = "none";
                    break;
                case REPL_STATE_CONNECT:
                    slavestate = "connect";
                    break;
                case REPL_STATE_CONNECTING:
                    slavestate = "connecting";
                    break;
                case REPL_STATE_TRANSFER:
                    slavestate = "sync";
                    break;
                case REPL_STATE_CONNECTED:
                    slavestate = "connected";
                    break;
                default:
                    slavestate = "unknown";
                    break;
            }
        }
        addReplyBulkCString(c, slavestate);
        addReplyLongLong(c, server.master ? server.master->reploff : -1);
    }
}

void replicationSendAck(void) {

    client *c = server.master;

    if (c != NULL) {
        c->flags |= CLIENT_MASTER_FORCE_REPLY;
        addReplyMultiBulkLen(c, 3);
        addReplyBulkCString(c, "REPLCONF");
        addReplyBulkCString(c, "ACK");
        addReplyBulkLongLong(c, c->reploff);
        c->flags &= ~CLIENT_MASTER_FORCE_REPLY;
    }
}

void replicationCacheMaster(client *c) {
    serverAssert(server.master != NULL && server.cached_master == NULL);
    serverLog(LL_NOTICE, "Caching the disconnected master state.");

    /* Unlink the client from the server structures. */
    unlinkClient(c);

    /* Reset the master client so that's ready to accept new commands:
     * we want to discard te non processed query buffers and non processed
     * offsets, including pending transactions, already populated arguments,
     * pending outputs to the master. */
    sdsclear(server.master->querybuf);
    sdsclear(server.master->pending_querybuf);
    server.master->read_reploff = server.master->reploff;
    if (c->flags & CLIENT_MULTI) discardTransaction(c);
    listEmpty(c->reply);
    c->bufpos = 0;
    resetClient(c);

    /* Save the master. Server.master will be set to null later by
     * replicationHandleMasterDisconnection(). */
    server.cached_master = server.master;

    /* Invalidate the Peer ID cache. */
    if (c->peerid) {
        sdsfree(c->peerid);
        c->peerid = NULL;
    }

    /* Caching the master happens instead of the actual freeClient() call,
     * so make sure to adjust the replication state. This function will
     * also set server.master to NULL. */
    replicationHandleMasterDisconnection();
}

/**
 * 复制缓存
 */
void replicationCacheMasterUsingMyself(
        void) {

    server.master_initial_offset = server.master_repl_offset;
    replicationCreateMasterClient(-1, -1);

    memcpy(server.master->replid, server.replid, sizeof(server.replid));

    unlinkClient(server.master);
    server.cached_master = server.master;
    server.master = NULL;
    serverLog(LL_NOTICE,
              "Before turning into a slave, using my master parameters to synthesize a cached master: I may be able to synchronize with the new master with just a partial transfer.");
}

/* Free a cached master, called when there are no longer the conditions for
 * a partial resync on reconnection. */
void replicationDiscardCachedMaster(void) {
    if (server.cached_master == NULL) return;

    serverLog(LL_NOTICE, "Discarding previously cached master state.");
    server.cached_master->flags &= ~CLIENT_MASTER;
    freeClient(server.cached_master);
    server.cached_master = NULL;
}

/* Turn the cached master into the current master, using the file descriptor
 * passed as argument as the socket for the new master.
 *
 * This function is called when successfully setup a partial resynchronization
 * so the stream of data that we'll receive will start from were this
 * master left. */
void replicationResurrectCachedMaster(int newfd) {
    server.master = server.cached_master;
    server.cached_master = NULL;
    server.master->fd = newfd;
    server.master->flags &= ~(CLIENT_CLOSE_AFTER_REPLY | CLIENT_CLOSE_ASAP);
    server.master->authenticated = 1;
    server.master->lastinteraction = server.unixtime;
    server.repl_state = REPL_STATE_CONNECTED;

    /* Re-add to the list of clients. */
    listAddNodeTail(server.clients, server.master);
    if (aeCreateFileEvent(server.el, newfd, AE_READABLE,
                          readQueryFromClient, server.master)) {
        serverLog(LL_WARNING, "Error resurrecting the cached master, impossible to add the readable handler: %s",
                  strerror(errno));
        freeClientAsync(server.master); /* Close ASAP. */
    }

    /* We may also need to install the write handler as well if there is
     * pending data in the write buffers. */
    if (clientHasPendingReplies(server.master)) {
        if (aeCreateFileEvent(server.el, newfd, AE_WRITABLE,
                              sendReplyToClient, server.master)) {
            serverLog(LL_WARNING, "Error resurrecting the cached master, impossible to add the writable handler: %s",
                      strerror(errno));
            freeClientAsync(server.master); /* Close ASAP. */
        }
    }
}

/* ------------------------- MIN-SLAVES-TO-WRITE  --------------------------- */

/* This function counts the number of slaves with lag <= min-slaves-max-lag.
 * If the option is active, the server will prevent writes if there are not
 * enough connected slaves with the specified lag (or less). */
void refreshGoodSlavesCount(void) {
    listIter li;
    listNode *ln;
    int good = 0;

    if (!server.repl_min_slaves_to_write ||
        !server.repl_min_slaves_max_lag)
        return;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;
        time_t lag = server.unixtime - slave->repl_ack_time;

        if (slave->replstate == SLAVE_STATE_ONLINE &&
            lag <= server.repl_min_slaves_max_lag)
            good++;
    }
    server.repl_good_slaves_count = good;
}

/* ----------------------- REPLICATION SCRIPT CACHE --------------------------
 * The goal of this code is to keep track of scripts already sent to every
 * connected slave, in order to be able to replicate EVALSHA as it is without
 * translating it to EVAL every time it is possible.
 *
 * We use a capped collection implemented by a hash table for fast lookup
 * of scripts we can send as EVALSHA, plus a linked list that is used for
 * eviction of the oldest entry when the max number of items is reached.
 *
 * We don't care about taking a different cache for every different slave
 * since to fill the cache again is not very costly, the goal of this code
 * is to avoid that the same big script is trasmitted a big number of times
 * per second wasting bandwidth and processor speed, but it is not a problem
 * if we need to rebuild the cache from scratch from time to time, every used
 * script will need to be transmitted a single time to reappear in the cache.
 *
 * This is how the system works:
 *
 * 1) Every time a new slave connects, we flush the whole script cache.
 * 2) We only send as EVALSHA what was sent to the master as EVALSHA, without
 *    trying to convert EVAL into EVALSHA specifically for slaves.
 * 3) Every time we trasmit a script as EVAL to the slaves, we also add the
 *    corresponding SHA1 of the script into the cache as we are sure every
 *    slave knows about the script starting from now.
 * 4) On SCRIPT FLUSH command, we replicate the command to all the slaves
 *    and at the same time flush the script cache.
 * 5) When the last slave disconnects, flush the cache.
 * 6) We handle SCRIPT LOAD as well since that's how scripts are loaded
 *    in the master sometimes.
 */

/* Initialize the script cache, only called at startup. */
void replicationScriptCacheInit(void) {
    server.repl_scriptcache_size = 10000;
    server.repl_scriptcache_dict = dictCreate(&replScriptCacheDictType, NULL);
    server.repl_scriptcache_fifo = listCreate();
}

void replicationScriptCacheFlush(
        void) {

    dictEmpty(server.repl_scriptcache_dict, NULL);
    listRelease(server.repl_scriptcache_fifo);
    server.repl_scriptcache_fifo = listCreate();
}

/* Add an entry into the script cache, if we reach max number of entries the
 * oldest is removed from the list. */
void replicationScriptCacheAdd(sds sha1) {
    int retval;
    sds key = sdsdup(sha1);

    /* Evict oldest. */
    if (listLength(server.repl_scriptcache_fifo) == server.repl_scriptcache_size) {
        listNode *ln = listLast(server.repl_scriptcache_fifo);
        sds oldest = listNodeValue(ln);

        retval = dictDelete(server.repl_scriptcache_dict, oldest);
        serverAssert(retval == DICT_OK);
        listDelNode(server.repl_scriptcache_fifo, ln);
    }

    /* Add current. */
    retval = dictAdd(server.repl_scriptcache_dict, key, NULL);
    listAddNodeHead(server.repl_scriptcache_fifo, key);
    serverAssert(retval == DICT_OK);
}

int replicationScriptCacheExists(
        sds sha1) {
    return dictFind(server.repl_scriptcache_dict, sha1) != NULL;
}

void replicationRequestAckFromSlaves(
        void) {

    server.get_ack_from_slaves = 1;
}

int replicationCountAcksByOffset(
        long long offset) {

    listIter li;
    listNode *ln;
    int count = 0;

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        if (slave->replstate != SLAVE_STATE_ONLINE) continue;
        if (slave->repl_ack_off >= offset) count++;
    }
    return count;
}

/* WAIT for N replicas to acknowledge the processing of our latest
 * write command (and all the previous commands). */
void waitCommand(client *c) {
    mstime_t timeout;
    long numreplicas, ackreplicas;
    long long offset = c->woff;

    if (server.masterhost) {
        addReplyError(c,
                      "WAIT cannot be used with slave instances. Please also note that since Redis 4.0 if a slave is configured to be writable (which is not the default) writes to slaves are just local and are not propagated.");
        return;
    }

    /* Argument parsing. */
    if (getLongFromObjectOrReply(c, c->argv[1], &numreplicas, NULL) != C_OK)
        return;
    if (getTimeoutFromObjectOrReply(c, c->argv[2], &timeout, UNIT_MILLISECONDS)
        != C_OK)
        return;

    /* First try without blocking at all. */
    ackreplicas = replicationCountAcksByOffset(c->woff);
    if (ackreplicas >= numreplicas || c->flags & CLIENT_MULTI) {
        addReplyLongLong(c, ackreplicas);
        return;
    }

    /* Otherwise block the client and put it into our list of clients
     * waiting for ack from slaves. */
    c->bpop.timeout = timeout;
    c->bpop.reploffset = offset;
    c->bpop.numreplicas = numreplicas;
    listAddNodeTail(server.clients_waiting_acks, c);
    blockClient(c, BLOCKED_WAIT);

    /* Make sure that the server will send an ACK request to all the slaves
     * before returning to the event loop. */
    replicationRequestAckFromSlaves();
}

/* This is called by unblockClient() to perform the blocking op type
 * specific cleanup. We just remove the client from the list of clients
 * waiting for replica acks. Never call it directly, call unblockClient()
 * instead. */
void unblockClientWaitingReplicas(client *c) {
    listNode *ln = listSearchKey(server.clients_waiting_acks, c);
    serverAssert(ln != NULL);
    listDelNode(server.clients_waiting_acks, ln);
}

/* Check if there are clients blocked in WAIT that can be unblocked since
 * we received enough ACKs from slaves. */
void processClientsWaitingReplicas(void) {
    long long last_offset = 0;
    int last_numreplicas = 0;

    listIter li;
    listNode *ln;

    listRewind(server.clients_waiting_acks, &li);
    while ((ln = listNext(&li))) {
        client *c = ln->value;

        /* Every time we find a client that is satisfied for a given
         * offset and number of replicas, we remember it so the next client
         * may be unblocked without calling replicationCountAcksByOffset()
         * if the requested offset / replicas were equal or less. */
        if (last_offset && last_offset > c->bpop.reploffset &&
            last_numreplicas > c->bpop.numreplicas) {
            unblockClient(c);
            addReplyLongLong(c, last_numreplicas);
        } else {
            int numreplicas = replicationCountAcksByOffset(c->bpop.reploffset);

            if (numreplicas >= c->bpop.numreplicas) {
                last_offset = c->bpop.reploffset;
                last_numreplicas = numreplicas;
                unblockClient(c);
                addReplyLongLong(c, numreplicas);
            }
        }
    }
}

/* Return the slave replication offset for this instance, that is
 * the offset for which we already processed the master replication stream. */
long long replicationGetSlaveOffset(void) {
    long long offset = 0;

    if (server.masterhost != NULL) {
        if (server.master) {
            offset = server.master->reploff;
        } else if (server.cached_master) {
            offset = server.cached_master->reploff;
        }
    }
    /* offset may be -1 when the master does not support it at all, however
     * this function is designed to return an offset that can express the
     * amount of data processed by the master, so we return a positive
     * integer. */
    if (offset < 0) offset = 0;
    return offset;
}

void replicationCron(
        void) {

    static long long replication_cron_loops = 0;
    // 连接中并且是握手状态并且超时了
    if (server.masterhost && (server.repl_state == REPL_STATE_CONNECTING || slaveIsInHandshakeState()) &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING, "Timeout connecting to the MASTER...");
        // 取消握手
        cancelReplicationHandshake();
    }

    // 获取rdb超时
    if (server.masterhost && server.repl_state == REPL_STATE_TRANSFER &&
        (time(NULL) - server.repl_transfer_lastio) > server.repl_timeout) {
        serverLog(LL_WARNING,
                  "Timeout receiving bulk data from MASTER... If the problem persists try to set the 'repl-timeout' parameter in redis.conf to a larger value.");
        // 取消复制
        cancelReplicationHandshake();
    }

    // 主机ping超时
    if (server.masterhost && server.repl_state == REPL_STATE_CONNECTED &&
        (time(NULL) - server.master->lastinteraction) > server.repl_timeout) {
        serverLog(LL_WARNING, "MASTER timeout: no data nor PING received...");
        // 释放主
        freeClient(server.master);
    }

    // 如果是重新连接主节点状态
    if (server.repl_state == REPL_STATE_CONNECT) {
        serverLog(LL_NOTICE, "Connecting to MASTER %s:%d", server.masterhost, server.masterport);
        // 建立非阻塞连接
        if (connectWithMaster() == C_OK) {
            serverLog(LL_NOTICE, "MASTER <-> SLAVE sync started");
        }
    }

    // 从节点在主线程中每隔1秒发送REPLCONF ACK <offset>命令,给主节点报告自己当前复制偏移量
    if (server.masterhost && server.master && !(server.master->flags & CLIENT_PRE_PSYNC))
        replicationSendAck();

    listIter li;
    listNode *ln;
    robj *ping_argv[1];

    // 首先,根据当前节点发送PING命令给从节点的频率发送PING命令(主节点默认10秒发送ping给从节点)
    // 如果当前节点是某以节点的主节点,那么发送PING给从节点
    if ((replication_cron_loops % server.repl_ping_slave_period) == 0 && listLength(server.slaves)) {
        ping_argv[0] = createStringObject("PING", 4);
        replicationFeedSlaves(server.slaves, server.slaveseldb, ping_argv, 1);
        decrRefCount(ping_argv[0]);
    }

    listRewind(server.slaves, &li);
    while ((ln = listNext(&li))) {
        client *slave = ln->value;

        int is_presync = (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START ||
                          (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_END &&
                           server.rdb_child_type != RDB_CHILD_TYPE_SOCKET));
        if (is_presync) {
            if (write(slave->fd, "\n", 1) == -1) {
            }
        }
    }

    // 断开超时的从节点
    if (listLength(server.slaves)) {
        listIter li;
        listNode *ln;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;

            if (slave->replstate != SLAVE_STATE_ONLINE) continue;
            if (slave->flags & CLIENT_PRE_PSYNC) continue;
            if ((server.unixtime - slave->repl_ack_time) > server.repl_timeout) {
                serverLog(LL_WARNING, "Disconnecting timedout slave: %s", replicationGetSlaveName(slave));
                freeClient(slave);
            }
        }
    }

    if (listLength(server.slaves) == 0 && server.repl_backlog_time_limit &&
        server.repl_backlog && server.masterhost == NULL) {
        time_t idle = server.unixtime - server.repl_no_slaves_since;

        if (idle > server.repl_backlog_time_limit) {
            freeReplicationBacklog();
            serverLog(LL_NOTICE,
                      "Replication backlog freed after %d seconds "
                      "without connected slaves.",
                      (int) server.repl_backlog_time_limit);
        }
    }

    if (listLength(server.slaves) == 0 && server.aof_state == AOF_OFF &&
        listLength(server.repl_scriptcache_fifo) != 0) {
        replicationScriptCacheFlush();
    }

    if (server.rdb_child_pid == -1 && server.aof_child_pid == -1) {
        time_t idle, max_idle = 0;
        int slaves_waiting = 0;
        int mincapa = -1;
        listNode *ln;
        listIter li;

        listRewind(server.slaves, &li);
        while ((ln = listNext(&li))) {
            client *slave = ln->value;
            if (slave->replstate == SLAVE_STATE_WAIT_BGSAVE_START) {
                idle = server.unixtime - slave->lastinteraction;
                if (idle > max_idle) max_idle = idle;
                slaves_waiting++;
                mincapa = (mincapa == -1) ? slave->slave_capa : (mincapa & slave->slave_capa);
            }
        }

        if (slaves_waiting &&
            (!server.repl_diskless_sync ||
             max_idle > server.repl_diskless_sync_delay)) {
        }
    }
    refreshGoodSlavesCount();
    replication_cron_loops++;
}