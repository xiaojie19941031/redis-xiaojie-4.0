/**
 * select模块[bit置位]
 *
 *      在一段指定的时间内,监听用户感兴趣的文件描述符上可读、可写和异常等事件
 *
 *      使用一种有序的方式,对多个套接字进行统一管理与调度
 */
#include <sys/select.h>
#include <string.h>

typedef struct aeApiState {

    fd_set rfds, wfds;
    fd_set _rfds, _wfds;
} aeApiState;

/**
 * 创建复用I/O
 *
 * @param eventLoop
 */
static int aeApiCreate(
        aeEventLoop *eventLoop) {

    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;
    // 将fd_set读变量的所有位初始化为0
    FD_ZERO(&state->rfds);
    // 将fd_set写变量的所有位初始化为0
    FD_ZERO(&state->wfds);
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(
        aeEventLoop *eventLoop,
        int setsize) {

    if (setsize >= FD_SETSIZE) return -1;
    return 0;
}

static void aeApiFree(
        aeEventLoop *eventLoop) {

    zfree(eventLoop->apidata);
}

static int aeApiAddEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    // 在参数fdset指向的变量中注册文件描述符fd读的信息
    if (mask & AE_READABLE) FD_SET(fd, &state->rfds);
    // 在参数fdset指向的变量中注册文件描述符fd的写信息
    if (mask & AE_WRITABLE) FD_SET(fd, &state->wfds);
    return 0;
}

static void aeApiDelEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    // 参数fdset指向的变量中清除文件描述符fd的读信息
    if (mask & AE_READABLE) FD_CLR(fd, &state->rfds);
    // 参数fdset指向的变量中清除文件描述符fd的写信息
    if (mask & AE_WRITABLE) FD_CLR(fd, &state->wfds);
}

static int aeApiPoll(
        aeEventLoop *eventLoop,
        struct timeval *tvp) {

    aeApiState *state = eventLoop->apidata;
    int retval, j, numevents = 0;
    memcpy(&state->_rfds, &state->rfds, sizeof(fd_set));
    memcpy(&state->_wfds, &state->wfds, sizeof(fd_set));
    // 复用I/O,阻塞tvp时间,当有事件来时进行判断是读还是写[被监听的文件描述符的总数,可读,可写,异常,超时时间],耗费CPU
    retval = select(eventLoop->maxfd + 1, &state->_rfds, &state->_wfds, NULL, tvp);
    // 超时返回0;失败返回-1,因关注的事件返回时,返回大于0的值,该值是发生事件的文件描述符数
    if (retval > 0) {
        // 循环文件资源SOCKET
        for (j = 0; j <= eventLoop->maxfd; j++) {
            int mask = 0;
            aeFileEvent *fe = &eventLoop->events[j];
            // 没有事件
            if (fe->mask == AE_NONE) continue;
            // 判断是否设置了监听读事件
            if (fe->mask & AE_READABLE && FD_ISSET(j, &state->_rfds))
                mask |= AE_READABLE;
            // 判断是否设置了监听写事件
            if (fe->mask & AE_WRITABLE && FD_ISSET(j, &state->_wfds))
                mask |= AE_WRITABLE;
            eventLoop->fired[numevents].fd = j;
            eventLoop->fired[numevents].mask = mask;
            numevents++;
        }
    }
    return numevents;
}

static char *aeApiName(
        void) {

    return "select";
}
