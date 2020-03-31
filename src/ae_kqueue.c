/**
 * kqueue模块
 *
 *      UNIX操作系统上的复用I/O
 */
#include <sys/types.h>
#include <sys/event.h>
#include <sys/time.h>

typedef struct aeApiState {
    // 资源描述符
    int kqfd;
    // kequeu事件[内部定义事件类型,事件行为]
    struct kevent *events;
} aeApiState;

static int aeApiCreate(
        aeEventLoop *eventLoop) {

    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;
    state->events = zmalloc(sizeof(struct kevent) * eventLoop->setsize);
    if (!state->events) {
        zfree(state);
        return -1;
    }
    // 生成内核事件队列,返回队列描述符,其他api操作都是基于此描述符
    state->kqfd = kqueue();
    if (state->kqfd == -1) {
        zfree(state->events);
        zfree(state);
        return -1;
    }
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(
        aeEventLoop *eventLoop,
        int setsize) {

    aeApiState *state = eventLoop->apidata;
    state->events = zrealloc(state->events, sizeof(struct kevent) * setsize);
    return 0;
}

static void aeApiFree(
        aeEventLoop *eventLoop) {

    aeApiState *state = eventLoop->apidata;
    close(state->kqfd);
    zfree(state->events);
    zfree(state);
}

static int aeApiAddEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    struct kevent ke;
    // 如果是可读事件创建可读kevent
    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    // 如果是可写事件创建可写kevent
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_ADD, 0, 0, NULL);
        if (kevent(state->kqfd, &ke, 1, NULL, 0, NULL) == -1) return -1;
    }
    return 0;
}

static void aeApiDelEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    struct kevent ke;

    if (mask & AE_READABLE) {
        EV_SET(&ke, fd, EVFILT_READ, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
    if (mask & AE_WRITABLE) {
        EV_SET(&ke, fd, EVFILT_WRITE, EV_DELETE, 0, 0, NULL);
        kevent(state->kqfd, &ke, 1, NULL, 0, NULL);
    }
}

static int aeApiPoll(
        aeEventLoop *eventLoop,
        struct timeval *tvp) {

    aeApiState *state = eventLoop->apidata;
    int retval, numevents = 0;
    if (tvp != NULL) {
        struct timespec timeout;
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        // events已触发的事件,通过filter属性判断,通过内核进行callback
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize, &timeout);
    } else {
        retval = kevent(state->kqfd, NULL, 0, state->events, eventLoop->setsize, NULL);
    }
    // 代表有事件触发
    if (retval > 0) {
        int j;
        // 循环已经触发的事件[相比select少了循环那些没有被触发的]
        numevents = retval;
        for (j = 0; j < numevents; j++) {
            int mask = 0;
            struct kevent *e = state->events + j;
            // 读事件
            if (e->filter == EVFILT_READ) mask |= AE_READABLE;
            // 写事件
            if (e->filter == EVFILT_WRITE) mask |= AE_WRITABLE;
            eventLoop->fired[j].fd = e->ident;
            eventLoop->fired[j].mask = mask;
        }
    }
    return numevents;
}

static char *aeApiName(
        void) {

    return "kqueue";
}
