/**
 * evport模块
 *
 *      用于Solaris 10以上系统
 */
#include <assert.h>
#include <errno.h>
#include <port.h>
#include <poll.h>

#include <sys/types.h>
#include <sys/time.h>

#include <stdio.h>

static int evport_debug = 0;

#define MAX_EVENT_BATCHSZ 512

typedef struct aeApiState {
    int     portfd;                             /* event port */
    int     npending;                           /* # of pending fds */
    int     pending_fds[MAX_EVENT_BATCHSZ];     /* pending fds */
    int     pending_masks[MAX_EVENT_BATCHSZ];   /* pending fds' masks */
} aeApiState;

static int aeApiCreate(
        aeEventLoop *eventLoop) {

    int i;
    aeApiState *state = zmalloc(sizeof(aeApiState));
    if (!state) return -1;
    // 创建Event ports队列
    state->portfd = port_create();
    if (state->portfd == -1) {
        zfree(state);
        return -1;
    }

    state->npending = 0;
    for (i = 0; i < MAX_EVENT_BATCHSZ; i++) {
        state->pending_fds[i] = -1;
        state->pending_masks[i] = AE_NONE;
    }
    eventLoop->apidata = state;
    return 0;
}

static int aeApiResize(
        aeEventLoop *eventLoop,
        int setsize) {

    /* Nothing to resize here. */
    return 0;
}

static void aeApiFree(
        aeEventLoop *eventLoop) {

    aeApiState *state = eventLoop->apidata;

    close(state->portfd);
    zfree(state);
}

static int aeApiLookupPending(
        aeApiState *state,
        int fd) {

    int i;

    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == fd)
            return (i);
    }

    return (-1);
}

static int aeApiAssociate(
        const char *where,
        int portfd,
        int fd,
        int mask) {

    int events = 0;
    int rv, err;

    // 读
    if (mask & AE_READABLE)
        events |= POLLIN;
    // 写
    if (mask & AE_WRITABLE)
        events |= POLLOUT;

    if (evport_debug)
        fprintf(stderr, "%s: port_associate(%d, 0x%x) = ", where, fd, events);

    // 将event与port进行关联
    rv = port_associate(portfd, PORT_SOURCE_FD, fd, events,(void *)(uintptr_t)mask);
    err = errno;
    if (evport_debug)
        fprintf(stderr, "%d (%s)\n", rv, rv == 0 ? "no error" : strerror(err));

    if (rv == -1) {
        fprintf(stderr, "%s: port_associate: %s\n", where, strerror(err));
        if (err == EAGAIN)
            fprintf(stderr, "aeApiAssociate: event port limit exceeded.");
    }
    return rv;
}

/**
 * 添加事件
 *
 * @param eventLoop
 * @param fd
 * @param mask
 */
static int aeApiAddEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;
    if (evport_debug)
        fprintf(stderr, "aeApiAddEvent: fd %d mask 0x%x\n", fd, mask);
    fullmask = mask | eventLoop->events[fd].mask;
    pfd = aeApiLookupPending(state, fd);
    if (pfd != -1) {
        if (evport_debug)
            fprintf(stderr, "aeApiAddEvent: adding to pending fd %d\n", fd);
        state->pending_masks[pfd] |= fullmask;
        return 0;
    }
    return (aeApiAssociate("aeApiAddEvent", state->portfd, fd, fullmask));
}

static void aeApiDelEvent(
        aeEventLoop *eventLoop,
        int fd,
        int mask) {

    aeApiState *state = eventLoop->apidata;
    int fullmask, pfd;

    if (evport_debug)
        fprintf(stderr, "del fd %d mask 0x%x\n", fd, mask);

    pfd = aeApiLookupPending(state, fd);

    if (pfd != -1) {
        if (evport_debug)
            fprintf(stderr, "deleting event from pending fd %d\n", fd);
        state->pending_masks[pfd] &= ~mask;
        if (state->pending_masks[pfd] == AE_NONE)
            state->pending_fds[pfd] = -1;
        return;
    }

    fullmask = eventLoop->events[fd].mask;
    if (fullmask == AE_NONE) {
        if (evport_debug)
            fprintf(stderr, "aeApiDelEvent: port_dissociate(%d)\n", fd);

        if (port_dissociate(state->portfd, PORT_SOURCE_FD, fd) != 0) {
            perror("aeApiDelEvent: port_dissociate");
            abort(); /* will not return */
        }
    } else if (aeApiAssociate("aeApiDelEvent", state->portfd, fd,fullmask) != 0) {
        abort();
    }
}

static int aeApiPoll(
        aeEventLoop *eventLoop,
        struct timeval *tvp) {

    aeApiState *state = eventLoop->apidata;
    struct timespec timeout, *tsp;
    int mask, i;
    uint_t nevents;
    port_event_t event[MAX_EVENT_BATCHSZ];
    for (i = 0; i < state->npending; i++) {
        if (state->pending_fds[i] == -1)
            continue;
        if (aeApiAssociate("aeApiPoll", state->portfd,
            state->pending_fds[i], state->pending_masks[i]) != 0) {
            abort();
        }

        state->pending_masks[i] = AE_NONE;
        state->pending_fds[i] = -1;
    }

    state->npending = 0;

    if (tvp != NULL) {
        timeout.tv_sec = tvp->tv_sec;
        timeout.tv_nsec = tvp->tv_usec * 1000;
        tsp = &timeout;
    } else {
        tsp = NULL;
    }

    nevents = 1;
    // 取回多个触发事件的event
    if (port_getn(state->portfd, event, MAX_EVENT_BATCHSZ, &nevents,
        tsp) == -1 && (errno != ETIME || nevents == 0)) {
        if (errno == ETIME || errno == EINTR)
            return 0;

        perror("aeApiPoll: port_get");
        abort();
    }

    state->npending = nevents;

    for (i = 0; i < nevents; i++) {
            mask = 0;
            if (event[i].portev_events & POLLIN)
                mask |= AE_READABLE;
            if (event[i].portev_events & POLLOUT)
                mask |= AE_WRITABLE;

            eventLoop->fired[i].fd = event[i].portev_object;
            eventLoop->fired[i].mask = mask;

            if (evport_debug)
                fprintf(stderr, "aeApiPoll: fd %d mask 0x%x\n",
                    (int)event[i].portev_object, mask);

            state->pending_fds[i] = event[i].portev_object;
            state->pending_masks[i] = (uintptr_t)event[i].portev_user;
    }

    return nevents;
}

static char *aeApiName(
        void) {

    return "evport";
}
