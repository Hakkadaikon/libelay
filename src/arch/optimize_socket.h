#ifndef NOSTR_INTERNAL_OPTIMIZE_SOCKET_H_
#define NOSTR_INTERNAL_OPTIMIZE_SOCKET_H_

#include "linux/x86_64/optimize_socket.h"

static inline int32_t internal_fcntl(const int32_t fd, const int32_t cmd, const int64_t arg)
{
    return linux_x8664_fcntl(fd, cmd, arg);
}

static inline int32_t internal_setsockopt(
    const int32_t   sock_fd,
    const int32_t   level,
    const int32_t   optname,
    const void*     optval,
    const socklen_t optlen)
{
    return linux_x8664_setsockopt(sock_fd, level, optname, optval, optlen);
}

#endif
