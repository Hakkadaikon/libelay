#ifndef NOSTR_INTERNAL_SEND_H_
#define NOSTR_INTERNAL_SEND_H_

#include "linux/x86_64/send.h"

static inline ssize_t internal_sendto(
    const int32_t    sock_fd,
    const void*      buf,
    const size_t     len,
    const int32_t    flags,
    struct sockaddr* dest_addr,
    const socklen_t  addr_len)
{
    return linux_x8664_sendto(sock_fd, buf, len, flags, dest_addr, addr_len);
}

#endif
