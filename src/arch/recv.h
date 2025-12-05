#ifndef NOSTR_INTERNAL_RECV_H_
#define NOSTR_INTERNAL_RECV_H_

#include "linux/x86_64/recv.h"

static inline ssize_t internal_recvfrom(
    const int32_t    sock_fd,
    void*            buf,
    const size_t     len,
    const int32_t    flags,
    struct sockaddr* src_addr,
    socklen_t*       addr_len)
{
  return linux_x8664_recvfrom(sock_fd, buf, len, flags, src_addr, addr_len);
}

#endif
