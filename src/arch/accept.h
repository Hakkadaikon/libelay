#ifndef NOSTR_INTERNAL_ACCEPT_H_
#define NOSTR_INTERNAL_ACCEPT_H_

#include "../util/types.h"
#include "linux/x86_64/accept.h"

static inline int32_t internal_accept(const int32_t sock_fd, struct sockaddr* addr, socklen_t* addrlen, const int32_t flags)
{
  return linux_x8664_accept4(sock_fd, addr, addrlen, flags);
}

#endif
