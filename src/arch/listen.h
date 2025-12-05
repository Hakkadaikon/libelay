#ifndef NOSTR_INTERNAL_LISTEN_H_
#define NOSTR_INTERNAL_LISTEN_H_

#include "../util/types.h"
#include "linux/x86_64/listen.h"

static inline int32_t internal_socket(const int32_t domain, const int32_t type, const int32_t protocol)
{
  return linux_x8664_socket(domain, type, protocol);
}

static inline int32_t internal_bind(const int32_t sockfd, const struct sockaddr* addr, const socklen_t addrlen)
{
  return linux_x8664_bind(sockfd, addr, addrlen);
}

static inline int32_t internal_listen(const int32_t sockfd, const int32_t backlog)
{
  return linux_x8664_listen(sockfd, backlog);
}

#endif
