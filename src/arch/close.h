#ifndef NOSTR_INTERNAL_CLOSE_H_
#define NOSTR_INTERNAL_CLOSE_H_

#include "../util/types.h"
#include "linux/x86_64/close.h"

static inline int32_t internal_close(int32_t fd)
{
#ifdef __APPLE__
  return darwin_close(fd);
#else
  return linux_x8664_close(fd);
#endif
}

#endif
