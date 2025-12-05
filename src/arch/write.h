#ifndef NOSTR_INTERNAL_WRITE_H_
#define NOSTR_INTERNAL_WRITE_H_

#include "linux/x86_64/write.h"

static inline ssize_t internal_write(const int32_t fd, const void* buf, const size_t count)
{
  return linux_x8664_write(fd, buf, count);
}
#endif
