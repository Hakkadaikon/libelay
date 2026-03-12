#ifndef NOSTR_INTERNAL_FILE_WRITE_H_
#define NOSTR_INTERNAL_FILE_WRITE_H_

#include "linux/x86_64/file_write.h"

static inline ssize_t internal_pwrite(int32_t fd, const void* buf, size_t count, int64_t offset)
{
  return linux_x8664_pwrite(fd, buf, count, offset);
}

#endif
