#ifndef NOSTR_INTERNAL_MKDIR_H_
#define NOSTR_INTERNAL_MKDIR_H_

#include "linux/x86_64/mkdir.h"

static inline int32_t internal_mkdir(const char* pathname, uint32_t mode)
{
  return linux_x8664_mkdir(pathname, mode);
}

#endif
