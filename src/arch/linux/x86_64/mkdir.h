#ifndef NOSTR_LINUX_X86_64_MKDIR_H_
#define NOSTR_LINUX_X86_64_MKDIR_H_

#include "../../../util/types.h"
#include "../errno.h"
#include "../fcntl_def.h"
#include "./asm.h"

static inline int32_t linux_x8664_mkdir(const char* pathname, uint32_t mode)
{
  int32_t ret = linux_x8664_asm_syscall2(
    __NR_mkdir,
    pathname,
    mode);

  SYSCALL_EARLY_RETURN(ret);
  return ret;
}

#endif
