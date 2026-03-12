#ifndef NOSTR_LINUX_X86_64_FILE_WRITE_H_
#define NOSTR_LINUX_X86_64_FILE_WRITE_H_

#include "../../../util/types.h"
#include "../errno.h"
#include "./asm.h"

static inline ssize_t linux_x8664_pwrite(int32_t fd, const void* buf, size_t count, int64_t offset)
{
  int32_t ret = linux_x8664_asm_syscall4(
    __NR_pwrite64,
    fd,
    buf,
    count,
    offset);

  SYSCALL_SIZE_EARLY_RETURN(ret);
  return (ssize_t)ret;
}

#endif
