#include "db_mmap.h"

#include "../../arch/mmap.h"
#include "../../util/string.h"

void* nostr_db_mmap_file(int32_t fd, size_t size, bool writable)
{
  require(fd >= 0, NULL);
  require(size > 0, NULL);

  int32_t prot = PROT_READ;
  if (writable) {
    prot |= PROT_WRITE;
  }

  void* addr = internal_mmap(NULL, size, prot, MAP_SHARED, fd, 0);
  if (addr == MAP_FAILED) {
    return NULL;
  }

  return addr;
}

int32_t nostr_db_munmap(void* addr, size_t size)
{
  require_not_null(addr, -1);
  require(size > 0, -1);

  return internal_munmap(addr, size);
}

int32_t nostr_db_msync(void* addr, size_t size, bool async)
{
  require_not_null(addr, -1);
  require(size > 0, -1);

  int32_t flags = async ? MS_ASYNC : MS_SYNC;
  return internal_msync(addr, size, flags);
}

void* nostr_db_mmap_extend(void* old_addr, size_t old_size, size_t new_size)
{
  require_not_null(old_addr, NULL);
  require(old_size > 0, NULL);
  require(new_size > old_size, NULL);

  // Use mremap with MREMAP_MAYMOVE flag to allow the kernel to move the mapping
  void* new_addr = internal_mremap(old_addr, old_size, new_size, MREMAP_MAYMOVE);
  if (new_addr == MAP_FAILED) {
    return NULL;
  }

  return new_addr;
}
