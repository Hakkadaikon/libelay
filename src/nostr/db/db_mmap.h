#ifndef NOSTR_DB_MMAP_H_
#define NOSTR_DB_MMAP_H_

#include "../../util/types.h"
#include "db_types.h"

/**
 * @brief Memory map a file
 * @param fd File descriptor
 * @param size Size to map
 * @param writable true for read-write mapping, false for read-only
 * @return Mapped address on success, NULL on error
 */
void* nostr_db_mmap_file(int32_t fd, size_t size, bool writable);

/**
 * @brief Unmap a memory-mapped region
 * @param addr Mapped address
 * @param size Size of mapped region
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_munmap(void* addr, size_t size);

/**
 * @brief Sync mapped memory to disk
 * @param addr Mapped address
 * @param size Size of region to sync
 * @param async true for asynchronous sync (MS_ASYNC), false for synchronous (MS_SYNC)
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_msync(void* addr, size_t size, bool async);

/**
 * @brief Extend a memory-mapped file (remap with larger size)
 * @param old_addr Current mapped address
 * @param old_size Current mapped size
 * @param new_size New size
 * @return New mapped address on success, NULL on error
 */
void* nostr_db_mmap_extend(void* old_addr, size_t old_size, size_t new_size);

#endif
