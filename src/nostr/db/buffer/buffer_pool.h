#ifndef NOSTR_DB_BUFFER_POOL_H_
#define NOSTR_DB_BUFFER_POOL_H_

#include "../../../util/types.h"
#include "../db_types.h"
#include "../disk/disk_manager.h"
#include "buffer_types.h"

// ============================================================================
// Initialization and shutdown
// ============================================================================

/**
 * @brief Initialize the buffer pool
 * @param pool Buffer pool instance (caller-provided)
 * @param disk Disk manager to use for I/O (must outlive the pool)
 * @param pool_size Number of page frames to allocate
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError buffer_pool_init(BufferPool* pool, DiskManager* disk, uint32_t pool_size);

/**
 * @brief Shutdown the buffer pool (flushes all dirty pages)
 * @param pool Buffer pool instance
 */
void buffer_pool_shutdown(BufferPool* pool);

// ============================================================================
// Page access
// ============================================================================

/**
 * @brief Pin a page in the buffer pool, loading from disk if necessary
 *
 * Increments the pin count. The caller MUST call buffer_pool_unpin()
 * when done using the page. A pinned page will not be evicted.
 *
 * @param pool Buffer pool instance
 * @param page_id Page ID to pin
 * @return Pointer to page data, or NULL on failure
 */
PageData* buffer_pool_pin(BufferPool* pool, page_id_t page_id);

/**
 * @brief Allocate a new page on disk and pin it in the buffer pool
 *
 * The returned page is zeroed, pinned (pin_count=1), and marked dirty.
 *
 * @param pool Buffer pool instance
 * @param out_page Receives pointer to the new page data (may be NULL)
 * @return Allocated page ID, or PAGE_ID_NULL on failure
 */
page_id_t buffer_pool_alloc_page(BufferPool* pool, PageData** out_page);

/**
 * @brief Unpin a page (decrement pin count)
 * @param pool Buffer pool instance
 * @param page_id Page ID to unpin
 */
void buffer_pool_unpin(BufferPool* pool, page_id_t page_id);

// ============================================================================
// Dirty tracking
// ============================================================================

/**
 * @brief Mark a page as dirty
 * @param pool Buffer pool instance
 * @param page_id Page ID to mark
 * @param lsn Log sequence number of the modification (0 if WAL not yet active)
 */
void buffer_pool_mark_dirty(BufferPool* pool, page_id_t page_id, uint64_t lsn);

// ============================================================================
// Flushing
// ============================================================================

/**
 * @brief Flush a specific dirty page to disk
 * @param pool Buffer pool instance
 * @param page_id Page ID to flush
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError buffer_pool_flush(BufferPool* pool, page_id_t page_id);

/**
 * @brief Flush all dirty pages to disk
 * @param pool Buffer pool instance
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError buffer_pool_flush_all(BufferPool* pool);

#endif
