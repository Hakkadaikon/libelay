#ifndef NOSTR_DB_DISK_MANAGER_H_
#define NOSTR_DB_DISK_MANAGER_H_

#include "../../../util/types.h"
#include "../db_types.h"
#include "disk_types.h"

// ============================================================================
// Initialization and shutdown
// ============================================================================

/**
 * @brief Open an existing database file
 * @param dm Disk manager instance (caller-provided)
 * @param path File path
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_manager_open(DiskManager* dm, const char* path);

/**
 * @brief Create a new database file
 * @param dm Disk manager instance (caller-provided)
 * @param path File path
 * @param initial_pages Number of initial pages to allocate
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_manager_create(DiskManager* dm, const char* path, uint32_t initial_pages);

/**
 * @brief Close the database file (syncs header before closing)
 * @param dm Disk manager instance
 */
void disk_manager_close(DiskManager* dm);

// ============================================================================
// Page I/O
// ============================================================================

/**
 * @brief Read a page from disk
 * @param dm Disk manager instance
 * @param page_id Page ID to read
 * @param out Output page buffer
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out);

/**
 * @brief Write a page to disk
 * @param dm Disk manager instance
 * @param page_id Page ID to write
 * @param data Page data to write
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id, const PageData* data);

/**
 * @brief Sync all pending writes to disk
 * @param dm Disk manager instance
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_sync(DiskManager* dm);

// ============================================================================
// Page allocation and deallocation
// ============================================================================

/**
 * @brief Allocate a new page
 * @param dm Disk manager instance
 * @return Allocated page ID, or PAGE_ID_NULL on failure
 */
page_id_t disk_alloc_page(DiskManager* dm);

/**
 * @brief Free a page (return to free list)
 * @param dm Disk manager instance
 * @param page_id Page ID to free
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_free_page(DiskManager* dm, page_id_t page_id);

/**
 * @brief Extend file by adding more pages
 * @param dm Disk manager instance
 * @param additional_pages Number of pages to add
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_extend(DiskManager* dm, uint32_t additional_pages);

/**
 * @brief Flush the cached header to disk (page 0)
 * @param dm Disk manager instance
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError disk_flush_header(DiskManager* dm);

#endif
