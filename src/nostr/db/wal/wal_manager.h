#ifndef NOSTR_DB_WAL_MANAGER_H_
#define NOSTR_DB_WAL_MANAGER_H_

#include "../../../util/types.h"
#include "../buffer/buffer_pool.h"
#include "../db_types.h"
#include "../disk/disk_manager.h"
#include "wal_types.h"

// ============================================================================
// Initialization and shutdown
// ============================================================================

/**
 * @brief Initialize the WAL manager, opening or creating the WAL file
 * @param wal WAL manager instance (caller-provided)
 * @param path WAL file path (e.g., "data/wal.log")
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_init(WalManager* wal, const char* path);

/**
 * @brief Shutdown the WAL manager (flush buffer, close file, free resources)
 * @param wal WAL manager instance
 */
void wal_shutdown(WalManager* wal);

// ============================================================================
// Transaction logging
// ============================================================================

/**
 * @brief Begin a new transaction, returning the assigned transaction ID
 * @param wal WAL manager instance
 * @return Transaction ID (0 on failure)
 */
uint32_t wal_log_begin(WalManager* wal);

/**
 * @brief Log a page update (before/after image)
 * @param wal WAL manager instance
 * @param tx_id Transaction ID
 * @param page_id Target page
 * @param offset Offset within the page
 * @param length Size of the change
 * @param old_data Before-image data (for undo)
 * @param new_data After-image data (for redo)
 * @return LSN of the update record, or LSN_NULL on failure
 */
lsn_t wal_log_update(WalManager* wal, uint32_t tx_id, page_id_t page_id,
                     uint16_t offset, uint16_t length, const void* old_data,
                     const void* new_data);

/**
 * @brief Log a page allocation
 * @param wal WAL manager instance
 * @param tx_id Transaction ID
 * @param page_id Allocated page ID
 * @return LSN of the record, or LSN_NULL on failure
 */
lsn_t wal_log_alloc_page(WalManager* wal, uint32_t tx_id, page_id_t page_id);

/**
 * @brief Log a page deallocation
 * @param wal WAL manager instance
 * @param tx_id Transaction ID
 * @param page_id Freed page ID
 * @return LSN of the record, or LSN_NULL on failure
 */
lsn_t wal_log_free_page(WalManager* wal, uint32_t tx_id, page_id_t page_id);

/**
 * @brief Commit a transaction (writes COMMIT record and flushes WAL buffer)
 * @param wal WAL manager instance
 * @param tx_id Transaction ID
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_log_commit(WalManager* wal, uint32_t tx_id);

/**
 * @brief Abort a transaction (writes ABORT record)
 * @param wal WAL manager instance
 * @param tx_id Transaction ID
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_log_abort(WalManager* wal, uint32_t tx_id);

// ============================================================================
// Flushing
// ============================================================================

/**
 * @brief Flush the WAL buffer to disk (pwrite + fdatasync)
 * @param wal WAL manager instance
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_flush(WalManager* wal);

// ============================================================================
// Recovery
// ============================================================================

/**
 * @brief Recover from WAL at startup (Redo/Undo phases)
 * @param wal WAL manager instance
 * @param disk Disk manager for page I/O
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_recover(WalManager* wal, DiskManager* disk);

// ============================================================================
// Checkpoint
// ============================================================================

/**
 * @brief Write a checkpoint (flush dirty pages, write CHECKPOINT record)
 * @param wal WAL manager instance
 * @param pool Buffer pool to flush
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError wal_checkpoint(WalManager* wal, BufferPool* pool);

#endif
