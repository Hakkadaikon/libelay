#include "../../../arch/fstat.h"
#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "wal_manager.h"

// ============================================================================
// wal_checkpoint
// ============================================================================
NostrDBError wal_checkpoint(WalManager* wal, BufferPool* pool)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);

  // 1. Flush all dirty pages in the buffer pool to disk
  NostrDBError err = buffer_pool_flush_all(pool);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // 2. Write a CHECKPOINT record to the WAL
  uint32_t total = sizeof(WalRecordHeader);
  if (wal->buffer_used + total > wal->buffer_size) {
    err = wal_flush(wal);
    if (err != NOSTR_DB_OK) {
      return err;
    }
  }

  lsn_t lsn = wal->next_lsn++;

  WalRecordHeader hdr;
  internal_memset(&hdr, 0, sizeof(hdr));
  hdr.lsn         = lsn;
  hdr.prev_lsn    = LSN_NULL;
  hdr.tx_id       = 0;  // Checkpoint is not associated with a transaction
  hdr.type        = WAL_RECORD_CHECKPOINT;
  hdr.data_length = 0;

  internal_memcpy(wal->buffer + wal->buffer_used, &hdr, sizeof(hdr));
  wal->buffer_used += sizeof(hdr);

  // 3. Flush WAL buffer to ensure checkpoint is durable
  err = wal_flush(wal);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // 4. Truncate WAL file to reclaim space
  // After checkpoint, all prior records are no longer needed for recovery.
  // We can truncate the file and reset the file offset.
  if (internal_ftruncate(wal->fd, 0) < 0) {
    // Non-fatal: WAL is still consistent, just won't reclaim space
    return NOSTR_DB_OK;
  }
  wal->file_offset = 0;

  return NOSTR_DB_OK;
}
