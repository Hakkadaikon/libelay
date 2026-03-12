#include "../../../arch/file_write.h"
#include "../../../arch/fsync.h"
#include "../../../util/string.h"
#include "wal_manager.h"

// ============================================================================
// wal_flush
// ============================================================================
NostrDBError wal_flush(WalManager* wal)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);

  if (wal->buffer_used == 0) {
    return NOSTR_DB_OK;
  }

  require(wal->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);

  // Write buffer to WAL file at the current offset
  uint32_t written = 0;
  while (written < wal->buffer_used) {
    ssize_t ret = internal_pwrite(wal->fd, wal->buffer + written,
                                  wal->buffer_used - written,
                                  wal->file_offset + written);
    if (ret < 0) {
      return NOSTR_DB_ERROR_FILE_CREATE;  // Write error
    }
    written += (uint32_t)ret;
  }

  // Sync to disk
  if (internal_fdatasync(wal->fd) < 0) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  // Update file offset and flushed LSN
  wal->file_offset += written;

  // Scan the buffer to find the highest LSN that was flushed
  uint32_t pos = 0;
  while (pos + sizeof(WalRecordHeader) <= wal->buffer_used) {
    WalRecordHeader* hdr = (WalRecordHeader*)(wal->buffer + pos);
    if (hdr->lsn > wal->flushed_lsn) {
      wal->flushed_lsn = hdr->lsn;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }

  // Reset buffer
  wal->buffer_used = 0;

  return NOSTR_DB_OK;
}
