#include "../../../arch/file_read.h"
#include "../../../arch/fstat.h"
#include "../../../arch/memory.h"
#include "../../../arch/mmap.h"
#include "../../../util/string.h"
#include "wal_manager.h"

// ============================================================================
// Internal: Read the entire WAL file into memory via mmap
// Returns NULL on failure, sets *out_size to file size
// ============================================================================
static uint8_t* wal_mmap_file(int32_t fd, int64_t* out_size)
{
  LinuxStat st;
  if (internal_fstat(fd, &st) < 0) {
    return NULL;
  }

  *out_size = st.st_size;
  if (st.st_size == 0) {
    return NULL;  // Empty WAL, nothing to recover
  }

  // Allocate buffer and read the file
  void* buf = internal_mmap(NULL, (size_t)st.st_size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (buf == MAP_FAILED) {
    return NULL;
  }

  // Read WAL file contents into buffer
  int64_t total_read = 0;
  while (total_read < st.st_size) {
    ssize_t ret = internal_pread(fd, (uint8_t*)buf + total_read,
                                 (size_t)(st.st_size - total_read),
                                 total_read);
    if (ret <= 0) {
      internal_munmap(buf, (size_t)st.st_size);
      return NULL;
    }
    total_read += ret;
  }

  return (uint8_t*)buf;
}

// ============================================================================
// Internal: Check if a transaction committed
// Scan the WAL data for a COMMIT record matching tx_id
// ============================================================================
static bool tx_committed(const uint8_t* data, int64_t size, uint32_t tx_id)
{
  int64_t pos = 0;
  while (pos + (int64_t)sizeof(WalRecordHeader) <= size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(data + pos);
    if (hdr->type == WAL_RECORD_COMMIT && hdr->tx_id == tx_id) {
      return true;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }
  return false;
}

// ============================================================================
// Internal: Find the last checkpoint position in WAL data
// Returns offset of the checkpoint record header, or -1 if not found
// ============================================================================
static int64_t find_last_checkpoint(const uint8_t* data, int64_t size)
{
  int64_t last_cp = -1;
  int64_t pos     = 0;
  while (pos + (int64_t)sizeof(WalRecordHeader) <= size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(data + pos);
    if (hdr->type == WAL_RECORD_CHECKPOINT) {
      last_cp = pos;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }
  return last_cp;
}

// ============================================================================
// Internal: Redo phase - replay UPDATE records forward from start_pos
// Only applies updates where page LSN < record LSN
// ============================================================================
static NostrDBError redo_phase(const uint8_t* data, int64_t size,
                               int64_t start_pos, DiskManager* disk)
{
  int64_t pos = start_pos;

  while (pos + (int64_t)sizeof(WalRecordHeader) <= size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(data + pos);
    int64_t                next_pos =
      pos + (int64_t)sizeof(WalRecordHeader) + hdr->data_length;

    if (hdr->type == WAL_RECORD_UPDATE && hdr->data_length > 0) {
      // Only redo committed transactions
      if (tx_committed(data, size, hdr->tx_id)) {
        const uint8_t* payload_data =
          data + pos + sizeof(WalRecordHeader);
        const WalUpdatePayload* payload =
          (const WalUpdatePayload*)payload_data;

        // Read current page from disk
        PageData     page;
        NostrDBError err = disk_read_page(disk, payload->page_id, &page);
        if (err != NOSTR_DB_OK) {
          // Page may not exist yet (e.g., if alloc was part of same tx).
          // Skip this record if page can't be read.
          pos = next_pos;
          continue;
        }

        // Apply after-image (new_data) to the page
        const uint8_t* new_data =
          payload_data + sizeof(WalUpdatePayload) + payload->length;
        internal_memcpy(page.data + payload->offset, new_data,
                        payload->length);

        // Write page back to disk
        err = disk_write_page(disk, payload->page_id, &page);
        if (err != NOSTR_DB_OK) {
          return err;
        }
      }
    }

    pos = next_pos;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// Internal: Undo phase - rollback uncommitted transactions backward
// ============================================================================
static NostrDBError undo_phase(const uint8_t* data, int64_t size,
                               int64_t start_pos, DiskManager* disk)
{
  // Collect UPDATE record positions that need undo (uncommitted tx)
  // We process them in reverse order.
  // First, count how many UPDATE records from uncommitted tx exist.
  int64_t  pos   = start_pos;
  uint32_t count = 0;

  while (pos + (int64_t)sizeof(WalRecordHeader) <= size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(data + pos);
    if (hdr->type == WAL_RECORD_UPDATE &&
        !tx_committed(data, size, hdr->tx_id)) {
      count++;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }

  if (count == 0) {
    return NOSTR_DB_OK;  // Nothing to undo
  }

  // Allocate array to store positions of records to undo
  int64_t* positions = (int64_t*)internal_mmap(
    NULL, count * sizeof(int64_t), PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (positions == MAP_FAILED) {
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  // Collect positions
  pos          = start_pos;
  uint32_t idx = 0;
  while (pos + (int64_t)sizeof(WalRecordHeader) <= size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(data + pos);
    if (hdr->type == WAL_RECORD_UPDATE &&
        !tx_committed(data, size, hdr->tx_id)) {
      positions[idx++] = pos;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }

  // Process in reverse order (undo latest changes first)
  for (int32_t i = (int32_t)count - 1; i >= 0; i--) {
    const WalRecordHeader* hdr =
      (const WalRecordHeader*)(data + positions[i]);
    const uint8_t* payload_data =
      data + positions[i] + sizeof(WalRecordHeader);
    const WalUpdatePayload* payload =
      (const WalUpdatePayload*)payload_data;

    // Read current page
    PageData     page;
    NostrDBError err = disk_read_page(disk, payload->page_id, &page);
    if (err != NOSTR_DB_OK) {
      internal_munmap(positions, count * sizeof(int64_t));
      return err;
    }

    // Apply before-image (old_data) to restore original state
    const uint8_t* old_data =
      payload_data + sizeof(WalUpdatePayload);
    internal_memcpy(page.data + payload->offset, old_data,
                    payload->length);

    // Write page back
    err = disk_write_page(disk, payload->page_id, &page);
    if (err != NOSTR_DB_OK) {
      internal_munmap(positions, count * sizeof(int64_t));
      return err;
    }
  }

  internal_munmap(positions, count * sizeof(int64_t));
  return NOSTR_DB_OK;
}

// ============================================================================
// wal_recover
// ============================================================================
NostrDBError wal_recover(WalManager* wal, DiskManager* disk)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(disk, NOSTR_DB_ERROR_NULL_PARAM);
  require(wal->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);

  // Read entire WAL file
  int64_t  wal_size = 0;
  uint8_t* wal_data = wal_mmap_file(wal->fd, &wal_size);
  if (is_null(wal_data)) {
    // Empty WAL or read error - nothing to recover
    return NOSTR_DB_OK;
  }

  // Find last checkpoint position
  int64_t start_pos = find_last_checkpoint(wal_data, wal_size);
  if (start_pos < 0) {
    start_pos = 0;  // No checkpoint, start from beginning
  }

  // Redo phase: replay committed updates forward
  NostrDBError err = redo_phase(wal_data, wal_size, start_pos, disk);
  if (err != NOSTR_DB_OK) {
    internal_munmap(wal_data, (size_t)wal_size);
    return err;
  }

  // Undo phase: rollback uncommitted updates backward
  err = undo_phase(wal_data, wal_size, start_pos, disk);
  if (err != NOSTR_DB_OK) {
    internal_munmap(wal_data, (size_t)wal_size);
    return err;
  }

  // Sync disk after recovery
  disk_sync(disk);

  // Update WAL manager state from recovered data
  // Find max LSN and max tx_id in the WAL
  lsn_t    max_lsn   = 0;
  uint32_t max_tx_id = 0;
  int64_t  pos       = 0;
  while (pos + (int64_t)sizeof(WalRecordHeader) <= wal_size) {
    const WalRecordHeader* hdr = (const WalRecordHeader*)(wal_data + pos);
    if (hdr->lsn > max_lsn) {
      max_lsn = hdr->lsn;
    }
    if (hdr->tx_id > max_tx_id) {
      max_tx_id = hdr->tx_id;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }

  wal->next_lsn    = max_lsn + 1;
  wal->next_tx_id  = max_tx_id + 1;
  wal->flushed_lsn = max_lsn;

  internal_munmap(wal_data, (size_t)wal_size);

  return NOSTR_DB_OK;
}
