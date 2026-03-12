#include "wal_manager.h"

#include "../../../arch/close.h"
#include "../../../arch/fstat.h"
#include "../../../arch/memory.h"
#include "../../../arch/mmap.h"
#include "../../../arch/open.h"
#include "../../../util/string.h"

// ============================================================================
// wal_init
// ============================================================================
NostrDBError wal_init(WalManager* wal, const char* path)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(path, NOSTR_DB_ERROR_NULL_PARAM);

  internal_memset(wal, 0, sizeof(WalManager));
  wal->fd = -1;

  // Try to open existing WAL file, or create a new one
  int32_t fd = internal_open(path, O_RDWR | O_CREAT, S_IRUSR | S_IWUSR);
  if (fd < 0) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }
  wal->fd = fd;

  // Get file size to determine write offset
  LinuxStat st;
  if (internal_fstat(fd, &st) < 0) {
    internal_close(fd);
    wal->fd = -1;
    return NOSTR_DB_ERROR_FSTAT_FAILED;
  }
  wal->file_offset = st.st_size;

  // Allocate WAL buffer via anonymous mmap
  void* buf = internal_mmap(NULL, WAL_BUFFER_SIZE, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (buf == MAP_FAILED) {
    internal_close(fd);
    wal->fd = -1;
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  wal->buffer      = (uint8_t*)buf;
  wal->buffer_size = WAL_BUFFER_SIZE;
  wal->buffer_used = 0;

  // Initialize LSN and TX counters
  // If recovering from existing WAL, these will be updated by wal_recover()
  wal->next_lsn   = 1;
  wal->next_tx_id = 1;

  return NOSTR_DB_OK;
}

// ============================================================================
// wal_shutdown
// ============================================================================
void wal_shutdown(WalManager* wal)
{
  if (is_null(wal)) {
    return;
  }

  // Flush any remaining buffered data
  if (wal->fd >= 0 && wal->buffer_used > 0) {
    wal_flush(wal);
  }

  // Free the WAL buffer
  if (!is_null(wal->buffer)) {
    internal_munmap(wal->buffer, wal->buffer_size);
  }

  // Close the WAL file
  if (wal->fd >= 0) {
    internal_close(wal->fd);
  }

  internal_memset(wal, 0, sizeof(WalManager));
  wal->fd = -1;
}
