#ifndef NOSTR_DB_WAL_TYPES_H_
#define NOSTR_DB_WAL_TYPES_H_

#include "../../../util/types.h"
#include "../disk/disk_types.h"

// ============================================================================
// Log Sequence Number
// ============================================================================
typedef uint64_t lsn_t;
#define LSN_NULL ((lsn_t)0)

// ============================================================================
// WAL record types
// ============================================================================
typedef enum {
  WAL_RECORD_BEGIN      = 1,  // Transaction begin
  WAL_RECORD_COMMIT     = 2,  // Transaction commit
  WAL_RECORD_ABORT      = 3,  // Transaction abort
  WAL_RECORD_UPDATE     = 4,  // Page update (before/after image)
  WAL_RECORD_ALLOC_PAGE = 5,  // Page allocation
  WAL_RECORD_FREE_PAGE  = 6,  // Page deallocation
  WAL_RECORD_CHECKPOINT = 7,  // Checkpoint marker
} WalRecordType;

// ============================================================================
// WAL record header (fixed-length 32 bytes)
// ============================================================================
typedef struct {
  lsn_t         lsn;          // LSN of this record
  lsn_t         prev_lsn;     // Previous LSN of the same transaction
  uint32_t      tx_id;        // Transaction ID
  WalRecordType type;         // Record type
  uint16_t      data_length;  // Variable-length data size
  uint16_t      padding;
} WalRecordHeader;

_Static_assert(sizeof(WalRecordHeader) == 32, "WalRecordHeader must be 32 bytes");

// ============================================================================
// UPDATE record payload (followed by old_data[length] + new_data[length])
// ============================================================================
typedef struct {
  page_id_t page_id;  // Target page
  uint16_t  offset;   // Offset within page
  uint16_t  length;   // Size of change
} WalUpdatePayload;

// ============================================================================
// ALLOC_PAGE / FREE_PAGE payload
// ============================================================================
typedef struct {
  page_id_t page_id;  // Allocated or freed page
} WalPagePayload;

// ============================================================================
// WAL buffer size
// ============================================================================
#define WAL_BUFFER_SIZE (64 * 1024)  // 64KB

// ============================================================================
// WAL manager
// ============================================================================
typedef struct {
  int32_t  fd;           // WAL file descriptor (-1 = closed)
  lsn_t    flushed_lsn;  // Maximum LSN flushed to disk
  lsn_t    next_lsn;     // Next LSN to assign
  uint32_t next_tx_id;   // Next transaction ID

  // WAL buffer (accumulates records before flush)
  uint8_t* buffer;       // WAL buffer (64KB via anonymous mmap)
  uint32_t buffer_size;  // Buffer capacity
  uint32_t buffer_used;  // Bytes used in buffer

  // File write offset (total bytes written to WAL file)
  int64_t file_offset;

  // Active transaction tracking
  uint32_t active_tx[64];      // Active transaction ID list
  lsn_t    active_tx_lsn[64];  // Last LSN per active transaction
  uint8_t  active_tx_count;    // Number of active transactions
} WalManager;

#endif
