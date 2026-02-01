#ifndef NOSTR_DB_TYPES_H_
#define NOSTR_DB_TYPES_H_

#include "../../util/types.h"

// ============================================================================
// Magic numbers for file identification (8 bytes each)
// ============================================================================
#define NOSTR_DB_MAGIC_EVENTS    "NOSTRDB\0"
#define NOSTR_DB_MAGIC_IDX_ID    "NSTIDID\0"
#define NOSTR_DB_MAGIC_IDX_PUBKEY "NSTIDPK\0"
#define NOSTR_DB_MAGIC_IDX_KIND  "NSTIDK\0\0"
#define NOSTR_DB_MAGIC_IDX_PK_KIND "NSTIDPKK"
#define NOSTR_DB_MAGIC_IDX_TAG   "NSTIDTAG"
#define NOSTR_DB_MAGIC_IDX_TIME  "NSTIDTIM"

#define NOSTR_DB_MAGIC_SIZE 8

// ============================================================================
// Version and size constants
// ============================================================================
#define NOSTR_DB_VERSION                    1
#define NOSTR_DB_PAGE_SIZE                  4096
#define NOSTR_DB_DEFAULT_EVENT_FILE_SIZE    (64 * 1024 * 1024)   // 64MB
#define NOSTR_DB_DEFAULT_INDEX_FILE_SIZE    (16 * 1024 * 1024)   // 16MB
#define NOSTR_DB_MAX_EVENTS                 1000000
#define NOSTR_DB_HASH_LOAD_FACTOR_PERCENT   70  // 0.7 as integer percentage

// ============================================================================
// Offset type for file positions
// ============================================================================
typedef uint64_t nostr_db_offset_t;

#define NOSTR_DB_OFFSET_NULL      ((nostr_db_offset_t)0)
#define NOSTR_DB_OFFSET_NOT_FOUND ((nostr_db_offset_t)0xFFFFFFFFFFFFFFFFULL)

// ============================================================================
// Error codes
// ============================================================================
typedef enum {
  NOSTR_DB_OK                     = 0,
  NOSTR_DB_ERROR_FILE_OPEN        = -1,
  NOSTR_DB_ERROR_FILE_CREATE      = -2,
  NOSTR_DB_ERROR_MMAP_FAILED      = -3,
  NOSTR_DB_ERROR_INVALID_MAGIC    = -4,
  NOSTR_DB_ERROR_VERSION_MISMATCH = -5,
  NOSTR_DB_ERROR_FULL             = -6,
  NOSTR_DB_ERROR_NOT_FOUND        = -7,
  NOSTR_DB_ERROR_DUPLICATE        = -8,
  NOSTR_DB_ERROR_INVALID_EVENT    = -9,
  NOSTR_DB_ERROR_INDEX_CORRUPT    = -10,
  NOSTR_DB_ERROR_NULL_PARAM       = -11,
  NOSTR_DB_ERROR_FSTAT_FAILED     = -12,
  NOSTR_DB_ERROR_FTRUNCATE_FAILED = -13,
} NostrDBError;

// ============================================================================
// Event flags
// ============================================================================
#define NOSTR_DB_EVENT_FLAG_DELETED  (1 << 0)

// ============================================================================
// Index entry states
// ============================================================================
#define NOSTR_DB_BUCKET_STATE_EMPTY     0
#define NOSTR_DB_BUCKET_STATE_USED      1
#define NOSTR_DB_BUCKET_STATE_TOMBSTONE 2

// ============================================================================
// Statistics structure
// ============================================================================
typedef struct {
  uint64_t event_count;
  uint64_t deleted_count;
  uint64_t events_file_size;
  uint64_t id_index_entries;
  uint64_t pubkey_index_entries;
  uint64_t kind_index_entries;
  uint64_t tag_index_entries;
  uint64_t timeline_index_entries;
} NostrDBStats;

#endif
