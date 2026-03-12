#ifndef NOSTR_DB_RECORD_TYPES_H_
#define NOSTR_DB_RECORD_TYPES_H_

#include "../../../util/types.h"
#include "../disk/disk_types.h"

// ============================================================================
// Record Identifier (page number + slot index)
// ============================================================================
typedef struct {
  page_id_t page_id;     // Page ID
  uint16_t  slot_index;  // Slot index within the page
} RecordId;

#define RECORD_ID_NULL \
  (RecordId)           \
  {                    \
    PAGE_ID_NULL, 0    \
  }

// ============================================================================
// Page types
// ============================================================================
typedef enum {
  PAGE_TYPE_FREE        = 0,
  PAGE_TYPE_FILE_HEADER = 1,
  PAGE_TYPE_RECORD      = 2,  // Record storage page
  PAGE_TYPE_BTREE_LEAF  = 3,  // B+ tree leaf node
  PAGE_TYPE_BTREE_INNER = 4,  // B+ tree inner node
  PAGE_TYPE_OVERFLOW    = 5,  // Overflow page (for spanned records)
} PageType;

// ============================================================================
// Slot page header (placed at page start, 24 bytes)
// ============================================================================
typedef struct {
  page_id_t page_id;           // Own page ID (4)
  page_id_t overflow_page;     // Overflow page ID for spanned records (4)
  uint16_t  slot_count;        // Number of used slots (2)
  uint16_t  free_space_start;  // Free space start offset (after slot directory) (2)
  uint16_t  free_space_end;    // Free space end offset (before record data) (2)
  uint16_t  fragmented_space;  // Space lost to fragmentation (2)
  uint8_t   page_type;         // PageType (1)
  uint8_t   flags;             // Flags (1)
  uint8_t   reserved[6];       // (6) => total 24
} SlotPageHeader;

_Static_assert(sizeof(SlotPageHeader) == 24, "SlotPageHeader must be 24 bytes");

// ============================================================================
// Slot directory entry (4 bytes)
// ============================================================================
typedef struct {
  uint16_t offset;  // Record start offset within page (0 = deleted)
  uint16_t length;  // Record length
} SlotEntry;

_Static_assert(sizeof(SlotEntry) == 4, "SlotEntry must be 4 bytes");

// ============================================================================
// Slot page layout constants
// ============================================================================
#define SLOT_PAGE_HEADER_SIZE sizeof(SlotPageHeader)
#define SLOT_ENTRY_SIZE sizeof(SlotEntry)

// Maximum usable space in a page for records
#define SLOT_PAGE_DATA_SPACE (DB_PAGE_SIZE - SLOT_PAGE_HEADER_SIZE)

// Spanned record marker: when slot length == this, record spans overflow pages
#define SPANNED_MARKER ((uint16_t)0xFFFF)

// ============================================================================
// Overflow page header (placed at start of overflow pages)
// ============================================================================
typedef struct {
  page_id_t next_page;    // Next overflow page (PAGE_ID_NULL = end)
  uint16_t  data_length;  // Bytes of record data in this overflow page
  uint16_t  reserved;
} OverflowHeader;

_Static_assert(sizeof(OverflowHeader) == 8, "OverflowHeader must be 8 bytes");

// Usable data space in an overflow page
#define OVERFLOW_DATA_SPACE (DB_PAGE_SIZE - sizeof(OverflowHeader))

// ============================================================================
// Event record header (fixed 152 bytes)
// ============================================================================
typedef struct {
  uint8_t  id[32];          // Event ID (raw bytes)
  uint8_t  pubkey[32];      // Public key (raw bytes)
  uint8_t  sig[64];         // Signature (raw bytes)
  int64_t  created_at;      // Unix timestamp
  uint32_t kind;            // Event type
  uint32_t flags;           // bit0 = deleted
  uint16_t content_length;  // Content length
  uint16_t tags_length;     // Serialized tags length
  // Followed by:
  // uint8_t content[content_length];
  // uint8_t tags[tags_length];
} EventRecord;

_Static_assert(sizeof(EventRecord) == 152, "EventRecord must be 152 bytes");

#endif
