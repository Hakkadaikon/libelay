#ifndef NOSTR_DB_BTREE_TYPES_H_
#define NOSTR_DB_BTREE_TYPES_H_

#include "../../../util/types.h"
#include "../disk/disk_types.h"
#include "../record/record_types.h"

// ============================================================================
// Key comparison function type
// ============================================================================
typedef int32_t (*BTreeKeyCompare)(const void* a, const void* b,
                                   uint16_t key_size);

// ============================================================================
// Key type (selects comparison function)
// ============================================================================
typedef enum {
  BTREE_KEY_BYTES32   = 0,  // 32-byte fixed (ID, pubkey)
  BTREE_KEY_INT64     = 1,  // int64_t (created_at)
  BTREE_KEY_UINT32    = 2,  // uint32_t (kind)
  BTREE_KEY_COMPOSITE = 3,  // Composite key (pubkey+kind, tag_name+tag_value)
} BTreeKeyType;

// ============================================================================
// B+ tree metadata (stored in a dedicated meta page)
// ============================================================================
typedef struct {
  page_id_t root_page;    // Root node page ID (PAGE_ID_NULL if empty)
  uint32_t  height;       // Tree height (1 = leaf only)
  uint32_t  entry_count;  // Total number of entries
  uint32_t  leaf_count;   // Number of leaf nodes
  uint32_t  inner_count;  // Number of inner nodes
  uint16_t  key_size;     // Key size in bytes
  uint16_t  value_size;   // Value size in bytes
  uint8_t   key_type;     // BTreeKeyType
  uint8_t   flags;
  uint8_t   reserved[10];
} BTreeMeta;

_Static_assert(sizeof(BTreeMeta) == 36, "BTreeMeta must be 36 bytes");

// ============================================================================
// B+ tree node header (placed after SlotPageHeader in each node page)
// ============================================================================
typedef struct {
  uint16_t  key_count;  // Number of keys in this node
  uint8_t   is_leaf;    // 1 = leaf node, 0 = inner node
  uint8_t   reserved;
  page_id_t right_sibling;  // Right sibling page ID (leaf: linked list)
} BTreeNodeHeader;

_Static_assert(sizeof(BTreeNodeHeader) == 8, "BTreeNodeHeader must be 8 bytes");

// ============================================================================
// Node layout constants
// ============================================================================

// Usable space in a node page (after SlotPageHeader + BTreeNodeHeader)
#define BTREE_NODE_HEADER_OFFSET SLOT_PAGE_HEADER_SIZE
#define BTREE_DATA_OFFSET (SLOT_PAGE_HEADER_SIZE + sizeof(BTreeNodeHeader))
#define BTREE_NODE_SPACE (DB_PAGE_SIZE - BTREE_DATA_OFFSET)

// ============================================================================
// Leaf node layout (SoA):
//   [keys: key_size * N] [values: value_size * N]
//
// Inner node layout (SoA):
//   [keys: key_size * N] [children: page_id_t * (N+1)]
//   children[i] covers keys < keys[i]
//   children[N] covers keys >= keys[N-1]
// ============================================================================

// Maximum keys in a leaf node
#define BTREE_LEAF_MAX_KEYS(ks, vs) \
  ((uint16_t)(BTREE_NODE_SPACE / ((ks) + (vs))))

// Maximum keys in an inner node
// Space = key_size * N + sizeof(page_id_t) * (N+1)
// N * (key_size + 4) + 4 <= BTREE_NODE_SPACE
#define BTREE_INNER_MAX_KEYS(ks) \
  ((uint16_t)((BTREE_NODE_SPACE - sizeof(page_id_t)) / ((ks) + sizeof(page_id_t))))

// ============================================================================
// B+ tree handle
// ============================================================================
typedef struct {
  BufferPool*     pool;
  page_id_t       meta_page;  // Page ID where BTreeMeta is stored
  BTreeMeta       meta;       // Cached metadata
  BTreeKeyCompare compare;    // Key comparison function
} BTree;

// ============================================================================
// Scan callback type
// Returns true to continue scanning, false to stop
// ============================================================================
typedef bool (*BTreeScanCallback)(const void* key, const void* value,
                                  void* user_data);

// ============================================================================
// Ancestor stack for insert (tracks path from root to leaf)
// Maximum tree height is bounded by log of max pages
// ============================================================================
#define BTREE_MAX_HEIGHT 16

typedef struct {
  page_id_t page_ids[BTREE_MAX_HEIGHT];
  uint16_t  positions[BTREE_MAX_HEIGHT];  // Child index taken at each level
  uint16_t  depth;
} BTreePath;

// ============================================================================
// Duplicate key overflow page header
// Stored at the beginning of an overflow page for duplicate keys
// ============================================================================
typedef struct {
  page_id_t next_page;    // Next overflow page (PAGE_ID_NULL = end)
  uint16_t  entry_count;  // Number of RecordId entries in this page
  uint16_t  reserved;
} BTreeOverflowHeader;

_Static_assert(sizeof(BTreeOverflowHeader) == 8,
               "BTreeOverflowHeader must be 8 bytes");

// Maximum RecordId entries per overflow page
// RecordId = page_id_t(4) + uint16_t(2) = 6 bytes
#define BTREE_OVERFLOW_ENTRY_SIZE sizeof(RecordId)
#define BTREE_OVERFLOW_MAX_ENTRIES \
  ((DB_PAGE_SIZE - sizeof(BTreeOverflowHeader)) / BTREE_OVERFLOW_ENTRY_SIZE)

#endif
