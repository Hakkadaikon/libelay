#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// ============================================================================
// Internal: Get pointer to the BTreeNodeHeader within a page
// ============================================================================
static inline BTreeNodeHeader* get_node_header(PageData* page)
{
  return (BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);
}

static inline const BTreeNodeHeader* get_node_header_const(
  const PageData* page)
{
  return (const BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);
}

// ============================================================================
// Internal: Get pointer to the key array start
// ============================================================================
static inline uint8_t* get_keys_ptr(PageData* page)
{
  return page->data + BTREE_DATA_OFFSET;
}

static inline const uint8_t* get_keys_ptr_const(const PageData* page)
{
  return page->data + BTREE_DATA_OFFSET;
}

// ============================================================================
// btree_node_init_leaf
// ============================================================================
void btree_node_init_leaf(PageData* page, page_id_t page_id)
{
  if (is_null(page)) return;

  internal_memset(page->data, 0, DB_PAGE_SIZE);

  SlotPageHeader* hdr = (SlotPageHeader*)page->data;
  hdr->page_id        = page_id;
  hdr->page_type      = PAGE_TYPE_BTREE_LEAF;

  BTreeNodeHeader* node = get_node_header(page);
  node->key_count       = 0;
  node->is_leaf         = 1;
  node->right_sibling   = PAGE_ID_NULL;
}

// ============================================================================
// btree_node_init_inner
// ============================================================================
void btree_node_init_inner(PageData* page, page_id_t page_id)
{
  if (is_null(page)) return;

  internal_memset(page->data, 0, DB_PAGE_SIZE);

  SlotPageHeader* hdr = (SlotPageHeader*)page->data;
  hdr->page_id        = page_id;
  hdr->page_type      = PAGE_TYPE_BTREE_INNER;

  BTreeNodeHeader* node = get_node_header(page);
  node->key_count       = 0;
  node->is_leaf         = 0;
  node->right_sibling   = PAGE_ID_NULL;
}

// ============================================================================
// btree_node_key_at: Get pointer to key at position pos
// Layout: keys start at BTREE_DATA_OFFSET, each key_size bytes
// ============================================================================
const void* btree_node_key_at(const PageData* page, uint16_t pos,
                              uint16_t key_size)
{
  return get_keys_ptr_const(page) + (uint32_t)pos * key_size;
}

// ============================================================================
// btree_node_value_at: Get pointer to value at position pos (leaf only)
// Layout: [keys: key_size * max_keys] [values: value_size * max_keys]
// ============================================================================
const void* btree_node_value_at(const PageData* page, uint16_t pos,
                                uint16_t key_size, uint16_t value_size,
                                uint16_t max_keys)
{
  (void)value_size;
  const uint8_t* base = get_keys_ptr_const(page);
  return base + (uint32_t)max_keys * key_size + (uint32_t)pos * value_size;
}

// ============================================================================
// btree_node_child_at: Get child page ID at position pos (inner only)
// Layout: [keys: key_size * max_keys] [children: page_id_t * (max_keys+1)]
// ============================================================================
page_id_t btree_node_child_at(const PageData* page, uint16_t pos,
                              uint16_t key_size, uint16_t max_keys)
{
  const uint8_t* base           = get_keys_ptr_const(page);
  const uint8_t* children_start = base + (uint32_t)max_keys * key_size;
  page_id_t      child;
  internal_memcpy(&child, children_start + (uint32_t)pos * sizeof(page_id_t),
                  sizeof(page_id_t));
  return child;
}

// ============================================================================
// btree_node_search_key: Binary search for key within a node
// Returns the index where key is found or should be inserted.
// For inner nodes: result is the child index to follow.
// For leaf nodes: result is the position of exact match or insertion point.
// ============================================================================
uint16_t btree_node_search_key(const PageData* page, const void* key,
                               uint16_t key_size, BTreeKeyCompare compare)
{
  const BTreeNodeHeader* node = get_node_header_const(page);
  const uint8_t*         keys = get_keys_ptr_const(page);
  uint16_t               lo   = 0;
  uint16_t               hi   = node->key_count;

  while (lo < hi) {
    uint16_t mid = lo + (hi - lo) / 2;
    int32_t  cmp = compare(keys + (uint32_t)mid * key_size, key, key_size);
    if (cmp < 0) {
      lo = mid + 1;
    } else {
      hi = mid;
    }
  }

  return lo;
}

// ============================================================================
// btree_node_insert_key_value: Insert key+value at position in a leaf
// Shifts existing entries to make room
// ============================================================================
void btree_node_insert_key_value(PageData* page, uint16_t pos,
                                 const void* key, const void* value,
                                 uint16_t key_size, uint16_t value_size,
                                 uint16_t max_keys)
{
  BTreeNodeHeader* node = get_node_header(page);
  uint8_t*         keys = get_keys_ptr(page);
  uint8_t*         vals = keys + (uint32_t)max_keys * key_size;
  uint16_t         n    = node->key_count;

  // Shift keys right
  if (pos < n) {
    uint32_t shift_bytes = (uint32_t)(n - pos) * key_size;
    // Move from end to avoid overlap
    for (int32_t i = (int32_t)shift_bytes - 1; i >= 0; i--) {
      keys[(uint32_t)pos * key_size + (uint32_t)i + key_size] =
        keys[(uint32_t)pos * key_size + (uint32_t)i];
    }
  }
  internal_memcpy(keys + (uint32_t)pos * key_size, key, key_size);

  // Shift values right
  if (pos < n) {
    uint32_t shift_bytes = (uint32_t)(n - pos) * value_size;
    for (int32_t i = (int32_t)shift_bytes - 1; i >= 0; i--) {
      vals[(uint32_t)pos * value_size + (uint32_t)i + value_size] =
        vals[(uint32_t)pos * value_size + (uint32_t)i];
    }
  }
  internal_memcpy(vals + (uint32_t)pos * value_size, value, value_size);

  node->key_count++;
}

// ============================================================================
// btree_node_insert_key_child: Insert key+right_child at position in inner node
// children[pos+1] = right_child (shifted appropriately)
// ============================================================================
void btree_node_insert_key_child(PageData* page, uint16_t pos,
                                 const void* key, page_id_t right_child,
                                 uint16_t key_size, uint16_t max_keys)
{
  BTreeNodeHeader* node     = get_node_header(page);
  uint8_t*         keys     = get_keys_ptr(page);
  uint8_t*         children = keys + (uint32_t)max_keys * key_size;
  uint16_t         n        = node->key_count;

  // Shift keys right
  if (pos < n) {
    uint32_t shift_bytes = (uint32_t)(n - pos) * key_size;
    for (int32_t i = (int32_t)shift_bytes - 1; i >= 0; i--) {
      keys[(uint32_t)pos * key_size + (uint32_t)i + key_size] =
        keys[(uint32_t)pos * key_size + (uint32_t)i];
    }
  }
  internal_memcpy(keys + (uint32_t)pos * key_size, key, key_size);

  // Shift children right (from pos+1 onward)
  uint16_t child_pos = pos + 1;
  if (child_pos <= n) {
    uint32_t shift_bytes = (uint32_t)(n - pos) * sizeof(page_id_t);
    for (int32_t i = (int32_t)shift_bytes - 1; i >= 0; i--) {
      children[(uint32_t)child_pos * sizeof(page_id_t) + (uint32_t)i +
               sizeof(page_id_t)] =
        children[(uint32_t)child_pos * sizeof(page_id_t) + (uint32_t)i];
    }
  }
  internal_memcpy(children + (uint32_t)child_pos * sizeof(page_id_t),
                  &right_child, sizeof(page_id_t));

  node->key_count++;
}

// ============================================================================
// btree_node_delete_key_value: Delete key+value at position in a leaf
// ============================================================================
void btree_node_delete_key_value(PageData* page, uint16_t pos,
                                 uint16_t key_size, uint16_t value_size,
                                 uint16_t max_keys)
{
  BTreeNodeHeader* node = get_node_header(page);
  uint8_t*         keys = get_keys_ptr(page);
  uint8_t*         vals = keys + (uint32_t)max_keys * key_size;
  uint16_t         n    = node->key_count;

  // Shift keys left
  if (pos + 1 < n) {
    uint32_t shift = (uint32_t)(n - pos - 1) * key_size;
    internal_memcpy(keys + (uint32_t)pos * key_size,
                    keys + (uint32_t)(pos + 1) * key_size, shift);
  }

  // Shift values left
  if (pos + 1 < n) {
    uint32_t shift = (uint32_t)(n - pos - 1) * value_size;
    internal_memcpy(vals + (uint32_t)pos * value_size,
                    vals + (uint32_t)(pos + 1) * value_size, shift);
  }

  node->key_count--;
}

// ============================================================================
// btree_node_set_child: Set child page ID at position (inner node)
// ============================================================================
void btree_node_set_child(PageData* page, uint16_t pos, page_id_t child,
                          uint16_t key_size, uint16_t max_keys)
{
  uint8_t* keys     = get_keys_ptr(page);
  uint8_t* children = keys + (uint32_t)max_keys * key_size;
  internal_memcpy(children + (uint32_t)pos * sizeof(page_id_t), &child,
                  sizeof(page_id_t));
}

// ============================================================================
// btree_node_set_value: Set value at position (leaf node)
// ============================================================================
void btree_node_set_value(PageData* page, uint16_t pos, const void* value,
                          uint16_t key_size, uint16_t value_size,
                          uint16_t max_keys)
{
  uint8_t* keys = get_keys_ptr(page);
  uint8_t* vals = keys + (uint32_t)max_keys * key_size;
  internal_memcpy(vals + (uint32_t)pos * value_size, value, value_size);
}
