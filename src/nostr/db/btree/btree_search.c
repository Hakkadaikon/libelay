#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// ============================================================================
// Internal: Find the leaf page containing the given key
// ============================================================================
static page_id_t find_leaf(BTree* tree, const void* key)
{
  if (tree->meta.root_page == PAGE_ID_NULL) {
    return PAGE_ID_NULL;
  }

  uint16_t  key_size  = tree->meta.key_size;
  uint16_t  max_inner = BTREE_INNER_MAX_KEYS(key_size);
  page_id_t pid       = tree->meta.root_page;

  for (uint32_t level = 0; level < tree->meta.height - 1; level++) {
    PageData* page = buffer_pool_pin(tree->pool, pid);
    if (is_null(page)) return PAGE_ID_NULL;

    const BTreeNodeHeader* node =
      (const BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);
    uint16_t pos =
      btree_node_search_key(page, key, key_size, tree->compare);
    // For inner nodes: if exact match, go to right child
    if (pos < node->key_count) {
      const void* k = btree_node_key_at(page, pos, key_size);
      if (tree->compare(k, key, key_size) == 0) {
        pos++;
      }
    }
    page_id_t child =
      btree_node_child_at(page, pos, key_size, max_inner);
    buffer_pool_unpin(tree->pool, pid);
    pid = child;
  }

  return pid;
}

// ============================================================================
// Internal: Find the leftmost leaf (for scans starting from beginning)
// ============================================================================
static page_id_t find_leftmost_leaf(BTree* tree)
{
  if (tree->meta.root_page == PAGE_ID_NULL) {
    return PAGE_ID_NULL;
  }

  uint16_t  key_size  = tree->meta.key_size;
  uint16_t  max_inner = BTREE_INNER_MAX_KEYS(key_size);
  page_id_t pid       = tree->meta.root_page;

  for (uint32_t level = 0; level < tree->meta.height - 1; level++) {
    PageData* page = buffer_pool_pin(tree->pool, pid);
    if (is_null(page)) return PAGE_ID_NULL;

    page_id_t child = btree_node_child_at(page, 0, key_size, max_inner);
    buffer_pool_unpin(tree->pool, pid);
    pid = child;
  }

  return pid;
}

// ============================================================================
// btree_search
// ============================================================================
NostrDBError btree_search(BTree* tree, const void* key, void* value_out)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);

  if (tree->meta.root_page == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);

  page_id_t leaf_pid = find_leaf(tree, key);
  if (leaf_pid == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  PageData* page = buffer_pool_pin(tree->pool, leaf_pid);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  const BTreeNodeHeader* node =
    (const BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);

  uint16_t pos = btree_node_search_key(page, key, key_size, tree->compare);

  if (pos < node->key_count) {
    const void* found_key = btree_node_key_at(page, pos, key_size);
    if (tree->compare(found_key, key, key_size) == 0) {
      if (!is_null(value_out)) {
        const void* val =
          btree_node_value_at(page, pos, key_size, value_size, max_leaf);
        internal_memcpy(value_out, val, value_size);
      }
      buffer_pool_unpin(tree->pool, leaf_pid);
      return NOSTR_DB_OK;
    }
  }

  buffer_pool_unpin(tree->pool, leaf_pid);
  return NOSTR_DB_ERROR_NOT_FOUND;
}

// ============================================================================
// btree_range_scan
// ============================================================================
NostrDBError btree_range_scan(BTree* tree, const void* min_key,
                              const void*       max_key,
                              BTreeScanCallback callback, void* user_data)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(callback, NOSTR_DB_ERROR_NULL_PARAM);

  if (tree->meta.root_page == PAGE_ID_NULL) {
    return NOSTR_DB_OK;  // Empty tree, nothing to scan
  }

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);

  // Find starting leaf
  page_id_t leaf_pid;
  if (!is_null(min_key)) {
    leaf_pid = find_leaf(tree, min_key);
  } else {
    leaf_pid = find_leftmost_leaf(tree);
  }

  while (leaf_pid != PAGE_ID_NULL) {
    PageData* page = buffer_pool_pin(tree->pool, leaf_pid);
    if (is_null(page)) break;

    const BTreeNodeHeader* node =
      (const BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);

    // Find starting position within this leaf
    uint16_t start = 0;
    if (!is_null(min_key)) {
      start = btree_node_search_key(page, min_key, key_size, tree->compare);
    }

    bool done = false;
    for (uint16_t i = start; i < node->key_count; i++) {
      const void* k = btree_node_key_at(page, i, key_size);

      // Check max_key bound
      if (!is_null(max_key)) {
        if (tree->compare(k, max_key, key_size) > 0) {
          done = true;
          break;
        }
      }

      const void* v =
        btree_node_value_at(page, i, key_size, value_size, max_leaf);
      if (!callback(k, v, user_data)) {
        done = true;
        break;
      }
    }

    page_id_t next = node->right_sibling;
    buffer_pool_unpin(tree->pool, leaf_pid);

    if (done) break;

    leaf_pid = next;
    // After first leaf, scan from position 0
    min_key = NULL;
  }

  return NOSTR_DB_OK;
}
