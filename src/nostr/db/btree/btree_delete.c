#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// Forward declaration from btree_node.c
extern void btree_node_delete_key_value(PageData* page, uint16_t pos,
                                        uint16_t key_size,
                                        uint16_t value_size,
                                        uint16_t max_keys);

// ============================================================================
// btree_delete: Delete a key from the B+ tree (tombstone approach)
// No merge/redistribute — deleted entries are simply removed from the leaf.
// Empty leaves are left in place (cleaned up during rebuild).
// ============================================================================
NostrDBError btree_delete(BTree* tree, const void* key)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);

  if (tree->meta.root_page == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);
  uint16_t max_inner  = BTREE_INNER_MAX_KEYS(key_size);

  // Traverse to the leaf
  page_id_t pid = tree->meta.root_page;
  for (uint32_t level = 0; level < tree->meta.height - 1; level++) {
    PageData* page = buffer_pool_pin(tree->pool, pid);
    if (is_null(page)) return NOSTR_DB_ERROR_NOT_FOUND;

    const BTreeNodeHeader* inner_node =
      (const BTreeNodeHeader*)(page->data + BTREE_NODE_HEADER_OFFSET);
    uint16_t pos =
      btree_node_search_key(page, key, key_size, tree->compare);
    // For inner nodes: if exact match, go to right child
    if (pos < inner_node->key_count) {
      const void* k = btree_node_key_at(page, pos, key_size);
      if (tree->compare(k, key, key_size) == 0) {
        pos++;
      }
    }
    page_id_t child = btree_node_child_at(page, pos, key_size, max_inner);
    buffer_pool_unpin(tree->pool, pid);
    pid = child;
  }

  // Pin the leaf
  PageData* leaf = buffer_pool_pin(tree->pool, pid);
  if (is_null(leaf)) return NOSTR_DB_ERROR_NOT_FOUND;

  const BTreeNodeHeader* node =
    (const BTreeNodeHeader*)(leaf->data + BTREE_NODE_HEADER_OFFSET);

  uint16_t pos = btree_node_search_key(leaf, key, key_size, tree->compare);

  // Check exact match
  if (pos >= node->key_count) {
    buffer_pool_unpin(tree->pool, pid);
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  const void* found_key = btree_node_key_at(leaf, pos, key_size);
  if (tree->compare(found_key, key, key_size) != 0) {
    buffer_pool_unpin(tree->pool, pid);
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Delete the entry
  btree_node_delete_key_value(leaf, pos, key_size, value_size, max_leaf);
  buffer_pool_mark_dirty(tree->pool, pid, 0);
  buffer_pool_unpin(tree->pool, pid);

  tree->meta.entry_count--;
  return btree_flush_meta(tree);
}
