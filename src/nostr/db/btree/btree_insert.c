#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// Forward declarations of node manipulation functions from btree_node.c
extern void btree_node_insert_key_value(PageData* page, uint16_t pos,
                                        const void* key, const void* value,
                                        uint16_t key_size,
                                        uint16_t value_size,
                                        uint16_t max_keys);
extern void btree_node_insert_key_child(PageData* page, uint16_t pos,
                                        const void* key,
                                        page_id_t   right_child,
                                        uint16_t    key_size,
                                        uint16_t    max_keys);
extern void btree_node_set_child(PageData* page, uint16_t pos,
                                 page_id_t child, uint16_t key_size,
                                 uint16_t max_keys);

// ============================================================================
// Internal: Write cached metadata to meta page
// ============================================================================
NostrDBError btree_flush_meta(BTree* tree)
{
  PageData* page = buffer_pool_pin(tree->pool, tree->meta_page);
  if (is_null(page)) return NOSTR_DB_ERROR_NOT_FOUND;

  internal_memcpy(page->data, &tree->meta, sizeof(BTreeMeta));
  buffer_pool_mark_dirty(tree->pool, tree->meta_page, 0);
  buffer_pool_unpin(tree->pool, tree->meta_page);
  return NOSTR_DB_OK;
}

// ============================================================================
// btree_create: Create a new B+ tree
// ============================================================================
NostrDBError btree_create(BTree* tree, BufferPool* pool, uint16_t key_size,
                          uint16_t value_size, BTreeKeyType key_type)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require(key_size > 0, NOSTR_DB_ERROR_NULL_PARAM);
  require(value_size > 0, NOSTR_DB_ERROR_NULL_PARAM);

  // Allocate meta page
  PageData* meta_page = NULL;
  page_id_t meta_pid  = buffer_pool_alloc_page(pool, &meta_page);
  if (meta_pid == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_FULL;
  }

  tree->pool      = pool;
  tree->meta_page = meta_pid;
  tree->compare   = btree_get_comparator(key_type);

  internal_memset(&tree->meta, 0, sizeof(BTreeMeta));
  tree->meta.root_page   = PAGE_ID_NULL;
  tree->meta.height      = 0;
  tree->meta.entry_count = 0;
  tree->meta.leaf_count  = 0;
  tree->meta.inner_count = 0;
  tree->meta.key_size    = key_size;
  tree->meta.value_size  = value_size;
  tree->meta.key_type    = (uint8_t)key_type;

  internal_memcpy(meta_page->data, &tree->meta, sizeof(BTreeMeta));
  buffer_pool_mark_dirty(pool, meta_pid, 0);
  buffer_pool_unpin(pool, meta_pid);

  return NOSTR_DB_OK;
}

// ============================================================================
// btree_open: Open an existing B+ tree from its meta page
// ============================================================================
NostrDBError btree_open(BTree* tree, BufferPool* pool, page_id_t meta_page)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require(meta_page != PAGE_ID_NULL, NOSTR_DB_ERROR_NULL_PARAM);

  PageData* page = buffer_pool_pin(pool, meta_page);
  if (is_null(page)) return NOSTR_DB_ERROR_NOT_FOUND;

  internal_memcpy(&tree->meta, page->data, sizeof(BTreeMeta));
  buffer_pool_unpin(pool, meta_page);

  tree->pool      = pool;
  tree->meta_page = meta_page;
  tree->compare   = btree_get_comparator((BTreeKeyType)tree->meta.key_type);

  return NOSTR_DB_OK;
}

// ============================================================================
// Internal: Split a leaf node
// Returns the new right leaf's page_id and writes the split key
// ============================================================================
static page_id_t split_leaf(BTree* tree, PageData* left_page,
                            page_id_t left_pid, uint8_t* split_key_out)
{
  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);

  BTreeNodeHeader* left_node =
    (BTreeNodeHeader*)(left_page->data + BTREE_NODE_HEADER_OFFSET);
  uint16_t total = left_node->key_count;
  uint16_t mid   = total / 2;

  // Allocate new right leaf
  PageData* right_page = NULL;
  page_id_t right_pid  = buffer_pool_alloc_page(tree->pool, &right_page);
  if (right_pid == PAGE_ID_NULL) return PAGE_ID_NULL;

  btree_node_init_leaf(right_page, right_pid);
  BTreeNodeHeader* right_node =
    (BTreeNodeHeader*)(right_page->data + BTREE_NODE_HEADER_OFFSET);

  // Copy upper half of keys and values to right node
  uint8_t* left_keys  = left_page->data + BTREE_DATA_OFFSET;
  uint8_t* left_vals  = left_keys + (uint32_t)max_leaf * key_size;
  uint8_t* right_keys = right_page->data + BTREE_DATA_OFFSET;
  uint8_t* right_vals = right_keys + (uint32_t)max_leaf * key_size;
  uint16_t move_count = total - mid;

  internal_memcpy(right_keys, left_keys + (uint32_t)mid * key_size,
                  (uint32_t)move_count * key_size);
  internal_memcpy(right_vals, left_vals + (uint32_t)mid * value_size,
                  (uint32_t)move_count * value_size);

  right_node->key_count = move_count;

  // Link right sibling
  right_node->right_sibling = left_node->right_sibling;
  left_node->right_sibling  = right_pid;
  left_node->key_count      = mid;

  // Split key = first key of right node
  internal_memcpy(split_key_out, right_keys, key_size);

  buffer_pool_mark_dirty(tree->pool, left_pid, 0);
  buffer_pool_mark_dirty(tree->pool, right_pid, 0);
  buffer_pool_unpin(tree->pool, right_pid);

  tree->meta.leaf_count++;
  return right_pid;
}

// ============================================================================
// Internal: Split an inner node
// Returns the new right inner node's page_id and writes the pushed-up key
// ============================================================================
static page_id_t split_inner(BTree* tree, PageData* left_page,
                             page_id_t left_pid, uint8_t* split_key_out)
{
  uint16_t key_size  = tree->meta.key_size;
  uint16_t max_inner = BTREE_INNER_MAX_KEYS(key_size);

  BTreeNodeHeader* left_node =
    (BTreeNodeHeader*)(left_page->data + BTREE_NODE_HEADER_OFFSET);
  uint16_t total = left_node->key_count;
  uint16_t mid   = total / 2;

  // Allocate new right inner node
  PageData* right_page = NULL;
  page_id_t right_pid  = buffer_pool_alloc_page(tree->pool, &right_page);
  if (right_pid == PAGE_ID_NULL) return PAGE_ID_NULL;

  btree_node_init_inner(right_page, right_pid);
  BTreeNodeHeader* right_node =
    (BTreeNodeHeader*)(right_page->data + BTREE_NODE_HEADER_OFFSET);

  uint8_t* left_keys      = left_page->data + BTREE_DATA_OFFSET;
  uint8_t* left_children  = left_keys + (uint32_t)max_inner * key_size;
  uint8_t* right_keys     = right_page->data + BTREE_DATA_OFFSET;
  uint8_t* right_children = right_keys + (uint32_t)max_inner * key_size;

  // The middle key is pushed up to the parent
  internal_memcpy(split_key_out, left_keys + (uint32_t)mid * key_size,
                  key_size);

  // Copy keys after mid to right node (skip the mid key itself)
  uint16_t move_keys = total - mid - 1;
  if (move_keys > 0) {
    internal_memcpy(right_keys,
                    left_keys + (uint32_t)(mid + 1) * key_size,
                    (uint32_t)move_keys * key_size);
  }

  // Copy children from mid+1 to right node
  uint16_t move_children = move_keys + 1;
  internal_memcpy(right_children,
                  left_children + (uint32_t)(mid + 1) * sizeof(page_id_t),
                  (uint32_t)move_children * sizeof(page_id_t));

  right_node->key_count = move_keys;
  left_node->key_count  = mid;

  buffer_pool_mark_dirty(tree->pool, left_pid, 0);
  buffer_pool_mark_dirty(tree->pool, right_pid, 0);
  buffer_pool_unpin(tree->pool, right_pid);

  tree->meta.inner_count++;
  return right_pid;
}

// ============================================================================
// btree_insert
// ============================================================================
NostrDBError btree_insert(BTree* tree, const void* key, const void* value)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(value, NOSTR_DB_ERROR_NULL_PARAM);

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);
  uint16_t max_inner  = BTREE_INNER_MAX_KEYS(key_size);

  // Empty tree: create first leaf as root
  if (tree->meta.root_page == PAGE_ID_NULL) {
    PageData* page = NULL;
    page_id_t pid  = buffer_pool_alloc_page(tree->pool, &page);
    if (pid == PAGE_ID_NULL) return NOSTR_DB_ERROR_FULL;

    btree_node_init_leaf(page, pid);
    btree_node_insert_key_value(page, 0, key, value, key_size, value_size,
                                max_leaf);
    buffer_pool_mark_dirty(tree->pool, pid, 0);
    buffer_pool_unpin(tree->pool, pid);

    tree->meta.root_page   = pid;
    tree->meta.height      = 1;
    tree->meta.entry_count = 1;
    tree->meta.leaf_count  = 1;
    return btree_flush_meta(tree);
  }

  // Traverse from root to leaf, recording path
  BTreePath path;
  path.depth    = 0;
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

    path.page_ids[path.depth]  = pid;
    path.positions[path.depth] = pos;
    path.depth++;

    page_id_t child = btree_node_child_at(page, pos, key_size, max_inner);
    buffer_pool_unpin(tree->pool, pid);
    pid = child;
  }

  // Pin the leaf
  PageData* leaf = buffer_pool_pin(tree->pool, pid);
  if (is_null(leaf)) return NOSTR_DB_ERROR_NOT_FOUND;

  BTreeNodeHeader* leaf_node =
    (BTreeNodeHeader*)(leaf->data + BTREE_NODE_HEADER_OFFSET);

  uint16_t pos = btree_node_search_key(leaf, key, key_size, tree->compare);

  // Check for duplicate
  if (pos < leaf_node->key_count) {
    const void* existing = btree_node_key_at(leaf, pos, key_size);
    if (tree->compare(existing, key, key_size) == 0) {
      buffer_pool_unpin(tree->pool, pid);
      return NOSTR_DB_ERROR_DUPLICATE;
    }
  }

  // Insert into leaf if there's space
  if (leaf_node->key_count < max_leaf) {
    btree_node_insert_key_value(leaf, pos, key, value, key_size, value_size,
                                max_leaf);
    buffer_pool_mark_dirty(tree->pool, pid, 0);
    buffer_pool_unpin(tree->pool, pid);
    tree->meta.entry_count++;
    return btree_flush_meta(tree);
  }

  // Leaf is full: need to split
  // First insert into the leaf (temporarily over capacity in a temp buffer)
  // Strategy: split first, then insert into the correct half
  uint8_t split_key[256];  // Max key size

  page_id_t right_pid = split_leaf(tree, leaf, pid, split_key);
  if (right_pid == PAGE_ID_NULL) {
    buffer_pool_unpin(tree->pool, pid);
    return NOSTR_DB_ERROR_FULL;
  }

  // Determine which side gets the new entry
  int32_t cmp = tree->compare(key, split_key, key_size);
  if (cmp < 0) {
    // Insert into left leaf (still pinned)
    pos = btree_node_search_key(leaf, key, key_size, tree->compare);
    btree_node_insert_key_value(leaf, pos, key, value, key_size, value_size,
                                max_leaf);
    buffer_pool_mark_dirty(tree->pool, pid, 0);
  } else {
    // Insert into right leaf
    PageData* right_page = buffer_pool_pin(tree->pool, right_pid);
    if (is_null(right_page)) {
      buffer_pool_unpin(tree->pool, pid);
      return NOSTR_DB_ERROR_NOT_FOUND;
    }
    pos = btree_node_search_key(right_page, key, key_size, tree->compare);
    btree_node_insert_key_value(right_page, pos, key, value, key_size,
                                value_size, max_leaf);
    buffer_pool_mark_dirty(tree->pool, right_pid, 0);
    buffer_pool_unpin(tree->pool, right_pid);
  }
  buffer_pool_unpin(tree->pool, pid);

  // Propagate split up the tree
  page_id_t new_child = right_pid;
  uint8_t   promote_key[256];
  internal_memcpy(promote_key, split_key, key_size);

  while (path.depth > 0) {
    path.depth--;
    page_id_t parent_pid = path.page_ids[path.depth];

    PageData* parent = buffer_pool_pin(tree->pool, parent_pid);
    if (is_null(parent)) return NOSTR_DB_ERROR_NOT_FOUND;

    BTreeNodeHeader* parent_node =
      (BTreeNodeHeader*)(parent->data + BTREE_NODE_HEADER_OFFSET);

    if (parent_node->key_count < max_inner) {
      // Parent has space
      uint16_t insert_pos =
        btree_node_search_key(parent, promote_key, key_size, tree->compare);
      btree_node_insert_key_child(parent, insert_pos, promote_key,
                                  new_child, key_size, max_inner);
      buffer_pool_mark_dirty(tree->pool, parent_pid, 0);
      buffer_pool_unpin(tree->pool, parent_pid);
      tree->meta.entry_count++;
      return btree_flush_meta(tree);
    }

    // Parent is full: split inner node
    // First insert, then split
    uint16_t insert_pos =
      btree_node_search_key(parent, promote_key, key_size, tree->compare);
    btree_node_insert_key_child(parent, insert_pos, promote_key, new_child,
                                key_size, max_inner);
    buffer_pool_mark_dirty(tree->pool, parent_pid, 0);

    uint8_t   new_promote[256];
    page_id_t new_right =
      split_inner(tree, parent, parent_pid, new_promote);
    buffer_pool_unpin(tree->pool, parent_pid);

    if (new_right == PAGE_ID_NULL) return NOSTR_DB_ERROR_FULL;

    internal_memcpy(promote_key, new_promote, key_size);
    new_child = new_right;
  }

  // Root was split: create new root
  PageData* new_root_page = NULL;
  page_id_t new_root_pid  = buffer_pool_alloc_page(tree->pool, &new_root_page);
  if (new_root_pid == PAGE_ID_NULL) return NOSTR_DB_ERROR_FULL;

  btree_node_init_inner(new_root_page, new_root_pid);
  BTreeNodeHeader* new_root_node =
    (BTreeNodeHeader*)(new_root_page->data + BTREE_NODE_HEADER_OFFSET);

  // Set up: [old_root] [promote_key] [new_child]
  uint8_t* root_keys     = new_root_page->data + BTREE_DATA_OFFSET;
  uint8_t* root_children = root_keys + (uint32_t)max_inner * key_size;

  internal_memcpy(root_keys, promote_key, key_size);
  page_id_t old_root = tree->meta.root_page;
  internal_memcpy(root_children, &old_root, sizeof(page_id_t));
  internal_memcpy(root_children + sizeof(page_id_t), &new_child,
                  sizeof(page_id_t));
  new_root_node->key_count = 1;

  buffer_pool_mark_dirty(tree->pool, new_root_pid, 0);
  buffer_pool_unpin(tree->pool, new_root_pid);

  tree->meta.root_page = new_root_pid;
  tree->meta.height++;
  tree->meta.inner_count++;
  tree->meta.entry_count++;

  return btree_flush_meta(tree);
}
