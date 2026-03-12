#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "btree.h"

// Forward declaration from btree_node.c
extern void btree_node_set_value(PageData* page, uint16_t pos,
                                 const void* value, uint16_t key_size,
                                 uint16_t value_size, uint16_t max_keys);

// ============================================================================
// Internal: Get RecordId entry pointer within an overflow page
// ============================================================================
static inline RecordId* get_overflow_entry(PageData* page, uint16_t index)
{
  uint8_t* base = page->data + sizeof(BTreeOverflowHeader);
  return (RecordId*)(base + (uint32_t)index * BTREE_OVERFLOW_ENTRY_SIZE);
}

static inline const RecordId* get_overflow_entry_const(const PageData* page,
                                                       uint16_t        index)
{
  const uint8_t* base = page->data + sizeof(BTreeOverflowHeader);
  return (const RecordId*)(base +
                           (uint32_t)index * BTREE_OVERFLOW_ENTRY_SIZE);
}

// ============================================================================
// Internal: Initialize an overflow page
// ============================================================================
static void init_overflow_page(PageData* page)
{
  internal_memset(page->data, 0, DB_PAGE_SIZE);
  BTreeOverflowHeader* hdr = (BTreeOverflowHeader*)page->data;
  hdr->next_page           = PAGE_ID_NULL;
  hdr->entry_count         = 0;
}

// ============================================================================
// Internal: Compare two RecordIds for equality
// ============================================================================
static inline bool record_id_equal(RecordId a, RecordId b)
{
  return a.page_id == b.page_id && a.slot_index == b.slot_index;
}

// ============================================================================
// btree_insert_dup: Insert a RecordId under a (possibly duplicate) key
//
// The B+ tree stores page_id_t as the value: the head of an overflow chain.
// If the key doesn't exist yet, create a new overflow page and insert the key.
// If it exists, append the RecordId to the overflow chain.
// ============================================================================
NostrDBError btree_insert_dup(BTree* tree, const void* key, RecordId rid)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);

  // Try to find existing key
  page_id_t    chain_head = PAGE_ID_NULL;
  NostrDBError err        = btree_search(tree, key, &chain_head);

  if (err == NOSTR_DB_OK && chain_head != PAGE_ID_NULL) {
    // Key exists: find a page in the chain with space
    page_id_t pid = chain_head;
    while (pid != PAGE_ID_NULL) {
      PageData* page = buffer_pool_pin(tree->pool, pid);
      if (is_null(page)) return NOSTR_DB_ERROR_NOT_FOUND;

      BTreeOverflowHeader* hdr = (BTreeOverflowHeader*)page->data;

      if (hdr->entry_count < (uint16_t)BTREE_OVERFLOW_MAX_ENTRIES) {
        // Space available: append here
        RecordId* entry   = get_overflow_entry(page, hdr->entry_count);
        entry->page_id    = rid.page_id;
        entry->slot_index = rid.slot_index;
        hdr->entry_count++;
        buffer_pool_mark_dirty(tree->pool, pid, 0);
        buffer_pool_unpin(tree->pool, pid);
        return NOSTR_DB_OK;
      }

      page_id_t next = hdr->next_page;
      if (next == PAGE_ID_NULL) {
        // Need a new overflow page at end of chain
        PageData* new_page = NULL;
        page_id_t new_pid  = buffer_pool_alloc_page(tree->pool, &new_page);
        if (new_pid == PAGE_ID_NULL) {
          buffer_pool_unpin(tree->pool, pid);
          return NOSTR_DB_ERROR_FULL;
        }

        init_overflow_page(new_page);
        RecordId* entry                                     = get_overflow_entry(new_page, 0);
        entry->page_id                                      = rid.page_id;
        entry->slot_index                                   = rid.slot_index;
        ((BTreeOverflowHeader*)new_page->data)->entry_count = 1;

        hdr->next_page = new_pid;
        buffer_pool_mark_dirty(tree->pool, pid, 0);
        buffer_pool_mark_dirty(tree->pool, new_pid, 0);
        buffer_pool_unpin(tree->pool, new_pid);
        buffer_pool_unpin(tree->pool, pid);
        return NOSTR_DB_OK;
      }

      buffer_pool_unpin(tree->pool, pid);
      pid = next;
    }
    return NOSTR_DB_ERROR_FULL;
  }

  // Key doesn't exist: create new overflow page and insert key into B+ tree
  PageData* ov_page = NULL;
  page_id_t ov_pid  = buffer_pool_alloc_page(tree->pool, &ov_page);
  if (ov_pid == PAGE_ID_NULL) return NOSTR_DB_ERROR_FULL;

  init_overflow_page(ov_page);
  RecordId* entry                                    = get_overflow_entry(ov_page, 0);
  entry->page_id                                     = rid.page_id;
  entry->slot_index                                  = rid.slot_index;
  ((BTreeOverflowHeader*)ov_page->data)->entry_count = 1;
  buffer_pool_mark_dirty(tree->pool, ov_pid, 0);
  buffer_pool_unpin(tree->pool, ov_pid);

  // Insert key -> overflow_page_id into B+ tree
  return btree_insert(tree, key, &ov_pid);
}

// ============================================================================
// btree_scan_key: Scan all RecordIds for a given key
// ============================================================================
NostrDBError btree_scan_key(BTree* tree, const void* key,
                            BTreeScanCallback callback, void* user_data)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(callback, NOSTR_DB_ERROR_NULL_PARAM);

  page_id_t    chain_head = PAGE_ID_NULL;
  NostrDBError err        = btree_search(tree, key, &chain_head);
  if (err != NOSTR_DB_OK) return err;

  if (chain_head == PAGE_ID_NULL) return NOSTR_DB_OK;

  page_id_t pid = chain_head;
  while (pid != PAGE_ID_NULL) {
    PageData* page = buffer_pool_pin(tree->pool, pid);
    if (is_null(page)) break;

    const BTreeOverflowHeader* hdr =
      (const BTreeOverflowHeader*)page->data;

    bool stop = false;
    for (uint16_t i = 0; i < hdr->entry_count; i++) {
      const RecordId* entry = get_overflow_entry_const(page, i);
      if (!callback(key, entry, user_data)) {
        stop = true;
        break;
      }
    }

    page_id_t next = hdr->next_page;
    buffer_pool_unpin(tree->pool, pid);

    if (stop) break;
    pid = next;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// btree_delete_dup: Delete a specific RecordId from a key's overflow chain
// ============================================================================
NostrDBError btree_delete_dup(BTree* tree, const void* key, RecordId rid)
{
  require_not_null(tree, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(key, NOSTR_DB_ERROR_NULL_PARAM);

  uint16_t key_size   = tree->meta.key_size;
  uint16_t value_size = tree->meta.value_size;
  uint16_t max_leaf   = BTREE_LEAF_MAX_KEYS(key_size, value_size);

  page_id_t    chain_head = PAGE_ID_NULL;
  NostrDBError err        = btree_search(tree, key, &chain_head);
  if (err != NOSTR_DB_OK) return err;

  if (chain_head == PAGE_ID_NULL) return NOSTR_DB_ERROR_NOT_FOUND;

  page_id_t prev_pid = PAGE_ID_NULL;
  page_id_t pid      = chain_head;

  while (pid != PAGE_ID_NULL) {
    PageData* page = buffer_pool_pin(tree->pool, pid);
    if (is_null(page)) break;

    BTreeOverflowHeader* hdr = (BTreeOverflowHeader*)page->data;

    for (uint16_t i = 0; i < hdr->entry_count; i++) {
      RecordId* entry = get_overflow_entry(page, i);
      if (record_id_equal(*entry, rid)) {
        // Found: remove by swapping with last entry
        if (i < hdr->entry_count - 1) {
          RecordId* last    = get_overflow_entry(page, hdr->entry_count - 1);
          entry->page_id    = last->page_id;
          entry->slot_index = last->slot_index;
        }
        hdr->entry_count--;
        buffer_pool_mark_dirty(tree->pool, pid, 0);

        // If page is now empty and it's not the only page
        if (hdr->entry_count == 0) {
          if (pid == chain_head && hdr->next_page == PAGE_ID_NULL) {
            // Last page in chain: delete the key from the B+ tree
            buffer_pool_unpin(tree->pool, pid);
            disk_free_page(tree->pool->disk, pid);
            return btree_delete(tree, key);
          } else if (pid == chain_head) {
            // Head page empty but chain continues: update B+ tree value
            page_id_t new_head = hdr->next_page;
            buffer_pool_unpin(tree->pool, pid);
            disk_free_page(tree->pool->disk, pid);

            // Find and update the value in the leaf
            // Re-traverse to find the leaf
            page_id_t leaf_pid  = tree->meta.root_page;
            uint16_t  max_inner = BTREE_INNER_MAX_KEYS(key_size);
            for (uint32_t level = 0; level < tree->meta.height - 1;
                 level++) {
              PageData* lp = buffer_pool_pin(tree->pool, leaf_pid);
              if (is_null(lp)) return NOSTR_DB_ERROR_NOT_FOUND;
              const BTreeNodeHeader* inn =
                (const BTreeNodeHeader*)(lp->data +
                                         BTREE_NODE_HEADER_OFFSET);
              uint16_t p = btree_node_search_key(lp, key, key_size,
                                                 tree->compare);
              if (p < inn->key_count) {
                const void* ik = btree_node_key_at(lp, p, key_size);
                if (tree->compare(ik, key, key_size) == 0) p++;
              }
              page_id_t ch =
                btree_node_child_at(lp, p, key_size, max_inner);
              buffer_pool_unpin(tree->pool, leaf_pid);
              leaf_pid = ch;
            }
            PageData* leaf = buffer_pool_pin(tree->pool, leaf_pid);
            if (is_null(leaf)) return NOSTR_DB_ERROR_NOT_FOUND;
            uint16_t lpos =
              btree_node_search_key(leaf, key, key_size, tree->compare);
            btree_node_set_value(leaf, lpos, &new_head, key_size,
                                 value_size, max_leaf);
            buffer_pool_mark_dirty(tree->pool, leaf_pid, 0);
            buffer_pool_unpin(tree->pool, leaf_pid);
            return NOSTR_DB_OK;
          } else {
            // Middle/tail page empty: unlink from prev
            if (prev_pid != PAGE_ID_NULL) {
              PageData* prev = buffer_pool_pin(tree->pool, prev_pid);
              if (!is_null(prev)) {
                BTreeOverflowHeader* prev_hdr =
                  (BTreeOverflowHeader*)prev->data;
                prev_hdr->next_page = hdr->next_page;
                buffer_pool_mark_dirty(tree->pool, prev_pid, 0);
                buffer_pool_unpin(tree->pool, prev_pid);
              }
            }
            buffer_pool_unpin(tree->pool, pid);
            disk_free_page(tree->pool->disk, pid);
            return NOSTR_DB_OK;
          }
        }

        buffer_pool_unpin(tree->pool, pid);
        return NOSTR_DB_OK;
      }
    }

    page_id_t next = hdr->next_page;
    buffer_pool_unpin(tree->pool, pid);
    prev_pid = pid;
    pid      = next;
  }

  return NOSTR_DB_ERROR_NOT_FOUND;
}
