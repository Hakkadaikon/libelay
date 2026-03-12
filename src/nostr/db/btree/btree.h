#ifndef NOSTR_DB_BTREE_H_
#define NOSTR_DB_BTREE_H_

#include "../../../util/types.h"
#include "../buffer/buffer_pool.h"
#include "../db_types.h"
#include "btree_types.h"

// ============================================================================
// Initialization
// ============================================================================

/**
 * @brief Create a new B+ tree (allocates meta page)
 */
NostrDBError btree_create(BTree* tree, BufferPool* pool, uint16_t key_size,
                          uint16_t value_size, BTreeKeyType key_type);

/**
 * @brief Open an existing B+ tree from its meta page
 */
NostrDBError btree_open(BTree* tree, BufferPool* pool, page_id_t meta_page);

/**
 * @brief Flush cached metadata back to disk
 */
NostrDBError btree_flush_meta(BTree* tree);

// ============================================================================
// CRUD operations (unique key)
// ============================================================================

/**
 * @brief Insert a key-value pair
 * @return NOSTR_DB_ERROR_DUPLICATE if key already exists
 */
NostrDBError btree_insert(BTree* tree, const void* key, const void* value);

/**
 * @brief Search for a key and retrieve its value
 * @return NOSTR_DB_ERROR_NOT_FOUND if key does not exist
 */
NostrDBError btree_search(BTree* tree, const void* key, void* value_out);

/**
 * @brief Delete a key
 * @return NOSTR_DB_ERROR_NOT_FOUND if key does not exist
 */
NostrDBError btree_delete(BTree* tree, const void* key);

// ============================================================================
// Range scan
// ============================================================================

/**
 * @brief Scan keys in range [min_key, max_key]
 * @param min_key Minimum key (NULL = scan from beginning)
 * @param max_key Maximum key (NULL = scan to end)
 * @param callback Called for each matching entry; return false to stop
 * @param user_data Passed to callback
 */
NostrDBError btree_range_scan(BTree* tree, const void* min_key,
                              const void*       max_key,
                              BTreeScanCallback callback, void* user_data);

// ============================================================================
// Duplicate key operations (for non-unique indexes)
// Values are stored in overflow page chains; the B+ tree value is the
// head page_id of the overflow chain.
// ============================================================================

/**
 * @brief Insert a RecordId under a (possibly duplicate) key
 */
NostrDBError btree_insert_dup(BTree* tree, const void* key, RecordId rid);

/**
 * @brief Scan all RecordIds for a given key
 */
NostrDBError btree_scan_key(BTree* tree, const void* key,
                            BTreeScanCallback callback, void* user_data);

/**
 * @brief Delete a specific RecordId from a duplicate key's overflow chain
 */
NostrDBError btree_delete_dup(BTree* tree, const void* key, RecordId rid);

// ============================================================================
// Node operations (internal, exposed for testing)
// ============================================================================

/**
 * @brief Initialize a leaf node page
 */
void btree_node_init_leaf(PageData* page, page_id_t page_id);

/**
 * @brief Initialize an inner node page
 */
void btree_node_init_inner(PageData* page, page_id_t page_id);

/**
 * @brief Binary search for a key within a node
 * @return Index where key is found or should be inserted
 */
uint16_t btree_node_search_key(const PageData* page, const void* key,
                               uint16_t key_size, BTreeKeyCompare compare);

/**
 * @brief Get pointer to key at given position in a node
 */
const void* btree_node_key_at(const PageData* page, uint16_t pos,
                              uint16_t key_size);

/**
 * @brief Get pointer to value at given position in a leaf node
 */
const void* btree_node_value_at(const PageData* page, uint16_t pos,
                                uint16_t key_size, uint16_t value_size,
                                uint16_t max_keys);

/**
 * @brief Get child page ID at given position in an inner node
 */
page_id_t btree_node_child_at(const PageData* page, uint16_t pos,
                              uint16_t key_size, uint16_t max_keys);

// ============================================================================
// Key comparison functions
// ============================================================================

int32_t btree_compare_bytes32(const void* a, const void* b, uint16_t key_size);
int32_t btree_compare_int64(const void* a, const void* b, uint16_t key_size);
int32_t btree_compare_uint32(const void* a, const void* b, uint16_t key_size);
int32_t btree_compare_composite_pk_kind(const void* a, const void* b,
                                        uint16_t key_size);
int32_t btree_compare_composite_tag(const void* a, const void* b,
                                    uint16_t key_size);

/**
 * @brief Get the comparison function for a key type
 */
BTreeKeyCompare btree_get_comparator(BTreeKeyType key_type);

#endif
