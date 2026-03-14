#include "index_manager.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"

// ============================================================================
// Internal: Convert a hex character to its numeric value
// ============================================================================
static int32_t hex_val(uint8_t c)
{
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// ============================================================================
// Internal: Try to convert a hex string to 32 raw bytes
// Returns true on success
// ============================================================================
static bool hex_to_32bytes(const uint8_t* hex, size_t hex_len, uint8_t out[32])
{
  if (hex_len != 64) return false;

  for (size_t i = 0; i < 32; i++) {
    int32_t h = hex_val(hex[i * 2]);
    int32_t l = hex_val(hex[i * 2 + 1]);
    if (h < 0 || l < 0) return false;
    out[i] = (uint8_t)((h << 4) | l);
  }
  return true;
}

// ============================================================================
// Internal: Parse serialized tags and insert/delete each indexable tag
//
// Tag serialization format (from db_tags.c):
//   [tag_count: uint16_t]
//   For each tag:
//     [value_count: uint8_t][name_len: uint8_t][name: bytes]
//     For each value:
//       [value_len: uint16_t][value: bytes]
//
// Indexable tags: single-character name with first value being 64-char hex
// ============================================================================
typedef NostrDBError (*TagIndexOp)(BTree* tree, uint8_t tag_name,
                                   const uint8_t tag_value[32], RecordId rid);

static NostrDBError process_tags(BTree* tree, const uint8_t* tags_data,
                                 uint16_t tags_length, RecordId rid,
                                 TagIndexOp op)
{
  if (is_null(tags_data) || tags_length < 2) return NOSTR_DB_OK;

  const uint8_t* ptr = tags_data;
  const uint8_t* end = tags_data + tags_length;

  uint16_t tag_count = (uint16_t)(ptr[0] | (ptr[1] << 8));
  ptr += 2;

  for (uint16_t i = 0; i < tag_count && ptr + 2 <= end; i++) {
    uint8_t value_count = *ptr++;
    uint8_t name_len    = *ptr++;

    if (ptr + name_len > end) break;

    // Only index single-character tag names
    bool    indexable = (name_len == 1);
    uint8_t tag_name  = indexable ? ptr[0] : 0;
    ptr += name_len;

    // Process each value
    for (uint8_t j = 0; j < value_count && ptr + 2 <= end; j++) {
      uint16_t value_len = (uint16_t)(ptr[0] | (ptr[1] << 8));
      ptr += 2;

      if (ptr + value_len > end) return NOSTR_DB_OK;

      // Index the first value of single-char tags
      if (indexable && j == 0) {
        uint8_t raw_value[32];
        internal_memset(raw_value, 0, 32);
        if (!hex_to_32bytes(ptr, value_len, raw_value)) {
          // Non-hex value: zero-pad the raw string into 32 bytes
          size_t copy_len = value_len > 32 ? 32 : value_len;
          internal_memcpy(raw_value, ptr, copy_len);
        }
        NostrDBError err = op(tree, tag_name, raw_value, rid);
        if (err != NOSTR_DB_OK) return err;
      }

      ptr += value_len;
    }
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// index_manager_create: Create all indexes from scratch
// ============================================================================
NostrDBError index_manager_create(IndexManager* im, BufferPool* pool)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);

  im->pool = pool;

  // ID index: key=32 bytes, value=RecordId (6 bytes), unique
  NostrDBError err =
    btree_create(&im->id_index, pool, 32, sizeof(RecordId), BTREE_KEY_BYTES32);
  if (err != NOSTR_DB_OK) return err;

  // Timeline index: key=int64 (8 bytes), value=page_id_t (4 bytes), dup
  err = btree_create(&im->timeline_index, pool, sizeof(int64_t),
                     sizeof(page_id_t), BTREE_KEY_INT64);
  if (err != NOSTR_DB_OK) return err;

  // Pubkey index: key=32 bytes, value=page_id_t (4 bytes), dup
  err = btree_create(&im->pubkey_index, pool, 32, sizeof(page_id_t),
                     BTREE_KEY_BYTES32);
  if (err != NOSTR_DB_OK) return err;

  // Kind index: key=uint32 (4 bytes), value=page_id_t (4 bytes), dup
  err = btree_create(&im->kind_index, pool, sizeof(uint32_t),
                     sizeof(page_id_t), BTREE_KEY_UINT32);
  if (err != NOSTR_DB_OK) return err;

  // Pubkey+Kind index: key=36 bytes, value=page_id_t (4 bytes), dup
  err = btree_create(&im->pubkey_kind_index, pool, 36, sizeof(page_id_t),
                     BTREE_KEY_COMPOSITE);
  if (err != NOSTR_DB_OK) return err;

  // Tag index: key=33 bytes (tag_name[1]+tag_value[32]), value=page_id_t, dup
  err = btree_create(&im->tag_index, pool, 33, sizeof(page_id_t),
                     BTREE_KEY_COMPOSITE_TAG);
  if (err != NOSTR_DB_OK) return err;

  return NOSTR_DB_OK;
}

// ============================================================================
// index_manager_open: Open existing indexes from meta pages
// ============================================================================
NostrDBError index_manager_open(IndexManager* im, BufferPool* pool,
                                page_id_t id_meta, page_id_t timeline_meta,
                                page_id_t pubkey_meta, page_id_t kind_meta,
                                page_id_t pk_kind_meta, page_id_t tag_meta)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);

  im->pool = pool;

  NostrDBError err = btree_open(&im->id_index, pool, id_meta);
  if (err != NOSTR_DB_OK) return err;

  err = btree_open(&im->timeline_index, pool, timeline_meta);
  if (err != NOSTR_DB_OK) return err;

  err = btree_open(&im->pubkey_index, pool, pubkey_meta);
  if (err != NOSTR_DB_OK) return err;

  err = btree_open(&im->kind_index, pool, kind_meta);
  if (err != NOSTR_DB_OK) return err;

  err = btree_open(&im->pubkey_kind_index, pool, pk_kind_meta);
  if (err != NOSTR_DB_OK) return err;

  err = btree_open(&im->tag_index, pool, tag_meta);
  if (err != NOSTR_DB_OK) return err;

  return NOSTR_DB_OK;
}

// ============================================================================
// index_manager_flush: Flush all index metadata
// ============================================================================
NostrDBError index_manager_flush(IndexManager* im)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);

  NostrDBError err = btree_flush_meta(&im->id_index);
  if (err != NOSTR_DB_OK) return err;

  err = btree_flush_meta(&im->timeline_index);
  if (err != NOSTR_DB_OK) return err;

  err = btree_flush_meta(&im->pubkey_index);
  if (err != NOSTR_DB_OK) return err;

  err = btree_flush_meta(&im->kind_index);
  if (err != NOSTR_DB_OK) return err;

  err = btree_flush_meta(&im->pubkey_kind_index);
  if (err != NOSTR_DB_OK) return err;

  err = btree_flush_meta(&im->tag_index);
  if (err != NOSTR_DB_OK) return err;

  return NOSTR_DB_OK;
}

// ============================================================================
// index_manager_insert_event: Insert event into all indexes
// ============================================================================
NostrDBError index_manager_insert_event(IndexManager* im, RecordId rid,
                                        const EventRecord* record,
                                        const uint8_t*     tags_data,
                                        uint16_t           tags_length)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(record, NOSTR_DB_ERROR_NULL_PARAM);

  // 1. ID index (unique)
  NostrDBError err = index_id_insert(&im->id_index, record->id, rid);
  if (err != NOSTR_DB_OK) return err;

  // 2. Timeline index (dup)
  err = index_timeline_insert(&im->timeline_index, record->created_at, rid);
  if (err != NOSTR_DB_OK) return err;

  // 3. Pubkey index (dup)
  err = index_pubkey_insert(&im->pubkey_index, record->pubkey, rid);
  if (err != NOSTR_DB_OK) return err;

  // 4. Kind index (dup)
  err = index_kind_insert(&im->kind_index, record->kind, rid);
  if (err != NOSTR_DB_OK) return err;

  // 5. Pubkey+Kind index (dup)
  err = index_pk_kind_insert(&im->pubkey_kind_index, record->pubkey,
                             record->kind, rid);
  if (err != NOSTR_DB_OK) return err;

  // 6. Tag index (dup) — parse serialized tags
  err = process_tags(&im->tag_index, tags_data, tags_length, rid,
                     index_tag_insert);
  if (err != NOSTR_DB_OK) return err;

  return NOSTR_DB_OK;
}

// ============================================================================
// index_manager_delete_event: Delete event from all indexes
// ============================================================================
NostrDBError index_manager_delete_event(IndexManager* im, RecordId rid,
                                        const EventRecord* record,
                                        const uint8_t*     tags_data,
                                        uint16_t           tags_length)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(record, NOSTR_DB_ERROR_NULL_PARAM);

  // 1. ID index
  NostrDBError err = index_id_delete(&im->id_index, record->id);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  // 2. Timeline index
  err =
    index_timeline_delete(&im->timeline_index, record->created_at, rid);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  // 3. Pubkey index
  err = index_pubkey_delete(&im->pubkey_index, record->pubkey, rid);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  // 4. Kind index
  err = index_kind_delete(&im->kind_index, record->kind, rid);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  // 5. Pubkey+Kind index
  err = index_pk_kind_delete(&im->pubkey_kind_index, record->pubkey,
                             record->kind, rid);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  // 6. Tag index
  err = process_tags(&im->tag_index, tags_data, tags_length, rid,
                     index_tag_delete);
  if (err != NOSTR_DB_OK && err != NOSTR_DB_ERROR_NOT_FOUND) return err;

  return NOSTR_DB_OK;
}
