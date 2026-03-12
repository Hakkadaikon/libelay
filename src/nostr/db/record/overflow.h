#ifndef NOSTR_DB_OVERFLOW_H_
#define NOSTR_DB_OVERFLOW_H_

#include "../../../util/types.h"
#include "../buffer/buffer_pool.h"
#include "../db_types.h"
#include "record_types.h"

// ============================================================================
// Spanned record prefix (stored at the beginning of inline data in the slot)
//
// A spanned record is identified by slot->length == SPANNED_MARKER (0xFFFF).
// The prefix stores metadata needed to reconstruct the full record.
// ============================================================================
typedef struct {
  uint16_t  total_length;   // Total record data length (across all pages)
  uint16_t  inline_length;  // Bytes of inline data in this page (after prefix)
  page_id_t overflow_page;  // First overflow page (PAGE_ID_NULL if none)
} SpannedPrefix;

_Static_assert(sizeof(SpannedPrefix) == 8, "SpannedPrefix must be 8 bytes");

#define SPANNED_PREFIX_SIZE sizeof(SpannedPrefix)

/**
 * @brief Insert a spanned record across overflow pages
 * @param pool Buffer pool
 * @param data Record data
 * @param total_length Total record data length
 * @param out_rid Receives the RecordId
 * @return NOSTR_DB_OK on success
 */
NostrDBError overflow_insert(BufferPool* pool, const void* data,
                             uint16_t total_length, RecordId* out_rid);

/**
 * @brief Read a spanned record (slot->length must be SPANNED_MARKER)
 * @param pool Buffer pool
 * @param page Primary page (already pinned by caller)
 * @param slot_index Slot index
 * @param out Output buffer (may be NULL to query length)
 * @param out_length On input: buffer capacity. On output: total data length.
 * @return NOSTR_DB_OK on success
 */
NostrDBError overflow_read(BufferPool* pool, const PageData* page,
                           uint16_t slot_index, void* out,
                           uint16_t* out_length);

/**
 * @brief Free an overflow page chain
 * @param pool Buffer pool
 * @param first_overflow First overflow page ID
 * @return NOSTR_DB_OK on success
 */
NostrDBError overflow_free(BufferPool* pool, page_id_t first_overflow);

/**
 * @brief Check if a slot contains a spanned record
 *
 * A slot is spanned if and only if slot->length == SPANNED_MARKER.
 *
 * @param page Page data
 * @param slot_index Slot index
 * @return true if the slot is a spanned record
 */
bool overflow_is_spanned(const PageData* page, uint16_t slot_index);

#endif
