#ifndef NOSTR_DB_RECORD_MANAGER_H_
#define NOSTR_DB_RECORD_MANAGER_H_

#include "../../../util/types.h"
#include "../buffer/buffer_pool.h"
#include "../db_types.h"
#include "record_types.h"

// ============================================================================
// Record manager operations
// ============================================================================

/**
 * @brief Insert a record into the database
 *
 * Finds a page with sufficient free space (or allocates a new one),
 * inserts the record, and returns its RecordId. For records larger than
 * one page, uses overflow pages (spanned records).
 *
 * @param pool Buffer pool instance
 * @param data Record data
 * @param length Record data length
 * @param out_rid Receives the assigned RecordId
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError record_insert(BufferPool* pool, const void* data,
                           uint16_t length, RecordId* out_rid);

/**
 * @brief Read a record from the database
 * @param pool Buffer pool instance
 * @param rid Record ID to read
 * @param out Output buffer (may be NULL to query length)
 * @param length On input: buffer capacity. On output: actual record length.
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError record_read(BufferPool* pool, RecordId rid, void* out,
                         uint16_t* length);

/**
 * @brief Delete a record from the database
 * @param pool Buffer pool instance
 * @param rid Record ID to delete
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError record_delete(BufferPool* pool, RecordId rid);

/**
 * @brief Update a record in the database
 *
 * If the new data fits in the existing slot, updates in-place.
 * Otherwise, deletes the old record and inserts a new one
 * (the RecordId may change in this case).
 *
 * @param pool Buffer pool instance
 * @param rid Record ID to update (may be modified if relocated)
 * @param data New record data
 * @param length New record data length
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError record_update(BufferPool* pool, RecordId* rid,
                           const void* data, uint16_t length);

#endif
