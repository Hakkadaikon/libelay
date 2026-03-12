#ifndef NOSTR_DB_SLOT_PAGE_H_
#define NOSTR_DB_SLOT_PAGE_H_

#include "../../../util/types.h"
#include "../db_types.h"
#include "../disk/disk_types.h"
#include "record_types.h"

// ============================================================================
// Slot page operations
// ============================================================================

/**
 * @brief Initialize a page as a slot page
 * @param page Page data buffer
 * @param page_id Page ID to assign
 */
void slot_page_init(PageData* page, page_id_t page_id);

/**
 * @brief Insert a record into the slot page
 * @param page Page data buffer
 * @param data Record data to insert
 * @param length Record data length
 * @param slot_index Receives the assigned slot index
 * @return NOSTR_DB_OK on success, NOSTR_DB_ERROR_FULL if insufficient space
 */
NostrDBError slot_page_insert(PageData* page, const void* data,
                              uint16_t length, uint16_t* slot_index);

/**
 * @brief Read a record from the slot page
 * @param page Page data buffer
 * @param slot_index Slot index to read
 * @param out Output buffer for record data (may be NULL to query length only)
 * @param length On input: buffer capacity. On output: actual record length.
 * @return NOSTR_DB_OK on success, NOSTR_DB_ERROR_NOT_FOUND if slot is empty
 */
NostrDBError slot_page_read(const PageData* page, uint16_t slot_index,
                            void* out, uint16_t* length);

/**
 * @brief Delete a record from the slot page
 * @param page Page data buffer
 * @param slot_index Slot index to delete
 * @return NOSTR_DB_OK on success, NOSTR_DB_ERROR_NOT_FOUND if slot is empty
 */
NostrDBError slot_page_delete(PageData* page, uint16_t slot_index);

/**
 * @brief Get available free space in the slot page
 * @param page Page data buffer
 * @return Available free space in bytes (including reclaimable fragmented space)
 */
uint16_t slot_page_free_space(const PageData* page);

/**
 * @brief Compact the page by eliminating fragmentation
 * Moves all records toward the end of the page, reclaiming fragmented space.
 * Slot directory offsets are updated; external RecordIds remain valid.
 * @param page Page data buffer
 */
void slot_page_compact(PageData* page);

/**
 * @brief Get the slot count of a slot page
 * @param page Page data buffer
 * @return Number of slots in the directory
 */
uint16_t slot_page_slot_count(const PageData* page);

#endif
