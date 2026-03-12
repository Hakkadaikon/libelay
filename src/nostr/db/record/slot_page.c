#include "slot_page.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"

// ============================================================================
// Internal: Get pointer to the slot page header
// ============================================================================
static inline SlotPageHeader* get_header(PageData* page)
{
  return (SlotPageHeader*)page->data;
}

static inline const SlotPageHeader* get_header_const(const PageData* page)
{
  return (const SlotPageHeader*)page->data;
}

// ============================================================================
// Internal: Get pointer to a slot entry
// ============================================================================
static inline SlotEntry* get_slot(PageData* page, uint16_t index)
{
  uint8_t* base = page->data + SLOT_PAGE_HEADER_SIZE;
  return (SlotEntry*)(base + index * SLOT_ENTRY_SIZE);
}

static inline const SlotEntry* get_slot_const(const PageData* page,
                                              uint16_t        index)
{
  const uint8_t* base = page->data + SLOT_PAGE_HEADER_SIZE;
  return (const SlotEntry*)(base + index * SLOT_ENTRY_SIZE);
}

// ============================================================================
// slot_page_init
// ============================================================================
void slot_page_init(PageData* page, page_id_t page_id)
{
  if (is_null(page)) {
    return;
  }

  internal_memset(page->data, 0, DB_PAGE_SIZE);

  SlotPageHeader* hdr = get_header(page);
  hdr->page_id        = page_id;
  hdr->page_type      = PAGE_TYPE_RECORD;
  hdr->flags          = 0;
  hdr->slot_count     = 0;
  // Slot directory starts right after the header
  hdr->free_space_start = (uint16_t)SLOT_PAGE_HEADER_SIZE;
  // Record data grows from the end of the page backward
  hdr->free_space_end   = (uint16_t)DB_PAGE_SIZE;
  hdr->fragmented_space = 0;
  hdr->overflow_page    = PAGE_ID_NULL;
}

// ============================================================================
// slot_page_insert
// ============================================================================
NostrDBError slot_page_insert(PageData* page, const void* data,
                              uint16_t length, uint16_t* slot_index)
{
  require_not_null(page, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(slot_index, NOSTR_DB_ERROR_NULL_PARAM);
  require(length > 0, NOSTR_DB_ERROR_NULL_PARAM);

  SlotPageHeader* hdr = get_header(page);

  // Space needed: slot entry + record data
  uint16_t needed = length + (uint16_t)SLOT_ENTRY_SIZE;

  // Check if we can reuse a deleted slot (offset == 0 and length == 0)
  int32_t reuse_slot = -1;
  for (uint16_t i = 0; i < hdr->slot_count; i++) {
    SlotEntry* slot = get_slot(page, i);
    if (slot->offset == 0 && slot->length == 0) {
      reuse_slot = (int32_t)i;
      needed     = length;  // No new slot entry needed
      break;
    }
  }

  // Available contiguous free space
  uint16_t available = hdr->free_space_end - hdr->free_space_start;
  if (available < needed) {
    // Try compaction if fragmented space + available >= needed
    if (available + hdr->fragmented_space >= needed) {
      slot_page_compact(page);
      available = hdr->free_space_end - hdr->free_space_start;
    }
    if (available < needed) {
      return NOSTR_DB_ERROR_FULL;
    }
  }

  // Allocate record data space (grow from end backward)
  hdr->free_space_end -= length;
  uint16_t record_offset = hdr->free_space_end;

  // Copy record data
  internal_memcpy(page->data + record_offset, data, length);

  // Set up slot entry
  if (reuse_slot >= 0) {
    SlotEntry* slot = get_slot(page, (uint16_t)reuse_slot);
    slot->offset    = record_offset;
    slot->length    = length;
    *slot_index     = (uint16_t)reuse_slot;
  } else {
    // New slot at the end of directory
    uint16_t   idx  = hdr->slot_count;
    SlotEntry* slot = get_slot(page, idx);
    slot->offset    = record_offset;
    slot->length    = length;
    hdr->slot_count++;
    hdr->free_space_start += (uint16_t)SLOT_ENTRY_SIZE;
    *slot_index = idx;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// slot_page_read
// ============================================================================
NostrDBError slot_page_read(const PageData* page, uint16_t slot_index,
                            void* out, uint16_t* length)
{
  require_not_null(page, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(length, NOSTR_DB_ERROR_NULL_PARAM);

  const SlotPageHeader* hdr = get_header_const(page);
  require(slot_index < hdr->slot_count, NOSTR_DB_ERROR_NOT_FOUND);

  const SlotEntry* slot = get_slot_const(page, slot_index);

  // Deleted slot
  if (slot->offset == 0 && slot->length == 0) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  uint16_t record_len = slot->length;

  // If caller just wants the length
  if (is_null(out)) {
    *length = record_len;
    return NOSTR_DB_OK;
  }

  // Copy record data
  uint16_t copy_len = (*length < record_len) ? *length : record_len;
  internal_memcpy(out, page->data + slot->offset, copy_len);
  *length = record_len;

  return NOSTR_DB_OK;
}

// ============================================================================
// slot_page_delete
// ============================================================================
NostrDBError slot_page_delete(PageData* page, uint16_t slot_index)
{
  require_not_null(page, NOSTR_DB_ERROR_NULL_PARAM);

  SlotPageHeader* hdr = get_header(page);
  require(slot_index < hdr->slot_count, NOSTR_DB_ERROR_NOT_FOUND);

  SlotEntry* slot = get_slot(page, slot_index);

  // Already deleted
  if (slot->offset == 0 && slot->length == 0) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Track fragmented space
  hdr->fragmented_space += slot->length;

  // Mark slot as deleted
  slot->offset = 0;
  slot->length = 0;

  return NOSTR_DB_OK;
}

// ============================================================================
// slot_page_free_space
// ============================================================================
uint16_t slot_page_free_space(const PageData* page)
{
  if (is_null(page)) {
    return 0;
  }

  const SlotPageHeader* hdr        = get_header_const(page);
  uint16_t              contiguous = hdr->free_space_end - hdr->free_space_start;
  return contiguous + hdr->fragmented_space;
}

// ============================================================================
// slot_page_compact
// ============================================================================
void slot_page_compact(PageData* page)
{
  if (is_null(page)) {
    return;
  }

  SlotPageHeader* hdr = get_header(page);
  if (hdr->fragmented_space == 0) {
    return;  // Nothing to compact
  }

  // Compact by moving all live records toward the end of the page.
  // We rebuild record area from the end, processing slots in order.
  uint16_t new_end = (uint16_t)DB_PAGE_SIZE;

  // Use a temporary buffer on the stack to avoid overwriting data in-place
  // We only need to store the record data portion (not the header/slots)
  uint8_t temp[DB_PAGE_SIZE];

  for (uint16_t i = 0; i < hdr->slot_count; i++) {
    SlotEntry* slot = get_slot(page, i);

    // Skip deleted slots
    if (slot->offset == 0 && slot->length == 0) {
      continue;
    }

    uint16_t len = slot->length;
    new_end -= len;

    // Copy record data to temp buffer at new position
    internal_memcpy(temp + new_end, page->data + slot->offset, len);

    // Update slot offset
    slot->offset = new_end;
  }

  // Copy compacted data back from temp to page
  if (new_end < (uint16_t)DB_PAGE_SIZE) {
    internal_memcpy(page->data + new_end, temp + new_end,
                    (uint16_t)DB_PAGE_SIZE - new_end);
  }

  hdr->free_space_end   = new_end;
  hdr->fragmented_space = 0;
}

// ============================================================================
// slot_page_slot_count
// ============================================================================
uint16_t slot_page_slot_count(const PageData* page)
{
  if (is_null(page)) {
    return 0;
  }
  return get_header_const(page)->slot_count;
}
