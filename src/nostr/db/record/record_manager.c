#include "record_manager.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "overflow.h"
#include "slot_page.h"

// ============================================================================
// Internal: Maximum record size that fits in a single page
// (page size minus header minus one slot entry)
// ============================================================================
#define MAX_INLINE_RECORD_SIZE \
  (DB_PAGE_SIZE - SLOT_PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE)

// ============================================================================
// Internal: Try to insert into an existing record page in the buffer pool
// ============================================================================
static page_id_t try_insert_existing(BufferPool* pool, const void* data,
                                     uint16_t length, uint16_t* slot_index)
{
  uint16_t needed = length + (uint16_t)SLOT_ENTRY_SIZE;

  for (uint32_t i = 0; i < pool->pool_size; i++) {
    page_id_t pid = pool->page_ids[i];
    if (pid == PAGE_ID_NULL) {
      continue;
    }

    PageData* page = buffer_pool_pin(pool, pid);
    if (is_null(page)) {
      continue;
    }

    SlotPageHeader* hdr = (SlotPageHeader*)page->data;
    if (hdr->page_type != PAGE_TYPE_RECORD) {
      buffer_pool_unpin(pool, pid);
      continue;
    }

    if (slot_page_free_space(page) >= needed) {
      NostrDBError err = slot_page_insert(page, data, length, slot_index);
      if (err == NOSTR_DB_OK) {
        buffer_pool_mark_dirty(pool, pid, 0);
        buffer_pool_unpin(pool, pid);
        return pid;
      }
    }

    buffer_pool_unpin(pool, pid);
  }

  return PAGE_ID_NULL;
}

// ============================================================================
// record_insert
// ============================================================================
NostrDBError record_insert(BufferPool* pool, const void* data,
                           uint16_t length, RecordId* out_rid)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out_rid, NOSTR_DB_ERROR_NULL_PARAM);
  require(length > 0, NOSTR_DB_ERROR_NULL_PARAM);

  // Large record: use overflow pages
  if (length > MAX_INLINE_RECORD_SIZE) {
    return overflow_insert(pool, data, length, out_rid);
  }

  // Try existing page
  uint16_t  slot_idx = 0;
  page_id_t pid      = try_insert_existing(pool, data, length, &slot_idx);
  if (pid != PAGE_ID_NULL) {
    out_rid->page_id    = pid;
    out_rid->slot_index = slot_idx;
    return NOSTR_DB_OK;
  }

  // Allocate a new page
  PageData* page    = NULL;
  page_id_t new_pid = buffer_pool_alloc_page(pool, &page);
  if (new_pid == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_FULL;
  }

  slot_page_init(page, new_pid);

  NostrDBError err = slot_page_insert(page, data, length, &slot_idx);
  if (err != NOSTR_DB_OK) {
    buffer_pool_unpin(pool, new_pid);
    return err;
  }

  buffer_pool_mark_dirty(pool, new_pid, 0);
  buffer_pool_unpin(pool, new_pid);

  out_rid->page_id    = new_pid;
  out_rid->slot_index = slot_idx;

  return NOSTR_DB_OK;
}

// ============================================================================
// record_read
// ============================================================================
NostrDBError record_read(BufferPool* pool, RecordId rid, void* out,
                         uint16_t* length)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(length, NOSTR_DB_ERROR_NULL_PARAM);
  require(rid.page_id != PAGE_ID_NULL, NOSTR_DB_ERROR_NULL_PARAM);

  PageData* page = buffer_pool_pin(pool, rid.page_id);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Check for spanned record
  if (overflow_is_spanned(page, rid.slot_index)) {
    NostrDBError err =
      overflow_read(pool, page, rid.slot_index, out, length);
    buffer_pool_unpin(pool, rid.page_id);
    return err;
  }

  NostrDBError err = slot_page_read(page, rid.slot_index, out, length);
  buffer_pool_unpin(pool, rid.page_id);
  return err;
}

// ============================================================================
// record_delete
// ============================================================================
NostrDBError record_delete(BufferPool* pool, RecordId rid)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require(rid.page_id != PAGE_ID_NULL, NOSTR_DB_ERROR_NULL_PARAM);

  PageData* page = buffer_pool_pin(pool, rid.page_id);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Free overflow chain if spanned
  if (overflow_is_spanned(page, rid.slot_index)) {
    const SlotEntry* slot =
      (const SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE +
                         rid.slot_index * SLOT_ENTRY_SIZE);
    const SpannedPrefix* prefix =
      (const SpannedPrefix*)(page->data + slot->offset);
    if (prefix->overflow_page != PAGE_ID_NULL) {
      overflow_free(pool, prefix->overflow_page);
    }
  }

  NostrDBError err = slot_page_delete(page, rid.slot_index);
  if (err == NOSTR_DB_OK) {
    buffer_pool_mark_dirty(pool, rid.page_id, 0);
  }

  buffer_pool_unpin(pool, rid.page_id);
  return err;
}

// ============================================================================
// record_update
// ============================================================================
NostrDBError record_update(BufferPool* pool, RecordId* rid,
                           const void* data, uint16_t length)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rid, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data, NOSTR_DB_ERROR_NULL_PARAM);
  require(length > 0, NOSTR_DB_ERROR_NULL_PARAM);
  require(rid->page_id != PAGE_ID_NULL, NOSTR_DB_ERROR_NULL_PARAM);

  PageData* page = buffer_pool_pin(pool, rid->page_id);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Spanned records always get delete + reinsert
  if (overflow_is_spanned(page, rid->slot_index)) {
    buffer_pool_unpin(pool, rid->page_id);
    NostrDBError err = record_delete(pool, *rid);
    if (err != NOSTR_DB_OK) {
      return err;
    }
    return record_insert(pool, data, length, rid);
  }

  const SlotPageHeader* hdr = (const SlotPageHeader*)page->data;
  if (rid->slot_index >= hdr->slot_count) {
    buffer_pool_unpin(pool, rid->page_id);
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  const SlotEntry* slot =
    (const SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE +
                       rid->slot_index * SLOT_ENTRY_SIZE);

  if (slot->offset == 0 && slot->length == 0) {
    buffer_pool_unpin(pool, rid->page_id);
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // In-place update if new data fits
  if (length <= slot->length) {
    internal_memcpy(page->data + slot->offset, data, length);
    if (length < slot->length) {
      SlotPageHeader* mut_hdr = (SlotPageHeader*)page->data;
      mut_hdr->fragmented_space += (slot->length - length);
      SlotEntry* mut_slot =
        (SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE +
                     rid->slot_index * SLOT_ENTRY_SIZE);
      mut_slot->length = length;
    }
    buffer_pool_mark_dirty(pool, rid->page_id, 0);
    buffer_pool_unpin(pool, rid->page_id);
    return NOSTR_DB_OK;
  }

  buffer_pool_unpin(pool, rid->page_id);

  // Delete and reinsert
  NostrDBError err = record_delete(pool, *rid);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  return record_insert(pool, data, length, rid);
}
