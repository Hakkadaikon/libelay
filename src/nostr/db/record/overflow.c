#include "overflow.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "slot_page.h"

// ============================================================================
// overflow_is_spanned: A slot is spanned iff length == SPANNED_MARKER
// ============================================================================
bool overflow_is_spanned(const PageData* page, uint16_t slot_index)
{
  if (is_null(page)) {
    return false;
  }

  const SlotPageHeader* hdr = (const SlotPageHeader*)page->data;
  if (slot_index >= hdr->slot_count) {
    return false;
  }

  const SlotEntry* slot =
    (const SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE +
                       slot_index * SLOT_ENTRY_SIZE);

  return (slot->length == SPANNED_MARKER);
}

// ============================================================================
// overflow_insert
// ============================================================================
NostrDBError overflow_insert(BufferPool* pool, const void* data,
                             uint16_t total_length, RecordId* out_rid)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out_rid, NOSTR_DB_ERROR_NULL_PARAM);

  const uint8_t* src = (const uint8_t*)data;

  // Calculate how much inline data fits in primary page
  // Primary page (fresh): header + 1 slot entry + spanned prefix + inline data
  uint16_t max_inline =
    (uint16_t)(DB_PAGE_SIZE - SLOT_PAGE_HEADER_SIZE - SLOT_ENTRY_SIZE -
               SPANNED_PREFIX_SIZE);
  uint16_t inline_data_len =
    (total_length <= max_inline) ? total_length : max_inline;
  uint16_t remaining = total_length - inline_data_len;

  // Build overflow chain
  page_id_t first_overflow = PAGE_ID_NULL;
  page_id_t prev_overflow  = PAGE_ID_NULL;
  uint16_t  ov_offset      = inline_data_len;

  while (remaining > 0) {
    PageData* ov_page = NULL;
    page_id_t ov_pid  = buffer_pool_alloc_page(pool, &ov_page);
    if (ov_pid == PAGE_ID_NULL) {
      if (first_overflow != PAGE_ID_NULL) {
        overflow_free(pool, first_overflow);
      }
      return NOSTR_DB_ERROR_FULL;
    }

    internal_memset(ov_page->data, 0, DB_PAGE_SIZE);
    OverflowHeader* ov_hdr = (OverflowHeader*)ov_page->data;
    ov_hdr->next_page      = PAGE_ID_NULL;

    uint16_t chunk      = (remaining > (uint16_t)OVERFLOW_DATA_SPACE)
                            ? (uint16_t)OVERFLOW_DATA_SPACE
                            : remaining;
    ov_hdr->data_length = chunk;

    internal_memcpy(ov_page->data + sizeof(OverflowHeader),
                    src + ov_offset, chunk);
    buffer_pool_mark_dirty(pool, ov_pid, 0);

    if (first_overflow == PAGE_ID_NULL) {
      first_overflow = ov_pid;
    }

    if (prev_overflow != PAGE_ID_NULL) {
      PageData* prev_page = buffer_pool_pin(pool, prev_overflow);
      if (!is_null(prev_page)) {
        OverflowHeader* prev_hdr = (OverflowHeader*)prev_page->data;
        prev_hdr->next_page      = ov_pid;
        buffer_pool_mark_dirty(pool, prev_overflow, 0);
        buffer_pool_unpin(pool, prev_overflow);
      }
    }

    prev_overflow = ov_pid;
    buffer_pool_unpin(pool, ov_pid);
    ov_offset += chunk;
    remaining -= chunk;
  }

  // Allocate primary page
  PageData* primary_page = NULL;
  page_id_t primary_pid  = buffer_pool_alloc_page(pool, &primary_page);
  if (primary_pid == PAGE_ID_NULL) {
    if (first_overflow != PAGE_ID_NULL) {
      overflow_free(pool, first_overflow);
    }
    return NOSTR_DB_ERROR_FULL;
  }

  slot_page_init(primary_page, primary_pid);

  // Build inline data: [SpannedPrefix][inline_data_bytes]
  uint16_t inline_total = (uint16_t)SPANNED_PREFIX_SIZE + inline_data_len;
  uint8_t  inline_buf[DB_PAGE_SIZE];

  SpannedPrefix prefix;
  prefix.total_length  = total_length;
  prefix.inline_length = inline_data_len;
  prefix.overflow_page = first_overflow;
  internal_memcpy(inline_buf, &prefix, SPANNED_PREFIX_SIZE);
  internal_memcpy(inline_buf + SPANNED_PREFIX_SIZE, src, inline_data_len);

  uint16_t     slot_idx = 0;
  NostrDBError err =
    slot_page_insert(primary_page, inline_buf, inline_total, &slot_idx);
  if (err != NOSTR_DB_OK) {
    if (first_overflow != PAGE_ID_NULL) {
      overflow_free(pool, first_overflow);
    }
    buffer_pool_unpin(pool, primary_pid);
    return err;
  }

  // Mark slot as spanned by setting length to SPANNED_MARKER
  SlotEntry* slot =
    (SlotEntry*)(primary_page->data + SLOT_PAGE_HEADER_SIZE +
                 slot_idx * SLOT_ENTRY_SIZE);
  slot->length = SPANNED_MARKER;

  buffer_pool_mark_dirty(pool, primary_pid, 0);
  buffer_pool_unpin(pool, primary_pid);

  out_rid->page_id    = primary_pid;
  out_rid->slot_index = slot_idx;

  return NOSTR_DB_OK;
}

// ============================================================================
// overflow_read
// ============================================================================
NostrDBError overflow_read(BufferPool* pool, const PageData* page,
                           uint16_t slot_index, void* out,
                           uint16_t* out_length)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(page, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out_length, NOSTR_DB_ERROR_NULL_PARAM);

  const SlotPageHeader* hdr = (const SlotPageHeader*)page->data;
  require(slot_index < hdr->slot_count, NOSTR_DB_ERROR_NOT_FOUND);

  const SlotEntry* slot =
    (const SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE +
                       slot_index * SLOT_ENTRY_SIZE);

  if (slot->offset == 0) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Read SpannedPrefix
  const SpannedPrefix* prefix =
    (const SpannedPrefix*)(page->data + slot->offset);
  uint16_t  total_length    = prefix->total_length;
  uint16_t  inline_data_len = prefix->inline_length;
  page_id_t overflow_pid    = prefix->overflow_page;

  if (is_null(out)) {
    *out_length = total_length;
    return NOSTR_DB_OK;
  }

  uint16_t capacity = *out_length;
  *out_length       = total_length;

  uint8_t* dst     = (uint8_t*)out;
  uint16_t written = 0;

  // Copy inline data (after SpannedPrefix)
  uint16_t copy =
    (inline_data_len < capacity) ? inline_data_len : capacity;
  internal_memcpy(dst, page->data + slot->offset + SPANNED_PREFIX_SIZE,
                  copy);
  written += copy;

  // Walk overflow chain
  page_id_t walk_pid = overflow_pid;
  while (walk_pid != PAGE_ID_NULL && written < capacity) {
    PageData* ov_page = buffer_pool_pin(pool, walk_pid);
    if (is_null(ov_page)) {
      break;
    }
    const OverflowHeader* ov_hdr = (const OverflowHeader*)ov_page->data;
    uint16_t              chunk  = ov_hdr->data_length;
    if (written + chunk > capacity) {
      chunk = capacity - written;
    }
    internal_memcpy(dst + written,
                    ov_page->data + sizeof(OverflowHeader), chunk);
    written += chunk;
    page_id_t next = ov_hdr->next_page;
    buffer_pool_unpin(pool, walk_pid);
    walk_pid = next;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// overflow_free
// ============================================================================
NostrDBError overflow_free(BufferPool* pool, page_id_t first_overflow)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);

  page_id_t pid = first_overflow;
  while (pid != PAGE_ID_NULL) {
    PageData* page = buffer_pool_pin(pool, pid);
    if (is_null(page)) {
      break;
    }
    const OverflowHeader* ov_hdr = (const OverflowHeader*)page->data;
    page_id_t             next   = ov_hdr->next_page;
    buffer_pool_unpin(pool, pid);

    disk_free_page(pool->disk, pid);
    pid = next;
  }

  return NOSTR_DB_OK;
}
