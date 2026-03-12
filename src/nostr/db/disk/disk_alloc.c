#include "../../../arch/file_read.h"
#include "../../../arch/file_write.h"
#include "../../../arch/fstat.h"
#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "../db_file.h"
#include "disk_manager.h"

// ============================================================================
// Internal: Calculate byte offset for a page
// ============================================================================
static inline int64_t page_offset(page_id_t page_id)
{
  return (int64_t)page_id * DB_PAGE_SIZE;
}

// ============================================================================
// disk_alloc_page
// ============================================================================
page_id_t disk_alloc_page(DiskManager* dm)
{
  require_not_null(dm, PAGE_ID_NULL);
  require(dm->fd >= 0, PAGE_ID_NULL);

  // If free list is empty, extend the file
  if (dm->free_list_head == PAGE_ID_NULL) {
    if (disk_extend(dm, DB_EXTEND_PAGES) != NOSTR_DB_OK) {
      return PAGE_ID_NULL;
    }
  }

  // Pop from free list head
  page_id_t allocated = dm->free_list_head;

  // Read the free page to get next pointer
  PageData page;
  ssize_t  n = internal_pread(dm->fd, &page, DB_PAGE_SIZE, page_offset(allocated));
  if (n != DB_PAGE_SIZE) {
    return PAGE_ID_NULL;
  }

  FreePageEntry* entry = (FreePageEntry*)&page;
  dm->free_list_head   = entry->next_free;
  dm->free_page_count--;

  // Zero out the allocated page for clean use
  internal_memset(&page, 0, DB_PAGE_SIZE);
  n = internal_pwrite(dm->fd, &page, DB_PAGE_SIZE, page_offset(allocated));
  if (n != DB_PAGE_SIZE) {
    // Rollback: put it back on free list
    dm->free_list_head = allocated;
    dm->free_page_count++;
    return PAGE_ID_NULL;
  }

  return allocated;
}

// ============================================================================
// disk_free_page
// ============================================================================
NostrDBError disk_free_page(DiskManager* dm, page_id_t page_id)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);
  require(page_id > 0 && page_id < dm->total_pages, NOSTR_DB_ERROR_NOT_FOUND);

  // Write a free page entry pointing to current head
  PageData      page;
  FreePageEntry entry;

  internal_memset(&page, 0, DB_PAGE_SIZE);
  entry.next_free = dm->free_list_head;
  internal_memcpy(&page, &entry, sizeof(FreePageEntry));

  ssize_t n = internal_pwrite(dm->fd, &page, DB_PAGE_SIZE, page_offset(page_id));
  if (n != DB_PAGE_SIZE) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  // Update cached free list
  dm->free_list_head = page_id;
  dm->free_page_count++;

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_extend
// ============================================================================
NostrDBError disk_extend(DiskManager* dm, uint32_t additional_pages)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);
  require(additional_pages > 0, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t old_total = dm->total_pages;
  uint32_t new_total = old_total + additional_pages;

  // Check max pages limit
  if (new_total > DB_MAX_PAGES) {
    return NOSTR_DB_ERROR_FULL;
  }

  // Extend the file
  size_t new_size = (size_t)new_total * DB_PAGE_SIZE;
  if (nostr_db_file_extend(dm->fd, new_size) < 0) {
    return NOSTR_DB_ERROR_FTRUNCATE_FAILED;
  }

  // Build free list for new pages: new pages chain to each other,
  // the last new page points to the old free list head.
  for (uint32_t i = old_total; i < new_total; i++) {
    PageData      page;
    FreePageEntry entry;

    internal_memset(&page, 0, DB_PAGE_SIZE);
    entry.next_free = (i + 1 < new_total) ? (page_id_t)(i + 1) : dm->free_list_head;
    internal_memcpy(&page, &entry, sizeof(FreePageEntry));

    ssize_t n = internal_pwrite(dm->fd, &page, DB_PAGE_SIZE, page_offset(i));
    if (n != DB_PAGE_SIZE) {
      return NOSTR_DB_ERROR_FILE_CREATE;
    }
  }

  // Update cached state
  dm->free_list_head = (page_id_t)old_total;
  dm->free_page_count += additional_pages;
  dm->total_pages = new_total;

  return NOSTR_DB_OK;
}
