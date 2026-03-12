#include "../../../arch/file_read.h"
#include "../../../arch/file_write.h"
#include "../../../arch/fsync.h"
#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "disk_manager.h"

// ============================================================================
// Internal: Calculate byte offset for a page
// ============================================================================
static inline int64_t page_offset(page_id_t page_id)
{
  return (int64_t)page_id * DB_PAGE_SIZE;
}

// ============================================================================
// disk_read_page
// ============================================================================
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);
  require(page_id > 0 && page_id < dm->total_pages, NOSTR_DB_ERROR_NOT_FOUND);

  ssize_t n = internal_pread(dm->fd, out, DB_PAGE_SIZE, page_offset(page_id));
  if (n != DB_PAGE_SIZE) {
    return NOSTR_DB_ERROR_FILE_OPEN;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_write_page
// ============================================================================
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id, const PageData* data)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);
  require(page_id > 0 && page_id < dm->total_pages, NOSTR_DB_ERROR_NOT_FOUND);

  ssize_t n = internal_pwrite(dm->fd, data, DB_PAGE_SIZE, page_offset(page_id));
  if (n != DB_PAGE_SIZE) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_sync
// ============================================================================
NostrDBError disk_sync(DiskManager* dm)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);

  if (internal_fdatasync(dm->fd) < 0) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_flush_header
// ============================================================================
NostrDBError disk_flush_header(DiskManager* dm)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require(dm->fd >= 0, NOSTR_DB_ERROR_FILE_OPEN);

  // Build header page from cached fields
  PageData    header_page;
  FileHeader* header = (FileHeader*)&header_page;

  internal_memset(&header_page, 0, DB_PAGE_SIZE);
  internal_memcpy(header->magic, DB_FILE_MAGIC, DB_FILE_MAGIC_SIZE);
  header->version         = DB_FILE_VERSION;
  header->page_size       = DB_PAGE_SIZE;
  header->total_pages     = dm->total_pages;
  header->free_list_head  = dm->free_list_head;
  header->free_page_count = dm->free_page_count;

  ssize_t n = internal_pwrite(dm->fd, &header_page, DB_PAGE_SIZE, 0);
  if (n != DB_PAGE_SIZE) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  return NOSTR_DB_OK;
}
