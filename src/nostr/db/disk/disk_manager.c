#include "disk_manager.h"

#include "../../../arch/close.h"
#include "../../../arch/file_read.h"
#include "../../../arch/file_write.h"
#include "../../../arch/fstat.h"
#include "../../../arch/fsync.h"
#include "../../../arch/memory.h"
#include "../../../arch/open.h"
#include "../../../util/string.h"
#include "../db_file.h"

// ============================================================================
// Internal: Calculate byte offset for a page
// ============================================================================
static inline int64_t page_offset(page_id_t page_id)
{
  return (int64_t)page_id * DB_PAGE_SIZE;
}

// ============================================================================
// Internal: Validate header magic and version
// ============================================================================
static bool validate_header(const FileHeader* header)
{
  require_not_null(header, false);

  if (internal_memcmp(header->magic, DB_FILE_MAGIC, DB_FILE_MAGIC_SIZE) != 0) {
    return false;
  }
  if (header->version != DB_FILE_VERSION) {
    return false;
  }
  if (header->page_size != DB_PAGE_SIZE) {
    return false;
  }
  return true;
}

// ============================================================================
// Internal: Initialize header fields
// ============================================================================
static void init_header(FileHeader* header, uint32_t total_pages)
{
  internal_memset(header, 0, sizeof(FileHeader));
  internal_memcpy(header->magic, DB_FILE_MAGIC, DB_FILE_MAGIC_SIZE);
  header->version         = DB_FILE_VERSION;
  header->page_size       = DB_PAGE_SIZE;
  header->total_pages     = total_pages;
  header->free_list_head  = PAGE_ID_NULL;
  header->free_page_count = 0;
}

// ============================================================================
// Internal: Copy path to disk manager
// ============================================================================
static bool copy_path(DiskManager* dm, const char* path)
{
  size_t len = strlen(path);
  if (len >= sizeof(dm->path)) {
    return false;
  }
  internal_memcpy(dm->path, path, len + 1);
  return true;
}

// ============================================================================
// Internal: Load header from disk into cached fields
// ============================================================================
static NostrDBError load_header(DiskManager* dm)
{
  PageData page;
  ssize_t  n = internal_pread(dm->fd, &page, DB_PAGE_SIZE, 0);
  if (n != DB_PAGE_SIZE) {
    return NOSTR_DB_ERROR_FILE_OPEN;
  }

  FileHeader* header = (FileHeader*)&page;
  if (!validate_header(header)) {
    return NOSTR_DB_ERROR_INVALID_MAGIC;
  }

  dm->total_pages     = header->total_pages;
  dm->free_list_head  = header->free_list_head;
  dm->free_page_count = header->free_page_count;

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_manager_open
// ============================================================================
NostrDBError disk_manager_open(DiskManager* dm, const char* path)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(path, NOSTR_DB_ERROR_NULL_PARAM);

  internal_memset(dm, 0, sizeof(DiskManager));
  dm->fd = -1;

  if (!copy_path(dm, path)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }

  int32_t fd = internal_open(path, O_RDWR, 0);
  if (fd < 0) {
    return NOSTR_DB_ERROR_FILE_OPEN;
  }
  dm->fd = fd;

  NostrDBError err = load_header(dm);
  if (err != NOSTR_DB_OK) {
    internal_close(fd);
    dm->fd = -1;
    return err;
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_manager_create
// ============================================================================
NostrDBError disk_manager_create(DiskManager* dm, const char* path, uint32_t initial_pages)
{
  require_not_null(dm, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(path, NOSTR_DB_ERROR_NULL_PARAM);
  require(initial_pages >= 2, NOSTR_DB_ERROR_NULL_PARAM);

  internal_memset(dm, 0, sizeof(DiskManager));
  dm->fd = -1;

  if (!copy_path(dm, path)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }

  // Create file with initial size
  size_t  file_size = (size_t)initial_pages * DB_PAGE_SIZE;
  int32_t fd        = nostr_db_file_create(path, file_size);
  if (fd < 0) {
    return NOSTR_DB_ERROR_FILE_CREATE;
  }
  dm->fd = fd;

  // Write header to page 0
  PageData    header_page;
  FileHeader* header = (FileHeader*)&header_page;
  init_header(header, initial_pages);

  // All pages except page 0 (header) are initially free.
  // Build free list: page 1 -> page 2 -> ... -> page (N-1) -> NULL
  // We only store the head in the header; the chain is on disk.
  if (initial_pages > 1) {
    header->free_list_head  = 1;
    header->free_page_count = initial_pages - 1;
  }

  ssize_t n = internal_pwrite(fd, &header_page, DB_PAGE_SIZE, 0);
  if (n != DB_PAGE_SIZE) {
    internal_close(fd);
    dm->fd = -1;
    return NOSTR_DB_ERROR_FILE_CREATE;
  }

  // Write free list chain on each free page
  for (uint32_t i = 1; i < initial_pages; i++) {
    PageData      free_page;
    FreePageEntry entry;

    internal_memset(&free_page, 0, DB_PAGE_SIZE);
    entry.next_free = (i + 1 < initial_pages) ? (page_id_t)(i + 1) : PAGE_ID_NULL;
    internal_memcpy(&free_page, &entry, sizeof(FreePageEntry));

    n = internal_pwrite(fd, &free_page, DB_PAGE_SIZE, page_offset(i));
    if (n != DB_PAGE_SIZE) {
      internal_close(fd);
      dm->fd = -1;
      return NOSTR_DB_ERROR_FILE_CREATE;
    }
  }

  // Sync to ensure all pages are on disk
  internal_fdatasync(fd);

  // Cache header fields
  dm->total_pages     = header->total_pages;
  dm->free_list_head  = header->free_list_head;
  dm->free_page_count = header->free_page_count;

  return NOSTR_DB_OK;
}

// ============================================================================
// disk_manager_close
// ============================================================================
void disk_manager_close(DiskManager* dm)
{
  if (is_null(dm) || dm->fd < 0) {
    return;
  }

  // Flush header before closing
  disk_flush_header(dm);
  internal_fdatasync(dm->fd);
  internal_close(dm->fd);
  dm->fd = -1;
}
