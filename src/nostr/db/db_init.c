#include "../../arch/memory.h"
#include "../../arch/mkdir.h"
#include "../../util/string.h"
#include "db.h"
#include "db_file.h"
#include "db_internal.h"

// File names
#define DB_FILE "nostr.db"
#define WAL_FILE "wal.log"

// Path buffer size
#define PATH_BUFFER_SIZE 512

// Buffer pool size (number of frames)
#define POOL_SIZE 1024

// ============================================================================
// Helper: Build file path from directory and filename
// ============================================================================
static bool build_path(char* buffer, size_t capacity, const char* dir,
                       const char* filename)
{
  size_t dir_len  = strlen(dir);
  size_t file_len = strlen(filename);

  if (dir_len + 1 + file_len + 1 > capacity) {
    return false;
  }

  internal_memcpy(buffer, dir, dir_len);
  buffer[dir_len] = '/';
  internal_memcpy(buffer + dir_len + 1, filename, file_len);
  buffer[dir_len + 1 + file_len] = '\0';

  return true;
}

// ============================================================================
// Helper: Initialize metadata page for a new database
// ============================================================================
static NostrDBError init_meta_page(NostrDB* db, page_id_t* out_meta_page)
{
  // Allocate a page for metadata (should be page 1, first allocation)
  PageData* page      = NULL;
  page_id_t meta_page = buffer_pool_alloc_page(&db->buffer_pool, &page);
  if (meta_page == PAGE_ID_NULL || is_null(page)) {
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  DBMetaPage* meta = (DBMetaPage*)page->data;
  internal_memset(meta, 0, sizeof(DBMetaPage));
  internal_memcpy(meta->magic, DB_META_MAGIC, DB_META_MAGIC_SIZE);
  meta->version = DB_FILE_VERSION;

  buffer_pool_mark_dirty(&db->buffer_pool, meta_page, 0);
  buffer_pool_unpin(&db->buffer_pool, meta_page);

  *out_meta_page = meta_page;
  return NOSTR_DB_OK;
}

// ============================================================================
// Helper: Read metadata page and populate db fields
// ============================================================================
static NostrDBError read_meta_page(NostrDB* db)
{
  PageData* page = buffer_pool_pin(&db->buffer_pool, DB_META_PAGE_ID);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  DBMetaPage* meta = (DBMetaPage*)page->data;

  // Validate magic
  if (internal_memcmp(meta->magic, DB_META_MAGIC, DB_META_MAGIC_SIZE) != 0) {
    buffer_pool_unpin(&db->buffer_pool, DB_META_PAGE_ID);
    return NOSTR_DB_ERROR_INVALID_MAGIC;
  }

  db->event_count   = meta->event_count;
  db->deleted_count = meta->deleted_count;

  buffer_pool_unpin(&db->buffer_pool, DB_META_PAGE_ID);
  return NOSTR_DB_OK;
}

// ============================================================================
// Helper: Write metadata page from db fields
// ============================================================================
static NostrDBError write_meta_page(NostrDB* db)
{
  PageData* page = buffer_pool_pin(&db->buffer_pool, DB_META_PAGE_ID);
  if (is_null(page)) {
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  DBMetaPage* meta    = (DBMetaPage*)page->data;
  meta->event_count   = db->event_count;
  meta->deleted_count = db->deleted_count;

  // Save index meta page IDs
  meta->id_index_meta       = db->indexes.id_index.meta_page;
  meta->timeline_index_meta = db->indexes.timeline_index.meta_page;
  meta->pubkey_index_meta   = db->indexes.pubkey_index.meta_page;
  meta->kind_index_meta     = db->indexes.kind_index.meta_page;
  meta->pk_kind_index_meta  = db->indexes.pubkey_kind_index.meta_page;
  meta->tag_index_meta      = db->indexes.tag_index.meta_page;

  buffer_pool_mark_dirty(&db->buffer_pool, DB_META_PAGE_ID, 0);
  buffer_pool_unpin(&db->buffer_pool, DB_META_PAGE_ID);

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_init
// ============================================================================
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data_dir, NOSTR_DB_ERROR_NULL_PARAM);

  static NostrDB db_instance;
  NostrDB*       pdb = &db_instance;

  internal_memset(pdb, 0, sizeof(NostrDB));

  // Copy data directory
  size_t dir_len = strlen(data_dir);
  if (dir_len >= sizeof(pdb->data_dir)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  internal_memcpy(pdb->data_dir, data_dir, dir_len + 1);

  // Build file paths
  char db_path[PATH_BUFFER_SIZE];
  char wal_path[PATH_BUFFER_SIZE];

  if (!build_path(db_path, sizeof(db_path), data_dir, DB_FILE)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  if (!build_path(wal_path, sizeof(wal_path), data_dir, WAL_FILE)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }

  // Ensure data directory exists (ignore EEXIST)
  internal_mkdir(data_dir, S_IRWXU);

  // Open or create database file
  bool         is_new = !nostr_db_file_exists(db_path);
  NostrDBError err;

  if (is_new) {
    err = disk_manager_create(&pdb->disk, db_path, DB_INITIAL_PAGES);
  } else {
    err = disk_manager_open(&pdb->disk, db_path);
  }
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // Initialize buffer pool
  err = buffer_pool_init(&pdb->buffer_pool, &pdb->disk, POOL_SIZE);
  if (err != NOSTR_DB_OK) {
    disk_manager_close(&pdb->disk);
    return err;
  }

  // Initialize WAL
  err = wal_init(&pdb->wal, wal_path);
  if (err != NOSTR_DB_OK) {
    buffer_pool_shutdown(&pdb->buffer_pool);
    disk_manager_close(&pdb->disk);
    return err;
  }

  // WAL recovery (for existing databases)
  if (!is_new) {
    wal_recover(&pdb->wal, &pdb->disk);
  }

  if (is_new) {
    // Allocate and initialize metadata page
    page_id_t meta_page;
    err = init_meta_page(pdb, &meta_page);
    if (err != NOSTR_DB_OK) {
      nostr_db_shutdown(pdb);
      return err;
    }

    // Create all indexes
    err = index_manager_create(&pdb->indexes, &pdb->buffer_pool);
    if (err != NOSTR_DB_OK) {
      nostr_db_shutdown(pdb);
      return err;
    }

    // Save index meta pages to metadata
    err = write_meta_page(pdb);
    if (err != NOSTR_DB_OK) {
      nostr_db_shutdown(pdb);
      return err;
    }

    // Flush everything for new database
    buffer_pool_flush_all(&pdb->buffer_pool);
  } else {
    // Read metadata and open existing indexes
    err = read_meta_page(pdb);
    if (err != NOSTR_DB_OK) {
      nostr_db_shutdown(pdb);
      return err;
    }

    // Read index meta page IDs from metadata page
    PageData* page = buffer_pool_pin(&pdb->buffer_pool, DB_META_PAGE_ID);
    if (is_null(page)) {
      nostr_db_shutdown(pdb);
      return NOSTR_DB_ERROR_MMAP_FAILED;
    }

    DBMetaPage* meta = (DBMetaPage*)page->data;

    err = index_manager_open(
      &pdb->indexes, &pdb->buffer_pool, meta->id_index_meta,
      meta->timeline_index_meta, meta->pubkey_index_meta,
      meta->kind_index_meta, meta->pk_kind_index_meta, meta->tag_index_meta);

    buffer_pool_unpin(&pdb->buffer_pool, DB_META_PAGE_ID);

    if (err != NOSTR_DB_OK) {
      nostr_db_shutdown(pdb);
      return err;
    }
  }

  pdb->initialized = true;
  *db              = pdb;
  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_shutdown
// ============================================================================
void nostr_db_shutdown(NostrDB* db)
{
  if (is_null(db)) {
    return;
  }

  if (db->initialized) {
    // Write metadata page before shutting down
    write_meta_page(db);

    // Checkpoint WAL
    wal_checkpoint(&db->wal, &db->buffer_pool);

    // Flush all dirty pages
    buffer_pool_flush_all(&db->buffer_pool);
  }

  // Shutdown in reverse order of initialization
  wal_shutdown(&db->wal);
  buffer_pool_shutdown(&db->buffer_pool);
  disk_manager_close(&db->disk);

  db->initialized = false;
}

// ============================================================================
// nostr_db_get_stats
// ============================================================================
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(stats, NOSTR_DB_ERROR_NULL_PARAM);

  stats->event_count            = db->event_count;
  stats->deleted_count          = db->deleted_count;
  stats->events_file_size       = (uint64_t)db->disk.total_pages * DB_PAGE_SIZE;
  stats->id_index_entries       = db->indexes.id_index.meta.entry_count;
  stats->pubkey_index_entries   = db->indexes.pubkey_index.meta.entry_count;
  stats->kind_index_entries     = db->indexes.kind_index.meta.entry_count;
  stats->tag_index_entries      = db->indexes.tag_index.meta.entry_count;
  stats->timeline_index_entries = db->indexes.timeline_index.meta.entry_count;

  return NOSTR_DB_OK;
}
