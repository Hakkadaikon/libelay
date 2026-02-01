#include "../../arch/memory.h"
#include "../../util/string.h"
#include "db.h"
#include "db_file.h"
#include "db_internal.h"
#include "db_mmap.h"

// File names
#define EVENTS_FILE "events.dat"
#define ID_IDX_FILE "idx_id.dat"
#define PUBKEY_IDX_FILE "idx_pubkey.dat"
#define KIND_IDX_FILE "idx_kind.dat"
#define PK_KIND_IDX_FILE "idx_pubkey_kind.dat"
#define TAG_IDX_FILE "idx_tag.dat"
#define TIMELINE_IDX_FILE "idx_timeline.dat"

// Path buffer size
#define PATH_BUFFER_SIZE 512

// ============================================================================
// Helper: Build file path
// ============================================================================
static bool build_path(char* buffer, size_t capacity, const char* dir, const char* filename)
{
  size_t dir_len  = strlen(dir);
  size_t file_len = strlen(filename);

  // Check capacity: dir + "/" + filename + "\0"
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
// Helper: Initialize events header
// ============================================================================
static void init_events_header(NostrDBEventsHeader* header, size_t file_size)
{
  internal_memset(header, 0, sizeof(NostrDBEventsHeader));
  internal_memcpy(header->magic, NOSTR_DB_MAGIC_EVENTS, NOSTR_DB_MAGIC_SIZE);
  header->version           = NOSTR_DB_VERSION;
  header->flags             = 0;
  header->event_count       = 0;
  header->next_write_offset = sizeof(NostrDBEventsHeader);
  header->deleted_count     = 0;
  header->file_size         = file_size;
}

// ============================================================================
// Helper: Initialize index header
// ============================================================================
static void init_index_header(NostrDBIndexHeader* header, const char* magic, size_t pool_size)
{
  internal_memset(header, 0, sizeof(NostrDBIndexHeader));
  internal_memcpy(header->magic, magic, NOSTR_DB_MAGIC_SIZE);
  header->version          = NOSTR_DB_VERSION;
  header->flags            = 0;
  header->bucket_count     = 0;
  header->entry_count      = 0;
  header->pool_next_offset = sizeof(NostrDBIndexHeader);
  header->pool_size        = pool_size;
}

// ============================================================================
// Helper: Validate events header
// ============================================================================
static bool validate_events_header(const NostrDBEventsHeader* header)
{
  require_not_null(header, false);

  // Check magic
  for (int i = 0; i < NOSTR_DB_MAGIC_SIZE; i++) {
    if (header->magic[i] != NOSTR_DB_MAGIC_EVENTS[i]) {
      return false;
    }
  }

  // Check version
  if (header->version != NOSTR_DB_VERSION) {
    return false;
  }

  return true;
}

// ============================================================================
// Helper: Validate index header
// ============================================================================
static bool validate_index_header(const NostrDBIndexHeader* header, const char* expected_magic)
{
  require_not_null(header, false);
  require_not_null(expected_magic, false);

  // Check magic
  for (int i = 0; i < NOSTR_DB_MAGIC_SIZE; i++) {
    if (header->magic[i] != expected_magic[i]) {
      return false;
    }
  }

  // Check version
  if (header->version != NOSTR_DB_VERSION) {
    return false;
  }

  return true;
}

// ============================================================================
// Helper: Open or create a file with mmap
// ============================================================================
static NostrDBError open_or_create_file(
  const char* path,
  const char* magic,
  size_t      default_size,
  bool        is_events_file,
  int32_t*    out_fd,
  void**      out_map,
  size_t*     out_size)
{
  bool    exists = nostr_db_file_exists(path);
  int32_t fd;
  size_t  file_size;

  if (exists) {
    // Open existing file
    fd = nostr_db_file_open(path, true);
    if (fd < 0) {
      return NOSTR_DB_ERROR_FILE_OPEN;
    }

    // Get file size
    int64_t size = nostr_db_file_get_size(fd);
    if (size < 0) {
      nostr_db_file_close(fd);
      return NOSTR_DB_ERROR_FSTAT_FAILED;
    }
    file_size = (size_t)size;
  } else {
    // Create new file
    fd = nostr_db_file_create(path, default_size);
    if (fd < 0) {
      return NOSTR_DB_ERROR_FILE_CREATE;
    }
    file_size = default_size;
  }

  // Map the file
  void* map = nostr_db_mmap_file(fd, file_size, true);
  if (is_null(map)) {
    nostr_db_file_close(fd);
    return NOSTR_DB_ERROR_MMAP_FAILED;
  }

  // Initialize header for new file
  if (!exists) {
    if (is_events_file) {
      init_events_header((NostrDBEventsHeader*)map, file_size);
    } else {
      init_index_header((NostrDBIndexHeader*)map, magic, file_size - sizeof(NostrDBIndexHeader));
    }
    // Sync to disk
    nostr_db_msync(map, file_size, false);
  }

  // Validate header
  if (is_events_file) {
    if (!validate_events_header((NostrDBEventsHeader*)map)) {
      nostr_db_munmap(map, file_size);
      nostr_db_file_close(fd);
      return NOSTR_DB_ERROR_INVALID_MAGIC;
    }
  } else {
    if (!validate_index_header((NostrDBIndexHeader*)map, magic)) {
      nostr_db_munmap(map, file_size);
      nostr_db_file_close(fd);
      return NOSTR_DB_ERROR_INVALID_MAGIC;
    }
  }

  *out_fd   = fd;
  *out_map  = map;
  *out_size = file_size;

  return NOSTR_DB_OK;
}

// ============================================================================
// Helper: Close and unmap a file
// ============================================================================
static void close_file(int32_t* fd, void** map, size_t* size)
{
  if (!is_null(*map) && *size > 0) {
    nostr_db_msync(*map, *size, false);
    nostr_db_munmap(*map, *size);
    *map = NULL;
  }

  if (*fd >= 0) {
    nostr_db_file_close(*fd);
    *fd = -1;
  }

  *size = 0;
}

// ============================================================================
// nostr_db_init
// ============================================================================
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(data_dir, NOSTR_DB_ERROR_NULL_PARAM);

  // Allocate DB structure (caller is responsible for providing static memory)
  // For this implementation, we use a static instance
  static NostrDB db_instance;
  NostrDB*       pdb = &db_instance;

  // Initialize all fields
  internal_memset(pdb, 0, sizeof(NostrDB));
  pdb->events_fd          = -1;
  pdb->id_idx_fd          = -1;
  pdb->pubkey_idx_fd      = -1;
  pdb->kind_idx_fd        = -1;
  pdb->pubkey_kind_idx_fd = -1;
  pdb->tag_idx_fd         = -1;
  pdb->timeline_idx_fd    = -1;

  // Copy data directory
  size_t dir_len = strlen(data_dir);
  if (dir_len >= sizeof(pdb->data_dir)) {
    return NOSTR_DB_ERROR_NULL_PARAM;  // Path too long
  }
  internal_memcpy(pdb->data_dir, data_dir, dir_len + 1);

  // Build paths and open files
  char         path[PATH_BUFFER_SIZE];
  NostrDBError err;

  // Events file
  if (!build_path(path, sizeof(path), data_dir, EVENTS_FILE)) {
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_EVENTS,
                            NOSTR_DB_DEFAULT_EVENT_FILE_SIZE, true,
                            &pdb->events_fd, &pdb->events_map, &pdb->events_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->events_header = (NostrDBEventsHeader*)pdb->events_map;

  // ID index
  if (!build_path(path, sizeof(path), data_dir, ID_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_ID,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->id_idx_fd, &pdb->id_idx_map, &pdb->id_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->id_idx_header = (NostrDBIndexHeader*)pdb->id_idx_map;

  // Pubkey index
  if (!build_path(path, sizeof(path), data_dir, PUBKEY_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_PUBKEY,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->pubkey_idx_fd, &pdb->pubkey_idx_map, &pdb->pubkey_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->pubkey_idx_header = (NostrDBIndexHeader*)pdb->pubkey_idx_map;

  // Kind index
  if (!build_path(path, sizeof(path), data_dir, KIND_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_KIND,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->kind_idx_fd, &pdb->kind_idx_map, &pdb->kind_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->kind_idx_header = (NostrDBIndexHeader*)pdb->kind_idx_map;

  // Pubkey+Kind index
  if (!build_path(path, sizeof(path), data_dir, PK_KIND_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_PK_KIND,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->pubkey_kind_idx_fd, &pdb->pubkey_kind_idx_map,
                            &pdb->pubkey_kind_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->pubkey_kind_idx_header = (NostrDBIndexHeader*)pdb->pubkey_kind_idx_map;

  // Tag index
  if (!build_path(path, sizeof(path), data_dir, TAG_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_TAG,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->tag_idx_fd, &pdb->tag_idx_map, &pdb->tag_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->tag_idx_header = (NostrDBIndexHeader*)pdb->tag_idx_map;

  // Timeline index
  if (!build_path(path, sizeof(path), data_dir, TIMELINE_IDX_FILE)) {
    nostr_db_shutdown(pdb);
    return NOSTR_DB_ERROR_NULL_PARAM;
  }
  err = open_or_create_file(path, NOSTR_DB_MAGIC_IDX_TIME,
                            NOSTR_DB_DEFAULT_INDEX_FILE_SIZE, false,
                            &pdb->timeline_idx_fd, &pdb->timeline_idx_map,
                            &pdb->timeline_idx_map_size);
  if (err != NOSTR_DB_OK) {
    nostr_db_shutdown(pdb);
    return err;
  }
  pdb->timeline_idx_header = (NostrDBIndexHeader*)pdb->timeline_idx_map;

  *db = pdb;
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

  // Close all files (sync, unmap, close)
  close_file(&db->events_fd, &db->events_map, &db->events_map_size);
  close_file(&db->id_idx_fd, &db->id_idx_map, &db->id_idx_map_size);
  close_file(&db->pubkey_idx_fd, &db->pubkey_idx_map, &db->pubkey_idx_map_size);
  close_file(&db->kind_idx_fd, &db->kind_idx_map, &db->kind_idx_map_size);
  close_file(&db->pubkey_kind_idx_fd, &db->pubkey_kind_idx_map, &db->pubkey_kind_idx_map_size);
  close_file(&db->tag_idx_fd, &db->tag_idx_map, &db->tag_idx_map_size);
  close_file(&db->timeline_idx_fd, &db->timeline_idx_map, &db->timeline_idx_map_size);

  // Clear header pointers
  db->events_header          = NULL;
  db->id_idx_header          = NULL;
  db->pubkey_idx_header      = NULL;
  db->kind_idx_header        = NULL;
  db->pubkey_kind_idx_header = NULL;
  db->tag_idx_header         = NULL;
  db->timeline_idx_header    = NULL;
}

// ============================================================================
// nostr_db_get_stats
// ============================================================================
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(stats, NOSTR_DB_ERROR_NULL_PARAM);

  stats->event_count            = db->events_header->event_count;
  stats->deleted_count          = db->events_header->deleted_count;
  stats->events_file_size       = db->events_header->file_size;
  stats->id_index_entries       = db->id_idx_header->entry_count;
  stats->pubkey_index_entries   = db->pubkey_idx_header->entry_count;
  stats->kind_index_entries     = db->kind_idx_header->entry_count;
  stats->tag_index_entries      = db->tag_idx_header->entry_count;
  stats->timeline_index_entries = db->timeline_idx_header->entry_count;

  return NOSTR_DB_OK;
}
