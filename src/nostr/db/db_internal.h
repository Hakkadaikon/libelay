#ifndef NOSTR_DB_INTERNAL_H_
#define NOSTR_DB_INTERNAL_H_

#include "../../util/types.h"
#include "../nostr_types.h"
#include "buffer/buffer_pool.h"
#include "db_types.h"
#include "disk/disk_manager.h"
#include "index/index_manager.h"
#include "wal/wal_manager.h"

// ============================================================================
// DB metadata page (stored at page 1)
// ============================================================================
#define DB_META_PAGE_ID ((page_id_t)1)
#define DB_META_MAGIC "NDBMETA\0"
#define DB_META_MAGIC_SIZE 8

typedef struct {
  char      magic[8];               // "NDBMETA\0"
  uint32_t  version;                // DB_FILE_VERSION (2)
  uint32_t  reserved0;

  // Event counters
  uint64_t  event_count;
  uint64_t  deleted_count;

  // Index meta page IDs
  page_id_t id_index_meta;
  page_id_t timeline_index_meta;
  page_id_t pubkey_index_meta;
  page_id_t kind_index_meta;
  page_id_t pk_kind_index_meta;
  page_id_t tag_index_meta;

  // Record management
  page_id_t first_record_page;
  page_id_t last_record_page;

  uint8_t   reserved[DB_PAGE_SIZE - 64];
} DBMetaPage;

_Static_assert(sizeof(DBMetaPage) == DB_PAGE_SIZE, "DBMetaPage must be one page");

// ============================================================================
// NostrDB internal structure (new B+ tree based architecture)
// ============================================================================
struct NostrDB {
  DiskManager    disk;
  BufferPool     buffer_pool;
  WalManager     wal;
  IndexManager   indexes;

  // Cached metadata from DBMetaPage
  uint64_t       event_count;
  uint64_t       deleted_count;

  // Data directory path
  char           data_dir[256];

  // Initialization flag
  bool           initialized;
};

// ============================================================================
// Tag serialization functions (defined in db_tags.c)
// ============================================================================
int64_t nostr_db_serialize_tags(
  const NostrTagEntity* tags,
  uint32_t              tag_count,
  uint8_t*              buffer,
  size_t                capacity);

int32_t nostr_db_deserialize_tags(
  const uint8_t*  buffer,
  size_t          length,
  NostrTagEntity* tags,
  uint32_t        max_tags);

#endif
