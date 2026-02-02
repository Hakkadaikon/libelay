#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

extern "C" {

typedef struct NostrDB NostrDB;

typedef enum {
  NOSTR_DB_OK                     = 0,
  NOSTR_DB_ERROR_FILE_OPEN        = -1,
  NOSTR_DB_ERROR_FILE_CREATE      = -2,
  NOSTR_DB_ERROR_MMAP_FAILED      = -3,
  NOSTR_DB_ERROR_INVALID_MAGIC    = -4,
  NOSTR_DB_ERROR_VERSION_MISMATCH = -5,
  NOSTR_DB_ERROR_FULL             = -6,
  NOSTR_DB_ERROR_NOT_FOUND        = -7,
  NOSTR_DB_ERROR_DUPLICATE        = -8,
  NOSTR_DB_ERROR_INVALID_EVENT    = -9,
  NOSTR_DB_ERROR_INDEX_CORRUPT    = -10,
  NOSTR_DB_ERROR_NULL_PARAM       = -11,
} NostrDBError;

// Event header (48 bytes)
typedef struct {
  uint32_t total_length;
  uint32_t flags;
  uint8_t  id[32];
  int64_t  created_at;
} NostrDBEventHeader;

// Event body (104 bytes base)
typedef struct {
  uint8_t  pubkey[32];
  uint8_t  sig[64];
  uint32_t kind;
  uint32_t content_length;
} NostrDBEventBody;

// Events file header (64 bytes)
typedef struct {
  char     magic[8];
  uint32_t version;
  uint32_t flags;
  uint64_t event_count;
  uint64_t next_write_offset;
  uint64_t deleted_count;
  uint64_t file_size;
  uint8_t  reserved[16];
} NostrDBEventsHeader;

// Internal DB structure access
struct NostrDB {
  int32_t events_fd;
  int32_t id_idx_fd;
  int32_t pubkey_idx_fd;
  int32_t kind_idx_fd;
  int32_t pubkey_kind_idx_fd;
  int32_t tag_idx_fd;
  int32_t timeline_idx_fd;
  void*   events_map;
  void*   id_idx_map;
  void*   pubkey_idx_map;
  void*   kind_idx_map;
  void*   pubkey_kind_idx_map;
  void*   tag_idx_map;
  void*   timeline_idx_map;
  size_t  events_map_size;
  size_t  id_idx_map_size;
  size_t  pubkey_idx_map_size;
  size_t  kind_idx_map_size;
  size_t  pubkey_kind_idx_map_size;
  size_t  tag_idx_map_size;
  size_t  timeline_idx_map_size;
  NostrDBEventsHeader* events_header;
  void*   id_idx_header;
  void*   pubkey_idx_header;
  void*   kind_idx_header;
  void*   pubkey_kind_idx_header;
  void*   tag_idx_header;
  void*   timeline_idx_header;
  char    data_dir[256];
};

// DB functions
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir);
void         nostr_db_shutdown(NostrDB* db);

// Filter types
#define NOSTR_DB_FILTER_MAX_IDS 256
#define NOSTR_DB_FILTER_MAX_AUTHORS 256
#define NOSTR_DB_FILTER_MAX_KINDS 64
#define NOSTR_DB_FILTER_MAX_TAGS 26
#define NOSTR_DB_FILTER_MAX_TAG_VALUES 256

typedef struct {
  uint8_t value[32];
  size_t  prefix_len;
} NostrDBFilterId;

typedef struct {
  uint8_t value[32];
  size_t  prefix_len;
} NostrDBFilterPubkey;

typedef struct {
  char    name;
  uint8_t values[NOSTR_DB_FILTER_MAX_TAG_VALUES][32];
  size_t  values_count;
} NostrDBFilterTag;

typedef struct {
  NostrDBFilterId     ids[NOSTR_DB_FILTER_MAX_IDS];
  size_t              ids_count;
  NostrDBFilterPubkey authors[NOSTR_DB_FILTER_MAX_AUTHORS];
  size_t              authors_count;
  uint32_t            kinds[NOSTR_DB_FILTER_MAX_KINDS];
  size_t              kinds_count;
  NostrDBFilterTag    tags[NOSTR_DB_FILTER_MAX_TAGS];
  size_t              tags_count;
  int64_t             since;
  int64_t             until;
  uint32_t            limit;
} NostrDBFilter;

typedef struct {
  uint64_t* offsets;
  int64_t*  created_at;
  uint32_t  count;
  uint32_t  capacity;
} NostrDBResultSet;

typedef enum {
  NOSTR_DB_QUERY_STRATEGY_BY_ID = 0,
  NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND,
  NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY,
  NOSTR_DB_QUERY_STRATEGY_BY_KIND,
  NOSTR_DB_QUERY_STRATEGY_BY_TAG,
  NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN,
} NostrDBQueryStrategy;

// Filter functions
void nostr_db_filter_init(NostrDBFilter* filter);
bool nostr_db_filter_validate(const NostrDBFilter* filter);
bool nostr_db_filter_is_empty(const NostrDBFilter* filter);

// Result set functions
NostrDBResultSet* nostr_db_result_create(uint32_t capacity);
int32_t           nostr_db_result_add(NostrDBResultSet* result, uint64_t offset, int64_t created_at);
int32_t           nostr_db_result_sort(NostrDBResultSet* result);
void              nostr_db_result_apply_limit(NostrDBResultSet* result, uint32_t limit);
void              nostr_db_result_free(NostrDBResultSet* result);

// Query functions
NostrDBQueryStrategy nostr_db_query_select_strategy(const NostrDBFilter* filter);
NostrDBError         nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

// Index insert functions (for test setup)
int32_t nostr_db_id_index_insert(NostrDB* db, const uint8_t* id, uint64_t event_offset);
int32_t nostr_db_timeline_index_insert(NostrDB* db, int64_t created_at, uint64_t event_offset);
int32_t nostr_db_kind_index_insert(NostrDB* db, uint32_t kind, uint64_t event_offset, int64_t created_at);
int32_t nostr_db_pubkey_index_insert(NostrDB* db, const uint8_t* pubkey, uint64_t event_offset, int64_t created_at);
int32_t nostr_db_pubkey_kind_index_insert(NostrDB* db, const uint8_t* pubkey, uint32_t kind, uint64_t event_offset, int64_t created_at);
int32_t nostr_db_tag_index_insert(NostrDB* db, uint8_t tag_name, const uint8_t* tag_value, uint64_t event_offset, int64_t created_at);

}  // extern "C"

class NostrDBQueryTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_dir, sizeof(test_dir), "/tmp/nostr_db_query_test_%d", getpid());
    mkdir(test_dir, 0755);
    db = nullptr;
    NostrDBError err = nostr_db_init(&db, test_dir);
    ASSERT_EQ(err, NOSTR_DB_OK);
  }

  void TearDown() override {
    if (db != nullptr) {
      nostr_db_shutdown(db);
      db = nullptr;
    }
    cleanup_directory(test_dir);
  }

  void cleanup_directory(const char* path) {
    DIR* dir = opendir(path);
    if (dir == nullptr) return;

    struct dirent* entry;
    char           filepath[512];

    while ((entry = readdir(dir)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) continue;
      snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);
      unlink(filepath);
    }

    closedir(dir);
    rmdir(path);
  }

  void make_id(uint8_t* id, uint64_t value) {
    memset(id, 0, 32);
    for (int i = 0; i < 8; i++) {
      id[i] = (uint8_t)(value >> (i * 8));
    }
  }

  // Write a minimal event at a specific offset and return that offset
  // This creates valid event data that can be read by query functions
  uint64_t write_test_event(const uint8_t* id, const uint8_t* pubkey, uint32_t kind, int64_t created_at) {
    // Get current write offset
    uint64_t offset = db->events_header->next_write_offset;

    // Calculate record size (header + body + minimal content + tags)
    size_t record_size = sizeof(NostrDBEventHeader) + sizeof(NostrDBEventBody) + 4;  // 4 = tags_length(4) with 0 tags
    record_size = (record_size + 7) & ~7;  // Align to 8 bytes

    // Write header
    uint8_t* write_ptr = (uint8_t*)db->events_map + offset;
    NostrDBEventHeader* header = (NostrDBEventHeader*)write_ptr;
    header->total_length = (uint32_t)record_size;
    header->flags = 0;
    memcpy(header->id, id, 32);
    header->created_at = created_at;
    write_ptr += sizeof(NostrDBEventHeader);

    // Write body
    NostrDBEventBody* body = (NostrDBEventBody*)write_ptr;
    memcpy(body->pubkey, pubkey, 32);
    memset(body->sig, 0, 64);
    body->kind = kind;
    body->content_length = 0;
    write_ptr += sizeof(NostrDBEventBody);

    // Write tags length (0 tags)
    uint32_t tags_len = 2;  // Just tag count (0) as uint16
    memcpy(write_ptr, &tags_len, sizeof(uint32_t));

    // Update next write offset
    db->events_header->next_write_offset = offset + record_size;
    db->events_header->event_count++;

    return offset;
  }

  char     test_dir[256];
  NostrDB* db;
};

// ============================================================================
// Result Set Tests
// ============================================================================

TEST_F(NostrDBQueryTest, ResultSetCreate) {
  NostrDBResultSet* result = nostr_db_result_create(100);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->count, 0u);
  EXPECT_EQ(result->capacity, 100u);
  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetCreateDefault) {
  NostrDBResultSet* result = nostr_db_result_create(0);  // Should use default
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->count, 0u);
  EXPECT_GT(result->capacity, 0u);
  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetAdd) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  int32_t ret = nostr_db_result_add(result, 100, 1000);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(result->count, 1u);
  EXPECT_EQ(result->offsets[0], 100u);
  EXPECT_EQ(result->created_at[0], 1000);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetAddDuplicate) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  nostr_db_result_add(result, 100, 1000);
  int32_t ret = nostr_db_result_add(result, 100, 1000);  // Duplicate
  EXPECT_EQ(ret, 1);  // Returns 1 for duplicate
  EXPECT_EQ(result->count, 1u);  // Still only 1 entry

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetSort) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  nostr_db_result_add(result, 100, 1000);  // Oldest
  nostr_db_result_add(result, 200, 3000);  // Newest
  nostr_db_result_add(result, 300, 2000);  // Middle

  nostr_db_result_sort(result);

  // Should be sorted descending (newest first)
  EXPECT_EQ(result->offsets[0], 200u);  // 3000
  EXPECT_EQ(result->offsets[1], 300u);  // 2000
  EXPECT_EQ(result->offsets[2], 100u);  // 1000

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetApplyLimit) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  nostr_db_result_add(result, 100, 1000);
  nostr_db_result_add(result, 200, 2000);
  nostr_db_result_add(result, 300, 3000);

  nostr_db_result_apply_limit(result, 2);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetGrow) {
  NostrDBResultSet* result = nostr_db_result_create(2);
  ASSERT_NE(result, nullptr);

  // Add more than capacity
  for (int i = 0; i < 10; i++) {
    int32_t ret = nostr_db_result_add(result, (uint64_t)(i * 100), (int64_t)(i * 1000));
    EXPECT_EQ(ret, 0);
  }

  EXPECT_EQ(result->count, 10u);
  EXPECT_GE(result->capacity, 10u);

  nostr_db_result_free(result);
}

// ============================================================================
// Filter Tests
// ============================================================================

TEST_F(NostrDBQueryTest, FilterInit) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  EXPECT_EQ(filter.ids_count, 0u);
  EXPECT_EQ(filter.authors_count, 0u);
  EXPECT_EQ(filter.kinds_count, 0u);
  EXPECT_EQ(filter.tags_count, 0u);
  EXPECT_EQ(filter.since, 0);
  EXPECT_EQ(filter.until, 0);
  EXPECT_EQ(filter.limit, 0u);
}

TEST_F(NostrDBQueryTest, FilterIsEmpty) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  EXPECT_TRUE(nostr_db_filter_is_empty(&filter));

  filter.kinds_count = 1;
  filter.kinds[0] = 1;
  EXPECT_FALSE(nostr_db_filter_is_empty(&filter));
}

TEST_F(NostrDBQueryTest, FilterValidate) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  EXPECT_TRUE(nostr_db_filter_validate(&filter));

  // Invalid: since > until
  filter.since = 2000;
  filter.until = 1000;
  EXPECT_FALSE(nostr_db_filter_validate(&filter));
}

// ============================================================================
// Query Strategy Tests
// ============================================================================

TEST_F(NostrDBQueryTest, QueryStrategySelectById) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  uint8_t id[32];
  make_id(id, 123);
  memcpy(filter.ids[0].value, id, 32);
  filter.ids_count = 1;

  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_BY_ID);
}

TEST_F(NostrDBQueryTest, QueryStrategySelectByTag) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  filter.tags[0].name = 'e';
  memset(filter.tags[0].values[0], 0xAA, 32);
  filter.tags[0].values_count = 1;
  filter.tags_count = 1;

  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_BY_TAG);
}

TEST_F(NostrDBQueryTest, QueryStrategySelectByPubkeyKind) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  memset(filter.authors[0].value, 0x01, 32);
  filter.authors_count = 1;
  filter.kinds[0] = 1;
  filter.kinds_count = 1;

  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND);
}

TEST_F(NostrDBQueryTest, QueryStrategySelectByPubkey) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  memset(filter.authors[0].value, 0x01, 32);
  filter.authors_count = 1;

  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY);
}

TEST_F(NostrDBQueryTest, QueryStrategySelectByKind) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  filter.kinds[0] = 1;
  filter.kinds_count = 1;

  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_BY_KIND);
}

TEST_F(NostrDBQueryTest, QueryStrategySelectTimelineScan) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  // Empty filter should use timeline scan
  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(&filter);
  EXPECT_EQ(strategy, NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN);
}

// ============================================================================
// Query Execution Tests
// ============================================================================

TEST_F(NostrDBQueryTest, QueryByIdFound) {
  // Create test event data
  uint8_t id[32];
  uint8_t pubkey[32];
  make_id(id, 12345);
  memset(pubkey, 0x01, 32);

  // Write actual event and get its offset
  uint64_t offset = write_test_event(id, pubkey, 1, 1000);

  // Insert into ID index
  nostr_db_id_index_insert(db, id, offset);

  // Create filter
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  memcpy(filter.ids[0].value, id, 32);
  filter.ids_count = 1;

  // Execute query
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 1u);
  EXPECT_EQ(result->offsets[0], offset);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByIdNotFound) {
  uint8_t id[32];
  make_id(id, 99999);

  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  memcpy(filter.ids[0].value, id, 32);
  filter.ids_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 0u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByKind) {
  // Create and write test events
  uint8_t id1[32], id2[32], id3[32];
  uint8_t pubkey[32];
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);
  memset(pubkey, 0x01, 32);

  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 3, 3000);  // Different kind

  // Insert into kind index
  nostr_db_kind_index_insert(db, 1, offset1, 1000);
  nostr_db_kind_index_insert(db, 1, offset2, 2000);
  nostr_db_kind_index_insert(db, 3, offset3, 3000);

  // Create filter for kind 1
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.kinds[0] = 1;
  filter.kinds_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByPubkey) {
  uint8_t pubkey1[32], pubkey2[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey1, 0x01, 32);
  memset(pubkey2, 0x02, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey1, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey1, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey2, 1, 3000);

  // Insert into pubkey index
  nostr_db_pubkey_index_insert(db, pubkey1, offset1, 1000);
  nostr_db_pubkey_index_insert(db, pubkey1, offset2, 2000);
  nostr_db_pubkey_index_insert(db, pubkey2, offset3, 3000);

  // Create filter for pubkey1
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  memcpy(filter.authors[0].value, pubkey1, 32);
  filter.authors_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByPubkeyKind) {
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 3, 3000);  // Different kind

  // Insert into pubkey_kind index
  nostr_db_pubkey_kind_index_insert(db, pubkey, 1, offset1, 1000);
  nostr_db_pubkey_kind_index_insert(db, pubkey, 1, offset2, 2000);
  nostr_db_pubkey_kind_index_insert(db, pubkey, 3, offset3, 3000);

  // Create filter for pubkey + kind 1
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  memcpy(filter.authors[0].value, pubkey, 32);
  filter.authors_count = 1;
  filter.kinds[0] = 1;
  filter.kinds_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByTag) {
  uint8_t tag_value[32];
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32];
  memset(tag_value, 0xAA, 32);
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);

  // Insert into tag index
  nostr_db_tag_index_insert(db, 'e', tag_value, offset1, 1000);
  nostr_db_tag_index_insert(db, 'e', tag_value, offset2, 2000);

  // Create filter for #e tag
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.tags[0].name = 'e';
  memcpy(filter.tags[0].values[0], tag_value, 32);
  filter.tags[0].values_count = 1;
  filter.tags_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryTimelineScan) {
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 1, 3000);

  // Insert into timeline index
  nostr_db_timeline_index_insert(db, 1000, offset1);
  nostr_db_timeline_index_insert(db, 2000, offset2);
  nostr_db_timeline_index_insert(db, 3000, offset3);

  // Create empty filter (will use timeline scan)
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 3u);

  // Should be sorted by created_at descending
  EXPECT_EQ(result->offsets[0], offset3);  // Newest
  EXPECT_EQ(result->offsets[1], offset2);
  EXPECT_EQ(result->offsets[2], offset1);  // Oldest

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryWithLimit) {
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 1, 3000);

  // Insert into timeline index
  nostr_db_timeline_index_insert(db, 1000, offset1);
  nostr_db_timeline_index_insert(db, 2000, offset2);
  nostr_db_timeline_index_insert(db, 3000, offset3);

  // Create filter with limit
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.limit = 2;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryWithSince) {
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 1, 3000);

  // Insert into timeline index
  nostr_db_timeline_index_insert(db, 1000, offset1);
  nostr_db_timeline_index_insert(db, 2000, offset2);
  nostr_db_timeline_index_insert(db, 3000, offset3);

  // Create filter with since
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.since = 1500;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);  // Only 2000 and 3000

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryWithUntil) {
  uint8_t pubkey[32];
  uint8_t id1[32], id2[32], id3[32];
  memset(pubkey, 0x01, 32);
  make_id(id1, 1);
  make_id(id2, 2);
  make_id(id3, 3);

  // Write test events
  uint64_t offset1 = write_test_event(id1, pubkey, 1, 1000);
  uint64_t offset2 = write_test_event(id2, pubkey, 1, 2000);
  uint64_t offset3 = write_test_event(id3, pubkey, 1, 3000);

  // Insert into timeline index
  nostr_db_timeline_index_insert(db, 1000, offset1);
  nostr_db_timeline_index_insert(db, 2000, offset2);
  nostr_db_timeline_index_insert(db, 3000, offset3);

  // Create filter with until
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.until = 2500;

  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);  // Only 1000 and 2000

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryNullParams) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  NostrDBResultSet* result = nostr_db_result_create(10);

  // Null db
  EXPECT_EQ(nostr_db_query_execute(nullptr, &filter, result), NOSTR_DB_ERROR_NULL_PARAM);

  // Null filter
  EXPECT_EQ(nostr_db_query_execute(db, nullptr, result), NOSTR_DB_ERROR_NULL_PARAM);

  // Null result
  EXPECT_EQ(nostr_db_query_execute(db, &filter, nullptr), NOSTR_DB_ERROR_NULL_PARAM);

  nostr_db_result_free(result);
}
