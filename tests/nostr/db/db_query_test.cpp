#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

// Use smaller sizes for testing
#define NOSTR_EVENT_TAG_VALUE_COUNT 16
#define NOSTR_EVENT_TAG_VALUE_LENGTH 512
#define NOSTR_EVENT_TAG_LENGTH (2 * 1024)
#define NOSTR_EVENT_CONTENT_LENGTH (1 * 1024 * 1024)

extern "C" {

typedef struct {
  char   key[64];
  char   values[NOSTR_EVENT_TAG_VALUE_COUNT][NOSTR_EVENT_TAG_VALUE_LENGTH];
  size_t item_count;
} NostrTagEntity;

typedef struct {
  char           id[65];
  char           dummy1[7];
  char           pubkey[65];
  char           dummy2[7];
  uint32_t       kind;
  uint32_t       tag_count;
  int64_t        created_at;
  NostrTagEntity tags[NOSTR_EVENT_TAG_LENGTH];
  char           content[NOSTR_EVENT_CONTENT_LENGTH];
  char           sig[129];
  char           dummy3[7];
} NostrEventEntity;

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

// DB functions
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir);
void         nostr_db_shutdown(NostrDB* db);
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event);

// Filter functions
void                 nostr_db_filter_init(NostrDBFilter* filter);
bool                 nostr_db_filter_validate(const NostrDBFilter* filter);
bool                 nostr_db_filter_is_empty(const NostrDBFilter* filter);
NostrDBQueryStrategy nostr_db_query_select_strategy(const NostrDBFilter* f);

// Result set functions
NostrDBResultSet* nostr_db_result_create(uint32_t capacity);
int32_t           nostr_db_result_add(NostrDBResultSet* r, uint64_t offset,
                                      int64_t created_at);
int32_t           nostr_db_result_sort(NostrDBResultSet* r);
void              nostr_db_result_apply_limit(NostrDBResultSet* r, uint32_t l);
void              nostr_db_result_free(NostrDBResultSet* r);

// Query execution
NostrDBError nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter,
                                    NostrDBResultSet* result);

}  // extern "C"

class NostrDBQueryTest : public ::testing::Test {
 protected:
  void SetUp() override {
    snprintf(test_dir, sizeof(test_dir), "/tmp/nostr_db_query_test_%d",
             getpid());
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
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        continue;
      snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);
      unlink(filepath);
    }
    closedir(dir);
    rmdir(path);
  }

  NostrEventEntity* allocate_event() {
    NostrEventEntity* e =
        (NostrEventEntity*)malloc(sizeof(NostrEventEntity));
    memset(e, 0, sizeof(NostrEventEntity));
    return e;
  }

  void free_event(NostrEventEntity* e) { free(e); }

  // Write a test event with specified fields
  void write_event(const char* id_suffix, const char* pk_suffix,
                   uint32_t kind, int64_t created_at) {
    NostrEventEntity* e = allocate_event();
    // Build 64-char hex id with suffix
    snprintf(e->id, sizeof(e->id),
             "00000000000000000000000000000000000000000000000000000000%s",
             id_suffix);
    snprintf(e->pubkey, sizeof(e->pubkey),
             "00000000000000000000000000000000000000000000000000000000%s",
             pk_suffix);
    memset(e->sig, '0', 128);
    e->sig[128]   = '\0';
    e->kind       = kind;
    e->created_at = created_at;
    strcpy(e->content, "test");
    NostrDBError err = nostr_db_write_event(db, e);
    ASSERT_EQ(err, NOSTR_DB_OK);
    free_event(e);
  }

  void hex_to_bytes(const char* hex, uint8_t* bytes, size_t len) {
    for (size_t i = 0; i < len; i++) {
      unsigned int val;
      sscanf(hex + i * 2, "%2x", &val);
      bytes[i] = (uint8_t)val;
    }
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

TEST_F(NostrDBQueryTest, ResultSetAddAndSort) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  ASSERT_NE(result, nullptr);

  nostr_db_result_add(result, 100, 1000);
  nostr_db_result_add(result, 200, 3000);
  nostr_db_result_add(result, 300, 2000);

  nostr_db_result_sort(result);

  EXPECT_EQ(result->created_at[0], 3000);
  EXPECT_EQ(result->created_at[1], 2000);
  EXPECT_EQ(result->created_at[2], 1000);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetApplyLimit) {
  NostrDBResultSet* result = nostr_db_result_create(10);
  nostr_db_result_add(result, 100, 1000);
  nostr_db_result_add(result, 200, 2000);
  nostr_db_result_add(result, 300, 3000);

  nostr_db_result_apply_limit(result, 2);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, ResultSetGrow) {
  NostrDBResultSet* result = nostr_db_result_create(2);
  for (int i = 0; i < 10; i++) {
    int32_t ret =
        nostr_db_result_add(result, (uint64_t)(i * 100), (int64_t)(i * 1000));
    EXPECT_EQ(ret, 0);
  }
  EXPECT_EQ(result->count, 10u);
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
}

TEST_F(NostrDBQueryTest, FilterValidate) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  EXPECT_TRUE(nostr_db_filter_validate(&filter));

  filter.since = 2000;
  filter.until = 1000;
  EXPECT_FALSE(nostr_db_filter_validate(&filter));
}

TEST_F(NostrDBQueryTest, FilterIsEmpty) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  EXPECT_TRUE(nostr_db_filter_is_empty(&filter));

  filter.kinds_count = 1;
  EXPECT_FALSE(nostr_db_filter_is_empty(&filter));
}

// ============================================================================
// Strategy Selection Tests
// ============================================================================

TEST_F(NostrDBQueryTest, StrategyById) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.ids_count = 1;
  EXPECT_EQ(nostr_db_query_select_strategy(&filter),
            NOSTR_DB_QUERY_STRATEGY_BY_ID);
}

TEST_F(NostrDBQueryTest, StrategyByTag) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.tags_count = 1;
  EXPECT_EQ(nostr_db_query_select_strategy(&filter),
            NOSTR_DB_QUERY_STRATEGY_BY_TAG);
}

TEST_F(NostrDBQueryTest, StrategyByPubkeyKind) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.authors_count = 1;
  filter.kinds_count   = 1;
  EXPECT_EQ(nostr_db_query_select_strategy(&filter),
            NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND);
}

TEST_F(NostrDBQueryTest, StrategyTimeline) {
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  EXPECT_EQ(nostr_db_query_select_strategy(&filter),
            NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN);
}

// ============================================================================
// Integrated Query Execution Tests
// ============================================================================

TEST_F(NostrDBQueryTest, QueryByIdFound) {
  write_event("00000001", "00000010", 1, 1000);

  // Build filter with the event's ID
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  char id_hex[] =
      "0000000000000000000000000000000000000000000000000000000000000001";
  hex_to_bytes(id_hex, filter.ids[0].value, 32);
  filter.ids_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  NostrDBError      err    = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 1u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByKind) {
  write_event("00000001", "00000010", 1, 1000);
  write_event("00000002", "00000010", 1, 2000);
  write_event("00000003", "00000010", 3, 3000);

  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.kinds[0]    = 1;
  filter.kinds_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  NostrDBError      err    = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryByPubkey) {
  write_event("00000001", "00000010", 1, 1000);
  write_event("00000002", "00000010", 1, 2000);
  write_event("00000003", "00000020", 1, 3000);

  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  char pk_hex[] =
      "0000000000000000000000000000000000000000000000000000000000000010";
  hex_to_bytes(pk_hex, filter.authors[0].value, 32);
  filter.authors_count = 1;

  NostrDBResultSet* result = nostr_db_result_create(10);
  NostrDBError      err    = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryTimelineScan) {
  write_event("00000001", "00000010", 1, 1000);
  write_event("00000002", "00000010", 1, 2000);
  write_event("00000003", "00000010", 1, 3000);

  // Empty filter -> timeline scan
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  NostrDBResultSet* result = nostr_db_result_create(10);
  NostrDBError      err    = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 3u);

  // Results should be sorted descending by created_at
  EXPECT_EQ(result->created_at[0], 3000);
  EXPECT_EQ(result->created_at[1], 2000);
  EXPECT_EQ(result->created_at[2], 1000);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryWithLimit) {
  write_event("00000001", "00000010", 1, 1000);
  write_event("00000002", "00000010", 1, 2000);
  write_event("00000003", "00000010", 1, 3000);

  NostrDBFilter filter;
  nostr_db_filter_init(&filter);
  filter.limit = 2;

  NostrDBResultSet* result = nostr_db_result_create(10);
  NostrDBError      err    = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(result->count, 2u);

  nostr_db_result_free(result);
}

TEST_F(NostrDBQueryTest, QueryNullParams) {
  NostrDBFilter     filter;
  NostrDBResultSet* result = nostr_db_result_create(10);

  EXPECT_EQ(nostr_db_query_execute(nullptr, &filter, result),
            NOSTR_DB_ERROR_NULL_PARAM);
  EXPECT_EQ(nostr_db_query_execute(db, nullptr, result),
            NOSTR_DB_ERROR_NULL_PARAM);
  EXPECT_EQ(nostr_db_query_execute(db, &filter, nullptr),
            NOSTR_DB_ERROR_NULL_PARAM);

  nostr_db_result_free(result);
}
