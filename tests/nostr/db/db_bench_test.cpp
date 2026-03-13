#include <gtest/gtest.h>

#include <chrono>
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
  NOSTR_DB_ERROR_FSTAT_FAILED     = -12,
  NOSTR_DB_ERROR_FTRUNCATE_FAILED = -13,
} NostrDBError;

typedef struct {
  uint64_t event_count;
  uint64_t deleted_count;
  uint64_t events_file_size;
  uint64_t id_index_entries;
  uint64_t pubkey_index_entries;
  uint64_t kind_index_entries;
  uint64_t tag_index_entries;
  uint64_t timeline_index_entries;
} NostrDBStats;

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

NostrDBError      nostr_db_init(NostrDB** db, const char* data_dir);
void              nostr_db_shutdown(NostrDB* db);
NostrDBError      nostr_db_write_event(NostrDB* db,
                                       const NostrEventEntity* event);
NostrDBError      nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id,
                                           NostrEventEntity* out);
NostrDBError      nostr_db_get_stats(NostrDB* db, NostrDBStats* stats);
void              nostr_db_filter_init(NostrDBFilter* filter);
NostrDBResultSet* nostr_db_result_create(uint32_t capacity);
void              nostr_db_result_free(NostrDBResultSet* result);
NostrDBError      nostr_db_query_execute(NostrDB* db,
                                         const NostrDBFilter* filter,
                                         NostrDBResultSet* result);

}  // extern "C"

class NostrDBBenchTest : public ::testing::Test {
 protected:
  void SetUp() override {
    snprintf(test_dir, sizeof(test_dir), "/tmp/nostr_db_bench_%d", getpid());
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
// 9-4: Insert throughput benchmark
// ============================================================================
TEST_F(NostrDBBenchTest, InsertThroughput) {
  const int COUNT = 1000;

  NostrEventEntity* event = allocate_event();
  memset(event->sig, '0', 128);
  event->sig[128] = '\0';
  strcpy(event->content, "Benchmark event content for throughput test");

  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < COUNT; i++) {
    snprintf(
        event->id, sizeof(event->id),
        "%016x%016x%016x%016x", i, i * 31, i * 37, i * 41);
    snprintf(
        event->pubkey, sizeof(event->pubkey),
        "%016x%016x%016x%016x", i % 100, 0, 0, 0);
    event->kind       = (uint32_t)(i % 10);
    event->created_at = 1704067200 + i;
    event->tag_count  = 0;

    NostrDBError err = nostr_db_write_event(db, event);
    ASSERT_EQ(err, NOSTR_DB_OK) << "Failed at insert " << i;
  }

  auto   end     = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double>(end - start).count();
  double rate    = COUNT / elapsed;

  printf("\n  [BENCH] Insert %d events: %.3f sec (%.0f events/sec)\n",
         COUNT, elapsed, rate);

  NostrDBStats stats;
  nostr_db_get_stats(db, &stats);
  EXPECT_EQ(stats.event_count, (uint64_t)COUNT);

  free_event(event);
}

// ============================================================================
// 9-4: ID search latency benchmark
// ============================================================================
TEST_F(NostrDBBenchTest, IdSearchLatency) {
  const int COUNT = 500;

  // Insert events first
  NostrEventEntity* event = allocate_event();
  memset(event->sig, '0', 128);
  event->sig[128] = '\0';
  strcpy(event->content, "Benchmark");

  for (int i = 0; i < COUNT; i++) {
    snprintf(
        event->id, sizeof(event->id),
        "%016x%016x%016x%016x", i, i * 31, i * 37, i * 41);
    snprintf(
        event->pubkey, sizeof(event->pubkey),
        "%016x%016x%016x%016x", i % 100, 0, 0, 0);
    event->kind       = 1;
    event->created_at = 1704067200 + i;
    event->tag_count  = 0;
    ASSERT_EQ(nostr_db_write_event(db, event), NOSTR_DB_OK);
  }

  // Benchmark lookups
  NostrEventEntity* out = allocate_event();
  auto start = std::chrono::high_resolution_clock::now();

  for (int i = 0; i < COUNT; i++) {
    char id_hex[65];
    snprintf(id_hex, sizeof(id_hex),
             "%016x%016x%016x%016x", i, i * 31, i * 37, i * 41);
    uint8_t id_bytes[32];
    hex_to_bytes(id_hex, id_bytes, 32);
    NostrDBError err = nostr_db_get_event_by_id(db, id_bytes, out);
    ASSERT_EQ(err, NOSTR_DB_OK) << "Failed to find event " << i;
  }

  auto   end        = std::chrono::high_resolution_clock::now();
  double elapsed_us = std::chrono::duration<double, std::micro>(
                          end - start).count();
  double avg_us     = elapsed_us / COUNT;

  printf("\n  [BENCH] ID search %d lookups: %.0f us total (%.1f us/lookup)\n",
         COUNT, elapsed_us, avg_us);

  free_event(event);
  free_event(out);
}

// ============================================================================
// 9-4: Timeline scan (range query) benchmark
// ============================================================================
TEST_F(NostrDBBenchTest, TimelineScanThroughput) {
  const int COUNT = 500;

  // Insert events
  NostrEventEntity* event = allocate_event();
  memset(event->sig, '0', 128);
  event->sig[128] = '\0';
  strcpy(event->content, "Timeline scan benchmark");

  for (int i = 0; i < COUNT; i++) {
    snprintf(
        event->id, sizeof(event->id),
        "%016x%016x%016x%016x", i, i * 31, i * 37, i * 41);
    snprintf(
        event->pubkey, sizeof(event->pubkey),
        "%016x%016x%016x%016x", 1, 0, 0, 0);
    event->kind       = 1;
    event->created_at = 1704067200 + i;
    event->tag_count  = 0;
    ASSERT_EQ(nostr_db_write_event(db, event), NOSTR_DB_OK);
  }

  // Benchmark timeline scan (empty filter = full scan)
  NostrDBFilter filter;
  nostr_db_filter_init(&filter);

  auto start = std::chrono::high_resolution_clock::now();

  NostrDBResultSet* result = nostr_db_result_create(1000);
  ASSERT_NE(result, nullptr);
  NostrDBError err = nostr_db_query_execute(db, &filter, result);
  EXPECT_EQ(err, NOSTR_DB_OK);

  auto   end     = std::chrono::high_resolution_clock::now();
  double elapsed = std::chrono::duration<double, std::milli>(
                       end - start).count();

  printf(
      "\n  [BENCH] Timeline scan %u results: %.3f ms (%.0f results/ms)\n",
      result->count, elapsed,
      elapsed > 0 ? result->count / elapsed : 0);

  EXPECT_EQ(result->count, (uint32_t)COUNT);
  nostr_db_result_free(result);
  free_event(event);
}
