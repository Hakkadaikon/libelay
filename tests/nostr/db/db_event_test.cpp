#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

// Use smaller sizes for testing to avoid stack overflow
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
  int64_t        created_at;  // time_t
  NostrTagEntity tags[NOSTR_EVENT_TAG_LENGTH];
  char           content[NOSTR_EVENT_CONTENT_LENGTH];
  char           sig[129];
  char           dummy3[7];
} NostrEventEntity;

typedef struct NostrDB NostrDB;

typedef enum {
  NOSTR_DB_OK = 0,
  NOSTR_DB_ERROR_FILE_OPEN = -1,
  NOSTR_DB_ERROR_FILE_CREATE = -2,
  NOSTR_DB_ERROR_MMAP_FAILED = -3,
  NOSTR_DB_ERROR_INVALID_MAGIC = -4,
  NOSTR_DB_ERROR_VERSION_MISMATCH = -5,
  NOSTR_DB_ERROR_FULL = -6,
  NOSTR_DB_ERROR_NOT_FOUND = -7,
  NOSTR_DB_ERROR_DUPLICATE = -8,
  NOSTR_DB_ERROR_INVALID_EVENT = -9,
  NOSTR_DB_ERROR_INDEX_CORRUPT = -10,
  NOSTR_DB_ERROR_NULL_PARAM = -11,
  NOSTR_DB_ERROR_FSTAT_FAILED = -12,
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

// DB functions
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir);
void nostr_db_shutdown(NostrDB* db);
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event);
NostrDBError nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id, NostrEventEntity* out);
NostrDBError nostr_db_delete_event(NostrDB* db, const uint8_t* id);
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats);

}  // extern "C"

class NostrDBEventTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_dir, sizeof(test_dir), "/tmp/nostr_db_event_test_%d", getpid());
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
    if (dir == nullptr) {
      return;
    }

    struct dirent* entry;
    char filepath[512];

    while ((entry = readdir(dir)) != nullptr) {
      if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
        continue;
      }
      snprintf(filepath, sizeof(filepath), "%s/%s", path, entry->d_name);
      unlink(filepath);
    }

    closedir(dir);
    rmdir(path);
  }

  NostrEventEntity* allocate_event() {
    NostrEventEntity* event = (NostrEventEntity*)malloc(sizeof(NostrEventEntity));
    EXPECT_NE(event, nullptr);
    memset(event, 0, sizeof(NostrEventEntity));
    return event;
  }

  void free_event(NostrEventEntity* event) {
    free(event);
  }

  void create_sample_event(NostrEventEntity* event) {
    memset(event, 0, sizeof(NostrEventEntity));
    // Valid 64-char hex ID (all zeros for simplicity)
    strcpy(event->id, "0000000000000000000000000000000000000000000000000000000000000001");
    // Valid 64-char hex pubkey
    strcpy(event->pubkey, "0000000000000000000000000000000000000000000000000000000000000002");
    // Valid 128-char hex sig
    memset(event->sig, '0', 128);
    event->sig[128] = '\0';
    event->kind = 1;
    event->created_at = 1704067200;  // Some timestamp
    strcpy(event->content, "Hello, Nostr!");
    event->tag_count = 0;
  }

  void hex_to_bytes(const char* hex, uint8_t* bytes, size_t len) {
    for (size_t i = 0; i < len; i++) {
      unsigned int val;
      sscanf(hex + i * 2, "%2x", &val);
      bytes[i] = (uint8_t)val;
    }
  }

  char test_dir[256];
  NostrDB* db;
};

TEST_F(NostrDBEventTest, WriteEventSuccess) {
  NostrEventEntity* event = allocate_event();
  create_sample_event(event);

  NostrDBError err = nostr_db_write_event(db, event);
  EXPECT_EQ(err, NOSTR_DB_OK);

  NostrDBStats stats;
  nostr_db_get_stats(db, &stats);
  EXPECT_EQ(stats.event_count, 1u);

  free_event(event);
}

TEST_F(NostrDBEventTest, WriteAndReadEvent) {
  NostrEventEntity* event = allocate_event();
  create_sample_event(event);
  strcpy(event->content, "Test content 123");

  NostrDBError err = nostr_db_write_event(db, event);
  ASSERT_EQ(err, NOSTR_DB_OK);

  // Convert ID to bytes for lookup
  uint8_t id_bytes[32];
  hex_to_bytes(event->id, id_bytes, 32);

  // Read back
  NostrEventEntity* out = allocate_event();
  err = nostr_db_get_event_by_id(db, id_bytes, out);
  ASSERT_EQ(err, NOSTR_DB_OK);

  EXPECT_STREQ(out->id, event->id);
  EXPECT_STREQ(out->pubkey, event->pubkey);
  EXPECT_EQ(out->kind, event->kind);
  EXPECT_EQ(out->created_at, event->created_at);
  EXPECT_STREQ(out->content, "Test content 123");

  free_event(event);
  free_event(out);
}

TEST_F(NostrDBEventTest, WriteMultipleEvents) {
  NostrEventEntity* event = allocate_event();

  for (int i = 0; i < 5; i++) {
    create_sample_event(event);
    // Make each ID unique
    snprintf(event->id, sizeof(event->id),
             "00000000000000000000000000000000000000000000000000000000000000%02x", i);
    snprintf(event->content, sizeof(event->content), "Event %d", i);

    NostrDBError err = nostr_db_write_event(db, event);
    EXPECT_EQ(err, NOSTR_DB_OK);
  }

  NostrDBStats stats;
  nostr_db_get_stats(db, &stats);
  EXPECT_EQ(stats.event_count, 5u);

  free_event(event);
}

TEST_F(NostrDBEventTest, GetNonExistentEventReturnsNotFound) {
  uint8_t fake_id[32] = {0xFF};

  NostrEventEntity* out = allocate_event();
  NostrDBError err = nostr_db_get_event_by_id(db, fake_id, out);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NOT_FOUND);

  free_event(out);
}

TEST_F(NostrDBEventTest, DeleteEvent) {
  NostrEventEntity* event = allocate_event();
  create_sample_event(event);

  NostrDBError err = nostr_db_write_event(db, event);
  ASSERT_EQ(err, NOSTR_DB_OK);

  // Convert ID to bytes
  uint8_t id_bytes[32];
  hex_to_bytes(event->id, id_bytes, 32);

  // Delete the event
  err = nostr_db_delete_event(db, id_bytes);
  EXPECT_EQ(err, NOSTR_DB_OK);

  // Verify deleted
  NostrDBStats stats;
  nostr_db_get_stats(db, &stats);
  EXPECT_EQ(stats.deleted_count, 1u);

  // Should not be found anymore
  NostrEventEntity* out = allocate_event();
  err = nostr_db_get_event_by_id(db, id_bytes, out);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NOT_FOUND);

  free_event(event);
  free_event(out);
}

TEST_F(NostrDBEventTest, DeleteNonExistentEventReturnsNotFound) {
  uint8_t fake_id[32] = {0xFF};

  NostrDBError err = nostr_db_delete_event(db, fake_id);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NOT_FOUND);
}

TEST_F(NostrDBEventTest, WriteEventNullDbReturnsError) {
  NostrEventEntity* event = allocate_event();
  create_sample_event(event);

  NostrDBError err = nostr_db_write_event(nullptr, event);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);

  free_event(event);
}

TEST_F(NostrDBEventTest, WriteEventNullEventReturnsError) {
  NostrDBError err = nostr_db_write_event(db, nullptr);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);
}

TEST_F(NostrDBEventTest, WriteEventWithTags) {
  NostrEventEntity* event = allocate_event();
  create_sample_event(event);

  // Add tags
  strcpy(event->tags[0].key, "e");
  strcpy(event->tags[0].values[0], "0000000000000000000000000000000000000000000000000000000000000003");
  event->tags[0].item_count = 1;

  strcpy(event->tags[1].key, "p");
  strcpy(event->tags[1].values[0], "0000000000000000000000000000000000000000000000000000000000000004");
  strcpy(event->tags[1].values[1], "wss://relay.example.com");
  event->tags[1].item_count = 2;

  event->tag_count = 2;

  NostrDBError err = nostr_db_write_event(db, event);
  ASSERT_EQ(err, NOSTR_DB_OK);

  // Read back
  uint8_t id_bytes[32];
  hex_to_bytes(event->id, id_bytes, 32);

  NostrEventEntity* out = allocate_event();
  err = nostr_db_get_event_by_id(db, id_bytes, out);
  ASSERT_EQ(err, NOSTR_DB_OK);

  EXPECT_EQ(out->tag_count, 2u);
  EXPECT_STREQ(out->tags[0].key, "e");
  EXPECT_EQ(out->tags[0].item_count, 1u);
  EXPECT_STREQ(out->tags[1].key, "p");
  EXPECT_EQ(out->tags[1].item_count, 2u);

  free_event(event);
  free_event(out);
}
