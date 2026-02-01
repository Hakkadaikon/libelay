#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>

extern "C" {

// Forward declarations of DB structures and functions
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
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats);

}  // extern "C"

class NostrDBInitTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_dir, sizeof(test_dir), "/tmp/nostr_db_test_%d", getpid());
    // Create test directory
    mkdir(test_dir, 0755);
    db = nullptr;
  }

  void TearDown() override {
    if (db != nullptr) {
      nostr_db_shutdown(db);
      db = nullptr;
    }
    // Clean up test directory and files
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

  char test_dir[256];
  NostrDB* db;
};

TEST_F(NostrDBInitTest, InitCreatesFiles) {
  NostrDBError err = nostr_db_init(&db, test_dir);
  ASSERT_EQ(err, NOSTR_DB_OK);
  ASSERT_NE(db, nullptr);

  // Check that files were created
  char filepath[512];

  snprintf(filepath, sizeof(filepath), "%s/events.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_id.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_pubkey.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_kind.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_pubkey_kind.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_tag.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);

  snprintf(filepath, sizeof(filepath), "%s/idx_timeline.dat", test_dir);
  EXPECT_EQ(access(filepath, F_OK), 0);
}

TEST_F(NostrDBInitTest, InitTwiceReopensFiles) {
  // First init
  NostrDBError err = nostr_db_init(&db, test_dir);
  ASSERT_EQ(err, NOSTR_DB_OK);

  nostr_db_shutdown(db);
  db = nullptr;

  // Second init should succeed (reopening existing files)
  err = nostr_db_init(&db, test_dir);
  ASSERT_EQ(err, NOSTR_DB_OK);
  ASSERT_NE(db, nullptr);
}

TEST_F(NostrDBInitTest, ShutdownNullIsNoOp) {
  // Should not crash
  nostr_db_shutdown(nullptr);
}

TEST_F(NostrDBInitTest, GetStatsInitialValues) {
  NostrDBError err = nostr_db_init(&db, test_dir);
  ASSERT_EQ(err, NOSTR_DB_OK);

  NostrDBStats stats;
  err = nostr_db_get_stats(db, &stats);
  ASSERT_EQ(err, NOSTR_DB_OK);

  EXPECT_EQ(stats.event_count, 0u);
  EXPECT_EQ(stats.deleted_count, 0u);
  EXPECT_EQ(stats.id_index_entries, 0u);
  EXPECT_EQ(stats.pubkey_index_entries, 0u);
  EXPECT_EQ(stats.kind_index_entries, 0u);
  EXPECT_EQ(stats.tag_index_entries, 0u);
  EXPECT_EQ(stats.timeline_index_entries, 0u);
}

TEST_F(NostrDBInitTest, InitNullDbReturnsError) {
  NostrDBError err = nostr_db_init(nullptr, test_dir);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);
}

TEST_F(NostrDBInitTest, InitNullPathReturnsError) {
  NostrDBError err = nostr_db_init(&db, nullptr);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);
}

TEST_F(NostrDBInitTest, GetStatsNullDbReturnsError) {
  NostrDBStats stats;
  NostrDBError err = nostr_db_get_stats(nullptr, &stats);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);
}

TEST_F(NostrDBInitTest, GetStatsNullStatsReturnsError) {
  NostrDBError err = nostr_db_init(&db, test_dir);
  ASSERT_EQ(err, NOSTR_DB_OK);

  err = nostr_db_get_stats(db, nullptr);
  EXPECT_EQ(err, NOSTR_DB_ERROR_NULL_PARAM);
}
