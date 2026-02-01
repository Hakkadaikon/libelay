#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unistd.h>
#include <sys/stat.h>

extern "C" {

// File operations
bool nostr_db_file_exists(const char* path);
int32_t nostr_db_file_create(const char* path, size_t initial_size);
int32_t nostr_db_file_open(const char* path, bool writable);
int32_t nostr_db_file_close(int32_t fd);
int64_t nostr_db_file_get_size(int32_t fd);
int32_t nostr_db_file_extend(int32_t fd, size_t new_size);
int32_t nostr_db_file_sync(int32_t fd);

}  // extern "C"

class NostrDBFileTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary directory for tests
    snprintf(test_file, sizeof(test_file), "/tmp/nostr_db_test_%d.dat", getpid());
    // Ensure file doesn't exist before test
    unlink(test_file);
  }

  void TearDown() override {
    // Clean up test file
    unlink(test_file);
  }

  char test_file[256];
};

TEST_F(NostrDBFileTest, FileExistsReturnsFalseForNonExistent) {
  EXPECT_FALSE(nostr_db_file_exists(test_file));
}

TEST_F(NostrDBFileTest, CreateAndExists) {
  int32_t fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  nostr_db_file_close(fd);

  EXPECT_TRUE(nostr_db_file_exists(test_file));
}

TEST_F(NostrDBFileTest, CreateWithSize) {
  int32_t fd = nostr_db_file_create(test_file, 8192);
  ASSERT_GE(fd, 0);

  int64_t size = nostr_db_file_get_size(fd);
  EXPECT_EQ(size, 8192);

  nostr_db_file_close(fd);
}

TEST_F(NostrDBFileTest, OpenExistingFile) {
  // Create file first
  int32_t fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);
  nostr_db_file_close(fd);

  // Open existing file
  fd = nostr_db_file_open(test_file, true);
  ASSERT_GE(fd, 0);

  int64_t size = nostr_db_file_get_size(fd);
  EXPECT_EQ(size, 4096);

  nostr_db_file_close(fd);
}

TEST_F(NostrDBFileTest, ExtendFile) {
  int32_t fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  int32_t result = nostr_db_file_extend(fd, 16384);
  EXPECT_EQ(result, 0);

  int64_t size = nostr_db_file_get_size(fd);
  EXPECT_EQ(size, 16384);

  nostr_db_file_close(fd);
}

TEST_F(NostrDBFileTest, SyncFile) {
  int32_t fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  int32_t result = nostr_db_file_sync(fd);
  EXPECT_EQ(result, 0);

  nostr_db_file_close(fd);
}

TEST_F(NostrDBFileTest, CreateFailsForExistingFile) {
  // Create file first
  int32_t fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);
  nostr_db_file_close(fd);

  // Try to create again (should fail due to O_EXCL)
  fd = nostr_db_file_create(test_file, 4096);
  EXPECT_LT(fd, 0);
}

TEST_F(NostrDBFileTest, OpenFailsForNonExistent) {
  int32_t fd = nostr_db_file_open(test_file, true);
  EXPECT_LT(fd, 0);
}

TEST_F(NostrDBFileTest, NullPathReturnsError) {
  EXPECT_FALSE(nostr_db_file_exists(nullptr));
  EXPECT_LT(nostr_db_file_create(nullptr, 4096), 0);
  EXPECT_LT(nostr_db_file_open(nullptr, true), 0);
}
