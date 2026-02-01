#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unistd.h>

extern "C" {

// File operations
int32_t nostr_db_file_create(const char* path, size_t initial_size);
int32_t nostr_db_file_close(int32_t fd);

// Mmap operations
void* nostr_db_mmap_file(int32_t fd, size_t size, bool writable);
int32_t nostr_db_munmap(void* addr, size_t size);
int32_t nostr_db_msync(void* addr, size_t size, bool async);
void* nostr_db_mmap_extend(void* old_addr, size_t old_size, size_t new_size);

}  // extern "C"

class NostrDBMmapTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_file, sizeof(test_file), "/tmp/nostr_db_mmap_test_%d.dat", getpid());
    unlink(test_file);
    fd = -1;
    map = nullptr;
    map_size = 0;
  }

  void TearDown() override {
    if (map != nullptr && map_size > 0) {
      nostr_db_munmap(map, map_size);
    }
    if (fd >= 0) {
      nostr_db_file_close(fd);
    }
    unlink(test_file);
  }

  char test_file[256];
  int32_t fd;
  void* map;
  size_t map_size;
};

TEST_F(NostrDBMmapTest, MmapFileReadWrite) {
  fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  map = nostr_db_mmap_file(fd, 4096, true);
  ASSERT_NE(map, nullptr);
  map_size = 4096;

  // Write some data
  char* data = (char*)map;
  strcpy(data, "Hello, mmap!");

  // Sync to disk
  int32_t result = nostr_db_msync(map, map_size, false);
  EXPECT_EQ(result, 0);

  // Verify data
  EXPECT_STREQ(data, "Hello, mmap!");
}

TEST_F(NostrDBMmapTest, MmapFileReadOnly) {
  fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  map = nostr_db_mmap_file(fd, 4096, false);
  ASSERT_NE(map, nullptr);
  map_size = 4096;

  // Read-only mapping should succeed
  // Writing to it would cause a segfault, so we don't test that
}

TEST_F(NostrDBMmapTest, MunmapSuccess) {
  fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  map = nostr_db_mmap_file(fd, 4096, true);
  ASSERT_NE(map, nullptr);

  int32_t result = nostr_db_munmap(map, 4096);
  EXPECT_EQ(result, 0);
  map = nullptr;
  map_size = 0;
}

TEST_F(NostrDBMmapTest, MsyncAsync) {
  fd = nostr_db_file_create(test_file, 4096);
  ASSERT_GE(fd, 0);

  map = nostr_db_mmap_file(fd, 4096, true);
  ASSERT_NE(map, nullptr);
  map_size = 4096;

  // Async sync
  int32_t result = nostr_db_msync(map, map_size, true);
  EXPECT_EQ(result, 0);
}

TEST_F(NostrDBMmapTest, MmapInvalidFdReturnsNull) {
  void* result = nostr_db_mmap_file(-1, 4096, true);
  EXPECT_EQ(result, nullptr);
}

TEST_F(NostrDBMmapTest, MunmapNullReturnsError) {
  int32_t result = nostr_db_munmap(nullptr, 4096);
  EXPECT_LT(result, 0);
}

TEST_F(NostrDBMmapTest, MsyncNullReturnsError) {
  int32_t result = nostr_db_msync(nullptr, 4096, false);
  EXPECT_LT(result, 0);
}

TEST_F(NostrDBMmapTest, MmapExtendNullReturnsNull) {
  void* result = nostr_db_mmap_extend(nullptr, 4096, 8192);
  EXPECT_EQ(result, nullptr);
}
