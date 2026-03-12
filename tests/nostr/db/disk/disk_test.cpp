#include <gtest/gtest.h>

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <unistd.h>

extern "C" {

// Disk types
typedef uint32_t page_id_t;
#define PAGE_ID_NULL ((page_id_t)0)
#define DB_PAGE_SIZE 4096

typedef struct {
  uint8_t data[DB_PAGE_SIZE];
} __attribute__((aligned(DB_PAGE_SIZE))) PageData;

typedef struct {
  int32_t   fd;
  uint32_t  total_pages;
  page_id_t free_list_head;
  uint32_t  free_page_count;
  char      path[256];
} DiskManager;

// Error codes
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

// Disk manager API
NostrDBError disk_manager_open(DiskManager* dm, const char* path);
NostrDBError disk_manager_create(DiskManager* dm, const char* path, uint32_t initial_pages);
void         disk_manager_close(DiskManager* dm);
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out);
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id, const PageData* data);
NostrDBError disk_sync(DiskManager* dm);
page_id_t    disk_alloc_page(DiskManager* dm);
NostrDBError disk_free_page(DiskManager* dm, page_id_t page_id);
NostrDBError disk_extend(DiskManager* dm, uint32_t additional_pages);
NostrDBError disk_flush_header(DiskManager* dm);

}  // extern "C"

class DiskManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_file, sizeof(test_file), "/tmp/nostr_disk_test_%d.dat", getpid());
    unlink(test_file);
  }

  void TearDown() override {
    unlink(test_file);
  }

  char test_file[256];
};

// ============================================================================
// Create and open tests
// ============================================================================

TEST_F(DiskManagerTest, CreateNewFile) {
  DiskManager dm;
  NostrDBError err = disk_manager_create(&dm, test_file, 16);
  ASSERT_EQ(err, NOSTR_DB_OK);
  EXPECT_GE(dm.fd, 0);
  EXPECT_EQ(dm.total_pages, 16u);
  EXPECT_EQ(dm.free_list_head, 1u);
  EXPECT_EQ(dm.free_page_count, 15u);
  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, OpenExistingFile) {
  // Create first
  DiskManager dm;
  NostrDBError err = disk_manager_create(&dm, test_file, 16);
  ASSERT_EQ(err, NOSTR_DB_OK);
  disk_manager_close(&dm);

  // Open existing
  DiskManager dm2;
  err = disk_manager_open(&dm2, test_file);
  ASSERT_EQ(err, NOSTR_DB_OK);
  EXPECT_EQ(dm2.total_pages, 16u);
  EXPECT_EQ(dm2.free_list_head, 1u);
  EXPECT_EQ(dm2.free_page_count, 15u);
  disk_manager_close(&dm2);
}

TEST_F(DiskManagerTest, OpenNonExistentFileFails) {
  DiskManager dm;
  NostrDBError err = disk_manager_open(&dm, "/tmp/nonexistent_42.dat");
  EXPECT_NE(err, NOSTR_DB_OK);
}

TEST_F(DiskManagerTest, CreateWithNullParamsFails) {
  DiskManager dm;
  EXPECT_NE(disk_manager_create(nullptr, test_file, 16), NOSTR_DB_OK);
  EXPECT_NE(disk_manager_create(&dm, nullptr, 16), NOSTR_DB_OK);
  EXPECT_NE(disk_manager_create(&dm, test_file, 0), NOSTR_DB_OK);
  EXPECT_NE(disk_manager_create(&dm, test_file, 1), NOSTR_DB_OK);  // need at least 2 pages
}

// ============================================================================
// Page I/O tests
// ============================================================================

TEST_F(DiskManagerTest, WriteAndReadPage) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 16), NOSTR_DB_OK);

  // Allocate a page
  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  // Write data
  PageData write_page;
  memset(&write_page, 0, DB_PAGE_SIZE);
  const char* msg = "Hello B+ Tree";
  memcpy(write_page.data, msg, strlen(msg));

  ASSERT_EQ(disk_write_page(&dm, pid, &write_page), NOSTR_DB_OK);

  // Read it back
  PageData read_page;
  ASSERT_EQ(disk_read_page(&dm, pid, &read_page), NOSTR_DB_OK);

  EXPECT_EQ(memcmp(read_page.data, msg, strlen(msg)), 0);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, ReadInvalidPageFails) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 16), NOSTR_DB_OK);

  PageData page;
  // Page 0 is header, cannot read via disk_read_page
  EXPECT_NE(disk_read_page(&dm, 0, &page), NOSTR_DB_OK);
  // Page beyond total
  EXPECT_NE(disk_read_page(&dm, 100, &page), NOSTR_DB_OK);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, WriteInvalidPageFails) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 16), NOSTR_DB_OK);

  PageData page;
  memset(&page, 0, DB_PAGE_SIZE);
  EXPECT_NE(disk_write_page(&dm, 0, &page), NOSTR_DB_OK);
  EXPECT_NE(disk_write_page(&dm, 100, &page), NOSTR_DB_OK);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, DataPersistsAfterCloseAndReopen) {
  page_id_t pid;
  const char* msg = "persistent data";

  // Write
  {
    DiskManager dm;
    ASSERT_EQ(disk_manager_create(&dm, test_file, 16), NOSTR_DB_OK);
    pid = disk_alloc_page(&dm);
    ASSERT_NE(pid, PAGE_ID_NULL);

    PageData page;
    memset(&page, 0, DB_PAGE_SIZE);
    memcpy(page.data, msg, strlen(msg));
    ASSERT_EQ(disk_write_page(&dm, pid, &page), NOSTR_DB_OK);
    disk_manager_close(&dm);
  }

  // Read back after reopen
  {
    DiskManager dm;
    ASSERT_EQ(disk_manager_open(&dm, test_file), NOSTR_DB_OK);

    PageData page;
    ASSERT_EQ(disk_read_page(&dm, pid, &page), NOSTR_DB_OK);
    EXPECT_EQ(memcmp(page.data, msg, strlen(msg)), 0);
    disk_manager_close(&dm);
  }
}

// ============================================================================
// Allocation tests
// ============================================================================

TEST_F(DiskManagerTest, AllocPagesSequentially) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 8), NOSTR_DB_OK);
  // 7 free pages (1-7)

  // Allocate all free pages
  page_id_t pages[7];
  for (int i = 0; i < 7; i++) {
    pages[i] = disk_alloc_page(&dm);
    ASSERT_NE(pages[i], PAGE_ID_NULL) << "Failed to alloc page " << i;
  }
  EXPECT_EQ(dm.free_page_count, 0u);

  // All allocated pages should be unique and non-zero
  for (int i = 0; i < 7; i++) {
    EXPECT_GT(pages[i], 0u);
    for (int j = i + 1; j < 7; j++) {
      EXPECT_NE(pages[i], pages[j]);
    }
  }

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, AllocTriggersAutoExtend) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 4), NOSTR_DB_OK);
  // 3 free pages

  // Allocate all, then one more to trigger extend
  for (int i = 0; i < 3; i++) {
    ASSERT_NE(disk_alloc_page(&dm), PAGE_ID_NULL);
  }
  EXPECT_EQ(dm.free_page_count, 0u);

  // Next alloc should auto-extend
  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);
  EXPECT_GT(dm.total_pages, 4u);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, FreeAndRealloc) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 8), NOSTR_DB_OK);

  page_id_t p1 = disk_alloc_page(&dm);
  page_id_t p2 = disk_alloc_page(&dm);
  ASSERT_NE(p1, PAGE_ID_NULL);
  ASSERT_NE(p2, PAGE_ID_NULL);
  uint32_t free_before = dm.free_page_count;

  // Free p1
  ASSERT_EQ(disk_free_page(&dm, p1), NOSTR_DB_OK);
  EXPECT_EQ(dm.free_page_count, free_before + 1);

  // Re-allocate should give us p1 back (it's now head of free list)
  page_id_t p3 = disk_alloc_page(&dm);
  ASSERT_NE(p3, PAGE_ID_NULL);
  EXPECT_EQ(p3, p1);  // LIFO: most recently freed page is allocated first

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, FreeInvalidPageFails) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 8), NOSTR_DB_OK);

  EXPECT_NE(disk_free_page(&dm, 0), NOSTR_DB_OK);    // Cannot free header page
  EXPECT_NE(disk_free_page(&dm, 100), NOSTR_DB_OK);   // Out of range

  disk_manager_close(&dm);
}

// ============================================================================
// Extend tests
// ============================================================================

TEST_F(DiskManagerTest, ExtendFile) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 4), NOSTR_DB_OK);
  EXPECT_EQ(dm.total_pages, 4u);
  EXPECT_EQ(dm.free_page_count, 3u);

  ASSERT_EQ(disk_extend(&dm, 8), NOSTR_DB_OK);
  EXPECT_EQ(dm.total_pages, 12u);
  EXPECT_EQ(dm.free_page_count, 11u);  // 3 original + 8 new

  // New pages should be allocatable
  for (int i = 0; i < 11; i++) {
    ASSERT_NE(disk_alloc_page(&dm), PAGE_ID_NULL) << "Failed at page " << i;
  }
  EXPECT_EQ(dm.free_page_count, 0u);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, ExtendPreservesExistingData) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 4), NOSTR_DB_OK);

  // Write data to a page
  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  PageData page;
  memset(&page, 0, DB_PAGE_SIZE);
  page.data[0] = 0xDE;
  page.data[1] = 0xAD;
  ASSERT_EQ(disk_write_page(&dm, pid, &page), NOSTR_DB_OK);

  // Extend
  ASSERT_EQ(disk_extend(&dm, 8), NOSTR_DB_OK);

  // Verify data is still intact
  PageData read_page;
  ASSERT_EQ(disk_read_page(&dm, pid, &read_page), NOSTR_DB_OK);
  EXPECT_EQ(read_page.data[0], 0xDE);
  EXPECT_EQ(read_page.data[1], 0xAD);

  disk_manager_close(&dm);
}

// ============================================================================
// Sync and flush tests
// ============================================================================

TEST_F(DiskManagerTest, SyncSucceeds) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 4), NOSTR_DB_OK);

  EXPECT_EQ(disk_sync(&dm), NOSTR_DB_OK);

  disk_manager_close(&dm);
}

TEST_F(DiskManagerTest, FlushHeaderPersists) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 8), NOSTR_DB_OK);

  // Allocate some pages to change header state
  disk_alloc_page(&dm);
  disk_alloc_page(&dm);
  uint32_t  expected_free_count = dm.free_page_count;
  page_id_t expected_free_head  = dm.free_list_head;

  ASSERT_EQ(disk_flush_header(&dm), NOSTR_DB_OK);
  disk_manager_close(&dm);

  // Reopen and verify header
  DiskManager dm2;
  ASSERT_EQ(disk_manager_open(&dm2, test_file), NOSTR_DB_OK);
  EXPECT_EQ(dm2.free_page_count, expected_free_count);
  EXPECT_EQ(dm2.free_list_head, expected_free_head);
  disk_manager_close(&dm2);
}

// ============================================================================
// Stress test: many alloc/free cycles
// ============================================================================

TEST_F(DiskManagerTest, AllocFreeCycles) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 32), NOSTR_DB_OK);

  // Allocate and free in cycles
  for (int cycle = 0; cycle < 5; cycle++) {
    page_id_t pages[10];
    for (int i = 0; i < 10; i++) {
      pages[i] = disk_alloc_page(&dm);
      ASSERT_NE(pages[i], PAGE_ID_NULL);
    }
    // Free half
    for (int i = 0; i < 5; i++) {
      ASSERT_EQ(disk_free_page(&dm, pages[i]), NOSTR_DB_OK);
    }
  }

  disk_manager_close(&dm);

  // Reopen and verify consistency
  DiskManager dm2;
  ASSERT_EQ(disk_manager_open(&dm2, test_file), NOSTR_DB_OK);
  EXPECT_GT(dm2.total_pages, 0u);
  disk_manager_close(&dm2);
}

// ============================================================================
// Write multiple pages and verify isolation
// ============================================================================

TEST_F(DiskManagerTest, MultiplePageIsolation) {
  DiskManager dm;
  ASSERT_EQ(disk_manager_create(&dm, test_file, 16), NOSTR_DB_OK);

  page_id_t p1 = disk_alloc_page(&dm);
  page_id_t p2 = disk_alloc_page(&dm);
  page_id_t p3 = disk_alloc_page(&dm);
  ASSERT_NE(p1, PAGE_ID_NULL);
  ASSERT_NE(p2, PAGE_ID_NULL);
  ASSERT_NE(p3, PAGE_ID_NULL);

  // Write distinct patterns to each page
  PageData page;
  memset(&page, 0xAA, DB_PAGE_SIZE);
  ASSERT_EQ(disk_write_page(&dm, p1, &page), NOSTR_DB_OK);
  memset(&page, 0xBB, DB_PAGE_SIZE);
  ASSERT_EQ(disk_write_page(&dm, p2, &page), NOSTR_DB_OK);
  memset(&page, 0xCC, DB_PAGE_SIZE);
  ASSERT_EQ(disk_write_page(&dm, p3, &page), NOSTR_DB_OK);

  // Read back and verify isolation
  PageData read;
  ASSERT_EQ(disk_read_page(&dm, p1, &read), NOSTR_DB_OK);
  EXPECT_EQ(read.data[0], 0xAA);
  EXPECT_EQ(read.data[4095], 0xAA);

  ASSERT_EQ(disk_read_page(&dm, p2, &read), NOSTR_DB_OK);
  EXPECT_EQ(read.data[0], 0xBB);
  EXPECT_EQ(read.data[4095], 0xBB);

  ASSERT_EQ(disk_read_page(&dm, p3, &read), NOSTR_DB_OK);
  EXPECT_EQ(read.data[0], 0xCC);
  EXPECT_EQ(read.data[4095], 0xCC);

  disk_manager_close(&dm);
}
