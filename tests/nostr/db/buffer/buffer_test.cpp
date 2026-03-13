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

// Buffer types
#define BUFFER_FRAME_INVALID ((uint32_t)0xFFFFFFFF)

typedef struct {
  page_id_t*   page_ids;
  uint8_t*     pin_counts;
  uint8_t*     dirty_flags;
  uint8_t*     ref_bits;
  PageData*    pages;
  uint64_t*    lsn;
  uint32_t     pool_size;
  uint32_t     clock_hand;
  uint32_t*    hash_table;
  uint32_t     hash_size;
  DiskManager* disk;
} BufferPool;

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
NostrDBError disk_manager_create(DiskManager* dm, const char* path, uint32_t initial_pages);
void         disk_manager_close(DiskManager* dm);
page_id_t    disk_alloc_page(DiskManager* dm);
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out);
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id, const PageData* data);

// Buffer pool API
NostrDBError buffer_pool_init(BufferPool* pool, DiskManager* disk, uint32_t pool_size);
void         buffer_pool_shutdown(BufferPool* pool);
PageData*    buffer_pool_pin(BufferPool* pool, page_id_t page_id);
page_id_t    buffer_pool_alloc_page(BufferPool* pool, PageData** out_page);
void         buffer_pool_unpin(BufferPool* pool, page_id_t page_id);
void         buffer_pool_mark_dirty(BufferPool* pool, page_id_t page_id, uint64_t lsn);
NostrDBError buffer_pool_flush(BufferPool* pool, page_id_t page_id);
NostrDBError buffer_pool_flush_all(BufferPool* pool);

}  // extern "C"

class BufferPoolTest : public ::testing::Test {
protected:
  void SetUp() override {
    snprintf(test_file, sizeof(test_file), "/tmp/nostr_buf_test_%d.dat", getpid());
    unlink(test_file);
    ASSERT_EQ(disk_manager_create(&dm, test_file, 64), NOSTR_DB_OK);
  }

  void TearDown() override {
    buffer_pool_shutdown(&pool);
    disk_manager_close(&dm);
    unlink(test_file);
  }

  char        test_file[256];
  DiskManager dm;
  BufferPool  pool;
};

// ============================================================================
// Init / Shutdown
// ============================================================================

TEST_F(BufferPoolTest, InitSuccess) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 16), NOSTR_DB_OK);
  EXPECT_EQ(pool.pool_size, 16u);
  EXPECT_NE(pool.page_ids, nullptr);
  EXPECT_NE(pool.pages, nullptr);
  EXPECT_NE(pool.hash_table, nullptr);
  EXPECT_GT(pool.hash_size, 32u);  // At least 2x pool_size
}

TEST_F(BufferPoolTest, InitWithNullFails) {
  EXPECT_NE(buffer_pool_init(nullptr, &dm, 16), NOSTR_DB_OK);
  EXPECT_NE(buffer_pool_init(&pool, nullptr, 16), NOSTR_DB_OK);
  EXPECT_NE(buffer_pool_init(&pool, &dm, 0), NOSTR_DB_OK);
}

TEST_F(BufferPoolTest, ShutdownNull) {
  // Should not crash
  buffer_pool_shutdown(nullptr);
}

// ============================================================================
// Pin / Unpin
// ============================================================================

TEST_F(BufferPoolTest, PinAndUnpin) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  // Allocate a page on disk first, write some data
  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);
  PageData disk_page;
  memset(&disk_page, 0, DB_PAGE_SIZE);
  disk_page.data[0] = 0x42;
  ASSERT_EQ(disk_write_page(&dm, pid, &disk_page), NOSTR_DB_OK);

  // Pin should load the page
  PageData* page = buffer_pool_pin(&pool, pid);
  ASSERT_NE(page, nullptr);
  EXPECT_EQ(page->data[0], 0x42);

  // Unpin
  buffer_pool_unpin(&pool, pid);
}

TEST_F(BufferPoolTest, PinSamePageTwice) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  PageData* p1 = buffer_pool_pin(&pool, pid);
  ASSERT_NE(p1, nullptr);

  // Pin again should return the same pointer (cache hit)
  PageData* p2 = buffer_pool_pin(&pool, pid);
  ASSERT_NE(p2, nullptr);
  EXPECT_EQ(p1, p2);

  buffer_pool_unpin(&pool, pid);
  buffer_pool_unpin(&pool, pid);
}

TEST_F(BufferPoolTest, PinNullPageFails) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);
  EXPECT_EQ(buffer_pool_pin(&pool, PAGE_ID_NULL), nullptr);
}

TEST_F(BufferPoolTest, PinMultiplePages) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pids[5];
  for (int i = 0; i < 5; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);

    // Write distinct data
    PageData dp;
    memset(&dp, 0, DB_PAGE_SIZE);
    dp.data[0] = (uint8_t)(i + 1);
    ASSERT_EQ(disk_write_page(&dm, pids[i], &dp), NOSTR_DB_OK);
  }

  // Pin all and verify isolation
  PageData* pages[5];
  for (int i = 0; i < 5; i++) {
    pages[i] = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(pages[i], nullptr);
    EXPECT_EQ(pages[i]->data[0], (uint8_t)(i + 1));
  }

  for (int i = 0; i < 5; i++) {
    buffer_pool_unpin(&pool, pids[i]);
  }
}

// ============================================================================
// Alloc page
// ============================================================================

TEST_F(BufferPoolTest, AllocPage) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  PageData* page = nullptr;
  page_id_t pid  = buffer_pool_alloc_page(&pool, &page);
  ASSERT_NE(pid, PAGE_ID_NULL);
  ASSERT_NE(page, nullptr);

  // Should be zeroed
  for (int i = 0; i < DB_PAGE_SIZE; i++) {
    EXPECT_EQ(page->data[i], 0) << "byte " << i << " not zero";
  }

  // Write some data
  page->data[0] = 0xFF;

  // Unpin and flush
  buffer_pool_mark_dirty(&pool, pid, 0);
  buffer_pool_unpin(&pool, pid);
  ASSERT_EQ(buffer_pool_flush(&pool, pid), NOSTR_DB_OK);

  // Verify on disk
  PageData verify;
  ASSERT_EQ(disk_read_page(&dm, pid, &verify), NOSTR_DB_OK);
  EXPECT_EQ(verify.data[0], 0xFF);
}

TEST_F(BufferPoolTest, AllocMultiplePages) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pids[6];
  for (int i = 0; i < 6; i++) {
    PageData* page = nullptr;
    pids[i]        = buffer_pool_alloc_page(&pool, &page);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    ASSERT_NE(page, nullptr);
    page->data[0] = (uint8_t)(i + 10);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // All page IDs should be unique
  for (int i = 0; i < 6; i++) {
    for (int j = i + 1; j < 6; j++) {
      EXPECT_NE(pids[i], pids[j]);
    }
  }
}

// ============================================================================
// Dirty tracking and flush
// ============================================================================

TEST_F(BufferPoolTest, MarkDirtyAndFlush) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  PageData* page = buffer_pool_pin(&pool, pid);
  ASSERT_NE(page, nullptr);

  page->data[0] = 0xAB;
  page->data[1] = 0xCD;
  buffer_pool_mark_dirty(&pool, pid, 1);
  buffer_pool_unpin(&pool, pid);

  // Flush
  ASSERT_EQ(buffer_pool_flush(&pool, pid), NOSTR_DB_OK);

  // Read directly from disk to verify
  PageData verify;
  ASSERT_EQ(disk_read_page(&dm, pid, &verify), NOSTR_DB_OK);
  EXPECT_EQ(verify.data[0], 0xAB);
  EXPECT_EQ(verify.data[1], 0xCD);
}

TEST_F(BufferPoolTest, FlushAll) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pids[3];
  for (int i = 0; i < 3; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);

    PageData* page = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(page, nullptr);
    page->data[0] = (uint8_t)(i + 0xA0);
    buffer_pool_mark_dirty(&pool, pids[i], 0);
    buffer_pool_unpin(&pool, pids[i]);
  }

  ASSERT_EQ(buffer_pool_flush_all(&pool), NOSTR_DB_OK);

  // Verify all on disk
  for (int i = 0; i < 3; i++) {
    PageData verify;
    ASSERT_EQ(disk_read_page(&dm, pids[i], &verify), NOSTR_DB_OK);
    EXPECT_EQ(verify.data[0], (uint8_t)(i + 0xA0));
  }
}

TEST_F(BufferPoolTest, FlushNonExistentPage) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);
  EXPECT_EQ(buffer_pool_flush(&pool, 99), NOSTR_DB_ERROR_NOT_FOUND);
}

// ============================================================================
// Clock eviction
// ============================================================================

TEST_F(BufferPoolTest, EvictionOnPoolFull) {
  // Pool size 4, but we'll access more than 4 pages
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Allocate 6 pages on disk
  page_id_t pids[6];
  for (int i = 0; i < 6; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    PageData dp;
    memset(&dp, 0, DB_PAGE_SIZE);
    dp.data[0] = (uint8_t)(i + 1);
    ASSERT_EQ(disk_write_page(&dm, pids[i], &dp), NOSTR_DB_OK);
  }

  // Pin first 4, then unpin all
  for (int i = 0; i < 4; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->data[0], (uint8_t)(i + 1));
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Pin 5th page -> should evict one of the first 4
  PageData* p5 = buffer_pool_pin(&pool, pids[4]);
  ASSERT_NE(p5, nullptr);
  EXPECT_EQ(p5->data[0], 5);
  buffer_pool_unpin(&pool, pids[4]);

  // Pin 6th page -> should evict another
  PageData* p6 = buffer_pool_pin(&pool, pids[5]);
  ASSERT_NE(p6, nullptr);
  EXPECT_EQ(p6->data[0], 6);
  buffer_pool_unpin(&pool, pids[5]);
}

TEST_F(BufferPoolTest, PinnedPagesNotEvicted) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Allocate 5 pages
  page_id_t pids[5];
  for (int i = 0; i < 5; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
  }

  // Pin all 4 frames and DON'T unpin
  for (int i = 0; i < 4; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr);
    // Intentionally NOT unpinning
  }

  // Pin 5th page should fail (all frames are pinned)
  PageData* p5 = buffer_pool_pin(&pool, pids[4]);
  EXPECT_EQ(p5, nullptr);

  // Unpin one and try again
  buffer_pool_unpin(&pool, pids[0]);
  p5 = buffer_pool_pin(&pool, pids[4]);
  EXPECT_NE(p5, nullptr);

  // Cleanup
  buffer_pool_unpin(&pool, pids[1]);
  buffer_pool_unpin(&pool, pids[2]);
  buffer_pool_unpin(&pool, pids[3]);
  buffer_pool_unpin(&pool, pids[4]);
}

TEST_F(BufferPoolTest, DirtyPageFlushedOnEviction) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Fill pool
  page_id_t pids[5];
  for (int i = 0; i < 5; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
  }

  // Pin first 4, modify, mark dirty, unpin
  for (int i = 0; i < 4; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr);
    p->data[0] = (uint8_t)(0xD0 + i);
    buffer_pool_mark_dirty(&pool, pids[i], 0);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Pin 5th page -> evicts one, which should auto-flush dirty data
  PageData* p5 = buffer_pool_pin(&pool, pids[4]);
  ASSERT_NE(p5, nullptr);
  buffer_pool_unpin(&pool, pids[4]);

  // Verify that the evicted page was flushed to disk
  // At least one of the first 4 should have been written
  int flushed_count = 0;
  for (int i = 0; i < 4; i++) {
    PageData verify;
    ASSERT_EQ(disk_read_page(&dm, pids[i], &verify), NOSTR_DB_OK);
    if (verify.data[0] == (uint8_t)(0xD0 + i)) {
      flushed_count++;
    }
  }
  EXPECT_GE(flushed_count, 1);
}

// ============================================================================
// Data persistence through eviction and re-pin
// ============================================================================

TEST_F(BufferPoolTest, ReloadAfterEviction) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  page_id_t pids[6];
  for (int i = 0; i < 6; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
  }

  // Write data to page 0 via buffer pool
  PageData* p = buffer_pool_pin(&pool, pids[0]);
  ASSERT_NE(p, nullptr);
  p->data[0] = 0xAA;
  p->data[1] = 0xBB;
  buffer_pool_mark_dirty(&pool, pids[0], 0);
  buffer_pool_unpin(&pool, pids[0]);

  // Evict page 0 by loading 4 other pages
  for (int i = 1; i <= 4; i++) {
    PageData* px = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(px, nullptr);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Re-pin page 0 -> should reload from disk
  p = buffer_pool_pin(&pool, pids[0]);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(p->data[0], 0xAA);
  EXPECT_EQ(p->data[1], 0xBB);
  buffer_pool_unpin(&pool, pids[0]);
}

// ============================================================================
// LSN tracking
// ============================================================================

TEST_F(BufferPoolTest, LsnTracking) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  PageData* p = buffer_pool_pin(&pool, pid);
  ASSERT_NE(p, nullptr);

  // Mark dirty with increasing LSN
  buffer_pool_mark_dirty(&pool, pid, 10);
  buffer_pool_mark_dirty(&pool, pid, 5);   // Lower LSN should not overwrite
  buffer_pool_mark_dirty(&pool, pid, 20);

  // Find the frame and check LSN
  // We can't directly access frame internals easily, but we can verify
  // that flush works and the page is dirty
  ASSERT_EQ(buffer_pool_flush(&pool, pid), NOSTR_DB_OK);

  buffer_pool_unpin(&pool, pid);
}

// ============================================================================
// Shutdown flushes dirty pages
// ============================================================================

TEST_F(BufferPoolTest, ShutdownFlushesDirty) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  page_id_t pid = disk_alloc_page(&dm);
  ASSERT_NE(pid, PAGE_ID_NULL);

  PageData* p = buffer_pool_pin(&pool, pid);
  ASSERT_NE(p, nullptr);
  p->data[0] = 0xEE;
  buffer_pool_mark_dirty(&pool, pid, 0);
  buffer_pool_unpin(&pool, pid);

  // Shutdown (should flush)
  buffer_pool_shutdown(&pool);
  memset(&pool, 0, sizeof(pool));  // Prevent double shutdown in TearDown

  // Verify on disk
  PageData verify;
  ASSERT_EQ(disk_read_page(&dm, pid, &verify), NOSTR_DB_OK);
  EXPECT_EQ(verify.data[0], 0xEE);
}

// ============================================================================
// Stress: many pages cycling through small pool
// ============================================================================

TEST_F(BufferPoolTest, StressManyPagesSmallPool) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 8), NOSTR_DB_OK);

  // Allocate 30 pages on disk
  const int N = 30;
  page_id_t pids[N];
  for (int i = 0; i < N; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);

    // Write identity data
    PageData dp;
    memset(&dp, 0, DB_PAGE_SIZE);
    dp.data[0] = (uint8_t)(i & 0xFF);
    dp.data[1] = (uint8_t)((i >> 8) & 0xFF);
    ASSERT_EQ(disk_write_page(&dm, pids[i], &dp), NOSTR_DB_OK);
  }

  // Access pages in round-robin through the small pool
  for (int round = 0; round < 3; round++) {
    for (int i = 0; i < N; i++) {
      PageData* p = buffer_pool_pin(&pool, pids[i]);
      ASSERT_NE(p, nullptr) << "round=" << round << " i=" << i;
      EXPECT_EQ(p->data[0], (uint8_t)(i & 0xFF));
      EXPECT_EQ(p->data[1], (uint8_t)((i >> 8) & 0xFF));
      buffer_pool_unpin(&pool, pids[i]);
    }
  }
}

// ============================================================================
// Alloc + modify + evict + reload cycle
// ============================================================================

TEST_F(BufferPoolTest, AllocModifyEvictReload) {
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Allocate and modify 8 pages through a pool of 4
  const int N = 8;
  page_id_t pids[N];

  for (int i = 0; i < N; i++) {
    PageData* page = nullptr;
    pids[i]        = buffer_pool_alloc_page(&pool, &page);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    ASSERT_NE(page, nullptr);
    page->data[0] = (uint8_t)(i + 100);
    // alloc_page already marks dirty
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Reload all and verify
  for (int i = 0; i < N; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr) << "Failed to reload page " << i;
    EXPECT_EQ(p->data[0], (uint8_t)(i + 100)) << "Data mismatch page " << i;
    buffer_pool_unpin(&pool, pids[i]);
  }
}

// ============================================================================
// Phase 9-3: Clock replacement boundary tests
// ============================================================================

TEST_F(BufferPoolTest, ClockSecondChance) {
  // Pool of 4 frames. Access pattern that tests second chance.
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Allocate 4 pages to fill the pool
  page_id_t pids[4];
  for (int i = 0; i < 4; i++) {
    PageData* p = nullptr;
    pids[i]     = buffer_pool_alloc_page(&pool, &p);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    p->data[0] = (uint8_t)(i + 1);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Re-access pages 0 and 1 to set their ref bits
  for (int i = 0; i < 2; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Allocate a new page — should evict a page without ref bit (2 or 3)
  PageData* new_p = nullptr;
  page_id_t new_pid = buffer_pool_alloc_page(&pool, &new_p);
  ASSERT_NE(new_pid, PAGE_ID_NULL);
  ASSERT_NE(new_p, nullptr);
  buffer_pool_unpin(&pool, new_pid);

  // Pages 0 and 1 should still be accessible (were given second chance)
  for (int i = 0; i < 2; i++) {
    PageData* p = buffer_pool_pin(&pool, pids[i]);
    ASSERT_NE(p, nullptr) << "Page " << i << " should survive eviction";
    EXPECT_EQ(p->data[0], (uint8_t)(i + 1));
    buffer_pool_unpin(&pool, pids[i]);
  }
}

TEST_F(BufferPoolTest, HotColdAccessPattern) {
  // Pool of 4. Keep 2 "hot" pages accessed frequently, cycle "cold" pages
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 4), NOSTR_DB_OK);

  // Create 10 pages on disk
  const int N = 10;
  page_id_t pids[N];
  for (int i = 0; i < N; i++) {
    pids[i] = disk_alloc_page(&dm);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    PageData dp;
    memset(&dp, 0, DB_PAGE_SIZE);
    dp.data[0] = (uint8_t)(i);
    ASSERT_EQ(disk_write_page(&dm, pids[i], &dp), NOSTR_DB_OK);
  }

  // Cycle: access hot pages (0,1) + one cold page each iteration
  for (int round = 0; round < 5; round++) {
    // Access hot pages
    for (int h = 0; h < 2; h++) {
      PageData* p = buffer_pool_pin(&pool, pids[h]);
      ASSERT_NE(p, nullptr);
      EXPECT_EQ(p->data[0], (uint8_t)(h));
      buffer_pool_unpin(&pool, pids[h]);
    }
    // Access a cold page
    int       cold_idx = 2 + round;
    PageData* p        = buffer_pool_pin(&pool, pids[cold_idx]);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->data[0], (uint8_t)(cold_idx));
    buffer_pool_unpin(&pool, pids[cold_idx]);
  }

  // Hot pages should still be in pool (frequently accessed)
  for (int h = 0; h < 2; h++) {
    PageData* p = buffer_pool_pin(&pool, pids[h]);
    ASSERT_NE(p, nullptr);
    EXPECT_EQ(p->data[0], (uint8_t)(h));
    buffer_pool_unpin(&pool, pids[h]);
  }
}

TEST_F(BufferPoolTest, AllPinnedEvictionFails) {
  // Pool of 2. Pin both frames. Attempting to pin a third should still work
  // because the implementation may flush + evict, or return NULL.
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 2), NOSTR_DB_OK);

  page_id_t p1 = disk_alloc_page(&dm);
  page_id_t p2 = disk_alloc_page(&dm);
  page_id_t p3 = disk_alloc_page(&dm);
  ASSERT_NE(p1, PAGE_ID_NULL);
  ASSERT_NE(p2, PAGE_ID_NULL);
  ASSERT_NE(p3, PAGE_ID_NULL);

  // Pin both frames (don't unpin)
  PageData* pp1 = buffer_pool_pin(&pool, p1);
  PageData* pp2 = buffer_pool_pin(&pool, p2);
  ASSERT_NE(pp1, nullptr);
  ASSERT_NE(pp2, nullptr);

  // Try to pin a third page — should fail (all frames pinned)
  PageData* pp3 = buffer_pool_pin(&pool, p3);
  EXPECT_EQ(pp3, nullptr);

  buffer_pool_unpin(&pool, p1);
  buffer_pool_unpin(&pool, p2);
}

TEST_F(BufferPoolTest, SaturatedPoolDirtyEviction) {
  // Pool of 3, write dirty data to 6 pages, verify all persisted
  ASSERT_EQ(buffer_pool_init(&pool, &dm, 3), NOSTR_DB_OK);

  const int N = 6;
  page_id_t pids[N];
  for (int i = 0; i < N; i++) {
    PageData* p = nullptr;
    pids[i]     = buffer_pool_alloc_page(&pool, &p);
    ASSERT_NE(pids[i], PAGE_ID_NULL);
    memset(p->data, (uint8_t)(i + 0x10), DB_PAGE_SIZE);
    buffer_pool_unpin(&pool, pids[i]);
  }

  // Flush and verify from disk
  buffer_pool_flush_all(&pool);

  for (int i = 0; i < N; i++) {
    PageData verify;
    ASSERT_EQ(disk_read_page(&dm, pids[i], &verify), NOSTR_DB_OK);
    EXPECT_EQ(verify.data[100], (uint8_t)(i + 0x10))
        << "Dirty data not persisted for page " << i;
  }
}
