#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>

extern "C" {

// Redefine types to avoid _Static_assert issues in C++ context
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

#define BUFFER_FRAME_INVALID ((uint32_t)0xFFFFFFFF)

typedef struct {
  page_id_t* page_ids;
  uint8_t*   pin_counts;
  uint8_t*   dirty_flags;
  uint8_t*   ref_bits;
  PageData*  pages;
  uint64_t*  lsn;
  uint32_t   pool_size;
  uint32_t   clock_hand;
  uint32_t*  hash_table;
  uint32_t   hash_size;
  DiskManager* disk;
} BufferPool;

typedef uint64_t lsn_t;
#define LSN_NULL ((lsn_t)0)

typedef enum {
  WAL_RECORD_BEGIN      = 1,
  WAL_RECORD_COMMIT     = 2,
  WAL_RECORD_ABORT      = 3,
  WAL_RECORD_UPDATE     = 4,
  WAL_RECORD_ALLOC_PAGE = 5,
  WAL_RECORD_FREE_PAGE  = 6,
  WAL_RECORD_CHECKPOINT = 7,
} WalRecordType;

typedef struct {
  lsn_t         lsn;
  lsn_t         prev_lsn;
  uint32_t      tx_id;
  WalRecordType type;
  uint16_t      data_length;
  uint16_t      padding;
} WalRecordHeader;

typedef struct {
  page_id_t page_id;
  uint16_t  offset;
  uint16_t  length;
} WalUpdatePayload;

typedef struct {
  page_id_t page_id;
} WalPagePayload;

#define WAL_BUFFER_SIZE (64 * 1024)

typedef struct {
  int32_t  fd;
  lsn_t    flushed_lsn;
  lsn_t    next_lsn;
  uint32_t next_tx_id;
  uint8_t* buffer;
  uint32_t buffer_size;
  uint32_t buffer_used;
  int64_t  file_offset;
  uint32_t active_tx[64];
  lsn_t    active_tx_lsn[64];
  uint8_t  active_tx_count;
} WalManager;

// Disk manager API
NostrDBError disk_manager_create(DiskManager* dm, const char* path,
                                 uint32_t initial_pages);
NostrDBError disk_manager_open(DiskManager* dm, const char* path);
void         disk_manager_close(DiskManager* dm);
NostrDBError disk_read_page(DiskManager* dm, page_id_t page_id, PageData* out);
NostrDBError disk_write_page(DiskManager* dm, page_id_t page_id,
                             const PageData* data);
NostrDBError disk_sync(DiskManager* dm);

// Buffer pool API
NostrDBError buffer_pool_init(BufferPool* pool, DiskManager* disk,
                              uint32_t pool_size);
void         buffer_pool_shutdown(BufferPool* pool);
PageData*    buffer_pool_pin(BufferPool* pool, page_id_t page_id);
page_id_t    buffer_pool_alloc_page(BufferPool* pool, PageData** out_page);
void         buffer_pool_unpin(BufferPool* pool, page_id_t page_id);
void         buffer_pool_mark_dirty(BufferPool* pool, page_id_t page_id,
                                    uint64_t lsn);
NostrDBError buffer_pool_flush(BufferPool* pool, page_id_t page_id);
NostrDBError buffer_pool_flush_all(BufferPool* pool);

// WAL API
NostrDBError wal_init(WalManager* wal, const char* path);
void         wal_shutdown(WalManager* wal);
uint32_t     wal_log_begin(WalManager* wal);
lsn_t        wal_log_update(WalManager* wal, uint32_t tx_id,
                             page_id_t page_id, uint16_t offset,
                             uint16_t length, const void* old_data,
                             const void* new_data);
lsn_t        wal_log_alloc_page(WalManager* wal, uint32_t tx_id,
                                page_id_t page_id);
lsn_t        wal_log_free_page(WalManager* wal, uint32_t tx_id,
                                page_id_t page_id);
NostrDBError wal_log_commit(WalManager* wal, uint32_t tx_id);
NostrDBError wal_log_abort(WalManager* wal, uint32_t tx_id);
NostrDBError wal_flush(WalManager* wal);
NostrDBError wal_recover(WalManager* wal, DiskManager* disk);
NostrDBError wal_checkpoint(WalManager* wal, BufferPool* pool);

}  // extern "C"

namespace fs = std::filesystem;

class WalTest : public ::testing::Test {
 protected:
  static constexpr const char* TEST_DB_PATH  = "/tmp/wal_test_db.dat";
  static constexpr const char* TEST_WAL_PATH = "/tmp/wal_test.log";
  static constexpr uint32_t    TEST_POOL_SIZE = 16;

  DiskManager disk;
  BufferPool  pool;
  WalManager  wal;

  void SetUp() override
  {
    cleanup_files();
    memset(&disk, 0, sizeof(disk));
    memset(&pool, 0, sizeof(pool));
    memset(&wal, 0, sizeof(wal));
  }

  void TearDown() override
  {
    cleanup_files();
  }

  void cleanup_files()
  {
    fs::remove(TEST_DB_PATH);
    fs::remove(TEST_WAL_PATH);
  }

  void init_disk()
  {
    ASSERT_EQ(disk_manager_create(&disk, TEST_DB_PATH, 64), NOSTR_DB_OK);
  }

  void init_pool()
  {
    ASSERT_EQ(buffer_pool_init(&pool, &disk, TEST_POOL_SIZE), NOSTR_DB_OK);
  }

  void init_wal()
  {
    ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  }

  void shutdown_all()
  {
    wal_shutdown(&wal);
    buffer_pool_shutdown(&pool);
    disk_manager_close(&disk);
  }
};

// ============================================================================
// Basic init/shutdown
// ============================================================================

TEST_F(WalTest, InitAndShutdown)
{
  init_wal();
  EXPECT_GE(wal.fd, 0);
  EXPECT_NE(wal.buffer, nullptr);
  EXPECT_EQ(wal.buffer_size, (uint32_t)WAL_BUFFER_SIZE);
  EXPECT_EQ(wal.buffer_used, 0u);
  EXPECT_EQ(wal.next_lsn, 1u);
  EXPECT_EQ(wal.next_tx_id, 1u);
  wal_shutdown(&wal);
  EXPECT_EQ(wal.fd, -1);
}

TEST_F(WalTest, InitNullParams)
{
  EXPECT_NE(wal_init(nullptr, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_NE(wal_init(&wal, nullptr), NOSTR_DB_OK);
}

// ============================================================================
// Transaction begin
// ============================================================================

TEST_F(WalTest, BeginTransaction)
{
  init_wal();
  uint32_t tx1 = wal_log_begin(&wal);
  EXPECT_EQ(tx1, 1u);
  EXPECT_EQ(wal.active_tx_count, 1u);
  EXPECT_EQ(wal.active_tx[0], tx1);

  uint32_t tx2 = wal_log_begin(&wal);
  EXPECT_EQ(tx2, 2u);
  EXPECT_EQ(wal.active_tx_count, 2u);

  // Buffer should contain two BEGIN records
  EXPECT_EQ(wal.buffer_used, (uint32_t)(sizeof(WalRecordHeader) * 2));

  wal_shutdown(&wal);
}

// ============================================================================
// Commit and abort
// ============================================================================

TEST_F(WalTest, CommitTransaction)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);
  EXPECT_EQ(wal.active_tx_count, 1u);

  EXPECT_EQ(wal_log_commit(&wal, tx), NOSTR_DB_OK);
  EXPECT_EQ(wal.active_tx_count, 0u);

  // After commit, buffer should be flushed
  EXPECT_EQ(wal.buffer_used, 0u);
  EXPECT_GT(wal.flushed_lsn, LSN_NULL);

  wal_shutdown(&wal);
}

TEST_F(WalTest, AbortTransaction)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);
  EXPECT_EQ(wal.active_tx_count, 1u);

  EXPECT_EQ(wal_log_abort(&wal, tx), NOSTR_DB_OK);
  EXPECT_EQ(wal.active_tx_count, 0u);

  wal_shutdown(&wal);
}

// ============================================================================
// Update logging
// ============================================================================

TEST_F(WalTest, LogUpdate)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[8] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07};
  uint8_t new_data[8] = {0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17};

  lsn_t lsn = wal_log_update(&wal, tx, 1, 0, 8, old_data, new_data);
  EXPECT_NE(lsn, LSN_NULL);
  EXPECT_GT(lsn, (lsn_t)0);

  // Expected buffer usage: BEGIN header + UPDATE header + payload + old + new
  uint32_t expected =
      (uint32_t)(sizeof(WalRecordHeader)       // BEGIN
                 + sizeof(WalRecordHeader)      // UPDATE header
                 + sizeof(WalUpdatePayload) + 16);  // payload + data
  EXPECT_EQ(wal.buffer_used, expected);

  wal_shutdown(&wal);
}

TEST_F(WalTest, LogAllocAndFreePage)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  lsn_t alloc_lsn = wal_log_alloc_page(&wal, tx, 42);
  EXPECT_NE(alloc_lsn, LSN_NULL);

  lsn_t free_lsn = wal_log_free_page(&wal, tx, 42);
  EXPECT_NE(free_lsn, LSN_NULL);
  EXPECT_GT(free_lsn, alloc_lsn);

  wal_shutdown(&wal);
}

// ============================================================================
// WAL flush
// ============================================================================

TEST_F(WalTest, FlushWritesToFile)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[4] = {0};
  uint8_t new_data[4] = {1, 2, 3, 4};
  wal_log_update(&wal, tx, 1, 0, 4, old_data, new_data);

  uint32_t used_before = wal.buffer_used;
  EXPECT_GT(used_before, 0u);

  EXPECT_EQ(wal_flush(&wal), NOSTR_DB_OK);
  EXPECT_EQ(wal.buffer_used, 0u);
  EXPECT_GT(wal.file_offset, (int64_t)0);

  wal_shutdown(&wal);
}

TEST_F(WalTest, FlushEmptyBuffer)
{
  init_wal();
  EXPECT_EQ(wal_flush(&wal), NOSTR_DB_OK);
  EXPECT_EQ(wal.file_offset, (int64_t)0);
  wal_shutdown(&wal);
}

// ============================================================================
// Multiple transactions
// ============================================================================

TEST_F(WalTest, MultipleTransactions)
{
  init_wal();

  uint32_t tx1 = wal_log_begin(&wal);
  uint32_t tx2 = wal_log_begin(&wal);

  uint8_t old1[4] = {0};
  uint8_t new1[4] = {1, 1, 1, 1};
  uint8_t old2[4] = {0};
  uint8_t new2[4] = {2, 2, 2, 2};

  wal_log_update(&wal, tx1, 1, 0, 4, old1, new1);
  wal_log_update(&wal, tx2, 2, 0, 4, old2, new2);

  EXPECT_EQ(wal_log_commit(&wal, tx1), NOSTR_DB_OK);
  EXPECT_EQ(wal.active_tx_count, 1u);

  EXPECT_EQ(wal_log_commit(&wal, tx2), NOSTR_DB_OK);
  EXPECT_EQ(wal.active_tx_count, 0u);

  wal_shutdown(&wal);
}

// ============================================================================
// LSN ordering
// ============================================================================

TEST_F(WalTest, LSNMonotonicallyIncreasing)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[4] = {0};
  uint8_t new_data[4] = {1, 2, 3, 4};

  lsn_t lsn1 = wal_log_update(&wal, tx, 1, 0, 4, old_data, new_data);
  lsn_t lsn2 = wal_log_update(&wal, tx, 2, 0, 4, old_data, new_data);
  lsn_t lsn3 = wal_log_update(&wal, tx, 3, 0, 4, old_data, new_data);

  EXPECT_GT(lsn1, LSN_NULL);
  EXPECT_GT(lsn2, lsn1);
  EXPECT_GT(lsn3, lsn2);

  wal_shutdown(&wal);
}

// ============================================================================
// Prev LSN chaining
// ============================================================================

TEST_F(WalTest, PrevLSNChaining)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[4] = {0};
  uint8_t new_data[4] = {1, 2, 3, 4};

  wal_log_update(&wal, tx, 1, 0, 4, old_data, new_data);
  wal_log_update(&wal, tx, 2, 0, 4, old_data, new_data);

  // Verify the prev_lsn chain by reading records from buffer
  uint32_t pos      = 0;
  lsn_t    prev_lsn = LSN_NULL;

  while (pos + sizeof(WalRecordHeader) <= wal.buffer_used) {
    WalRecordHeader* hdr = (WalRecordHeader*)(wal.buffer + pos);
    if (hdr->tx_id == tx) {
      EXPECT_EQ(hdr->prev_lsn, prev_lsn);
      prev_lsn = hdr->lsn;
    }
    pos += sizeof(WalRecordHeader) + hdr->data_length;
  }

  wal_shutdown(&wal);
}

// ============================================================================
// WAL record format verification
// ============================================================================

TEST_F(WalTest, RecordFormatCorrect)
{
  init_wal();
  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[4] = {0xAA, 0xBB, 0xCC, 0xDD};
  uint8_t new_data[4] = {0x11, 0x22, 0x33, 0x44};

  wal_log_update(&wal, tx, 5, 100, 4, old_data, new_data);

  // Verify BEGIN record
  WalRecordHeader* begin_hdr = (WalRecordHeader*)wal.buffer;
  EXPECT_EQ(begin_hdr->type, WAL_RECORD_BEGIN);
  EXPECT_EQ(begin_hdr->tx_id, tx);
  EXPECT_EQ(begin_hdr->data_length, 0u);

  // Verify UPDATE record
  uint32_t         update_offset = sizeof(WalRecordHeader);
  WalRecordHeader* update_hdr =
      (WalRecordHeader*)(wal.buffer + update_offset);
  EXPECT_EQ(update_hdr->type, WAL_RECORD_UPDATE);
  EXPECT_EQ(update_hdr->tx_id, tx);

  uint16_t expected_data_len =
      sizeof(WalUpdatePayload) + 4 + 4;  // payload + old + new
  EXPECT_EQ(update_hdr->data_length, expected_data_len);

  // Verify payload
  WalUpdatePayload* payload = (WalUpdatePayload*)(wal.buffer + update_offset +
                                                   sizeof(WalRecordHeader));
  EXPECT_EQ(payload->page_id, 5u);
  EXPECT_EQ(payload->offset, 100u);
  EXPECT_EQ(payload->length, 4u);

  // Verify old_data
  uint8_t* stored_old = wal.buffer + update_offset + sizeof(WalRecordHeader) +
                         sizeof(WalUpdatePayload);
  EXPECT_EQ(memcmp(stored_old, old_data, 4), 0);

  // Verify new_data
  uint8_t* stored_new = stored_old + 4;
  EXPECT_EQ(memcmp(stored_new, new_data, 4), 0);

  wal_shutdown(&wal);
}

// ============================================================================
// Recovery: redo committed transaction
// ============================================================================

TEST_F(WalTest, RecoveryRedoCommitted)
{
  init_disk();
  init_pool();
  init_wal();

  // Allocate a page and write data through buffer pool
  PageData* page_data = nullptr;
  page_id_t pid       = buffer_pool_alloc_page(&pool, &page_data);
  ASSERT_NE(pid, PAGE_ID_NULL);

  // Start WAL transaction
  uint32_t tx = wal_log_begin(&wal);

  // Write known data to page
  uint8_t old_data[8];
  memset(old_data, 0, 8);
  uint8_t new_data[8] = {0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE};

  // Log the update
  lsn_t lsn = wal_log_update(&wal, tx, pid, 0, 8, old_data, new_data);
  EXPECT_NE(lsn, LSN_NULL);

  // Apply to page in buffer
  memcpy(page_data->data, new_data, 8);
  buffer_pool_mark_dirty(&pool, pid, lsn);

  // Commit and flush
  EXPECT_EQ(wal_log_commit(&wal, tx), NOSTR_DB_OK);

  // Flush buffer pool to disk
  buffer_pool_flush_all(&pool);

  // Now simulate crash: shutdown without cleanup
  wal_shutdown(&wal);
  buffer_pool_shutdown(&pool);
  disk_manager_close(&disk);

  // Zero out the page on disk to simulate partial write loss
  ASSERT_EQ(disk_manager_open(&disk, TEST_DB_PATH), NOSTR_DB_OK);
  PageData zero_page;
  memset(&zero_page, 0, sizeof(zero_page));
  EXPECT_EQ(disk_write_page(&disk, pid, &zero_page), NOSTR_DB_OK);
  disk_sync(&disk);

  // Now recover
  ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_EQ(wal_recover(&wal, &disk), NOSTR_DB_OK);

  // Read the page and verify data was recovered
  PageData recovered;
  EXPECT_EQ(disk_read_page(&disk, pid, &recovered), NOSTR_DB_OK);
  EXPECT_EQ(memcmp(recovered.data, new_data, 8), 0);

  wal_shutdown(&wal);
  disk_manager_close(&disk);
}

// ============================================================================
// Recovery: undo uncommitted transaction
// ============================================================================

TEST_F(WalTest, RecoveryUndoUncommitted)
{
  init_disk();
  init_pool();
  init_wal();

  // Allocate a page
  PageData* page_data = nullptr;
  page_id_t pid       = buffer_pool_alloc_page(&pool, &page_data);
  ASSERT_NE(pid, PAGE_ID_NULL);

  // Write original data to disk
  uint8_t original[8] = {0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};
  memcpy(page_data->data, original, 8);
  buffer_pool_mark_dirty(&pool, pid, 0);
  buffer_pool_flush_all(&pool);

  // Start transaction but DON'T commit
  uint32_t tx          = wal_log_begin(&wal);
  uint8_t  new_data[8] = {0xFF, 0xFE, 0xFD, 0xFC, 0xFB, 0xFA, 0xF9, 0xF8};
  wal_log_update(&wal, tx, pid, 0, 8, original, new_data);

  // Apply dirty write to page on disk (simulating dirty page flush before
  // crash)
  memcpy(page_data->data, new_data, 8);
  buffer_pool_mark_dirty(&pool, pid, 0);
  buffer_pool_flush_all(&pool);

  // Flush WAL manually (normally commit flushes, but tx is uncommitted)
  wal_flush(&wal);

  // Simulate crash
  wal_shutdown(&wal);
  buffer_pool_shutdown(&pool);
  disk_manager_close(&disk);

  // Recover
  ASSERT_EQ(disk_manager_open(&disk, TEST_DB_PATH), NOSTR_DB_OK);
  ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_EQ(wal_recover(&wal, &disk), NOSTR_DB_OK);

  // Page should be restored to original data
  PageData recovered;
  EXPECT_EQ(disk_read_page(&disk, pid, &recovered), NOSTR_DB_OK);
  EXPECT_EQ(memcmp(recovered.data, original, 8), 0);

  wal_shutdown(&wal);
  disk_manager_close(&disk);
}

// ============================================================================
// Checkpoint
// ============================================================================

TEST_F(WalTest, Checkpoint)
{
  init_disk();
  init_pool();
  init_wal();

  // Write some data
  uint32_t tx = wal_log_begin(&wal);
  uint8_t  old_data[4] = {0};
  uint8_t  new_data[4] = {1, 2, 3, 4};
  wal_log_update(&wal, tx, 1, 0, 4, old_data, new_data);
  wal_log_commit(&wal, tx);

  // Checkpoint should succeed
  EXPECT_EQ(wal_checkpoint(&wal, &pool), NOSTR_DB_OK);

  // WAL file should be truncated (offset reset)
  EXPECT_EQ(wal.file_offset, (int64_t)0);

  shutdown_all();
}

// ============================================================================
// Recovery with checkpoint
// ============================================================================

TEST_F(WalTest, RecoveryAfterCheckpoint)
{
  init_disk();
  init_pool();
  init_wal();

  // First transaction + checkpoint
  PageData* page_data1 = nullptr;
  page_id_t pid1       = buffer_pool_alloc_page(&pool, &page_data1);
  ASSERT_NE(pid1, PAGE_ID_NULL);

  uint32_t tx1 = wal_log_begin(&wal);
  uint8_t  data1[4] = {0xAA, 0xBB, 0xCC, 0xDD};
  uint8_t  zero4[4] = {0};
  wal_log_update(&wal, tx1, pid1, 0, 4, zero4, data1);
  memcpy(page_data1->data, data1, 4);
  buffer_pool_mark_dirty(&pool, pid1, 0);
  wal_log_commit(&wal, tx1);

  // Checkpoint flushes everything
  wal_checkpoint(&wal, &pool);

  // Second transaction after checkpoint
  PageData* page_data2 = nullptr;
  page_id_t pid2       = buffer_pool_alloc_page(&pool, &page_data2);
  ASSERT_NE(pid2, PAGE_ID_NULL);

  uint32_t tx2 = wal_log_begin(&wal);
  uint8_t  data2[4] = {0x11, 0x22, 0x33, 0x44};
  wal_log_update(&wal, tx2, pid2, 0, 4, zero4, data2);
  memcpy(page_data2->data, data2, 4);
  buffer_pool_mark_dirty(&pool, pid2, 0);
  wal_log_commit(&wal, tx2);

  // Flush and shutdown
  buffer_pool_flush_all(&pool);
  wal_shutdown(&wal);
  buffer_pool_shutdown(&pool);
  disk_manager_close(&disk);

  // Zero pid2 on disk to simulate incomplete flush
  ASSERT_EQ(disk_manager_open(&disk, TEST_DB_PATH), NOSTR_DB_OK);
  PageData zero_page;
  memset(&zero_page, 0, sizeof(zero_page));
  disk_write_page(&disk, pid2, &zero_page);
  disk_sync(&disk);

  // Recover - should redo tx2's changes
  ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_EQ(wal_recover(&wal, &disk), NOSTR_DB_OK);

  PageData recovered;
  EXPECT_EQ(disk_read_page(&disk, pid2, &recovered), NOSTR_DB_OK);
  EXPECT_EQ(memcmp(recovered.data, data2, 4), 0);

  wal_shutdown(&wal);
  disk_manager_close(&disk);
}

// ============================================================================
// WAL reopen preserves state
// ============================================================================

TEST_F(WalTest, ReopenPreservesFileOffset)
{
  init_wal();

  uint32_t tx = wal_log_begin(&wal);
  uint8_t  old_data[4] = {0};
  uint8_t  new_data[4] = {1, 2, 3, 4};
  wal_log_update(&wal, tx, 1, 0, 4, old_data, new_data);
  wal_log_commit(&wal, tx);

  int64_t offset_after_first = wal.file_offset;
  EXPECT_GT(offset_after_first, (int64_t)0);

  wal_shutdown(&wal);

  // Reopen - should start writing after existing data
  ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_EQ(wal.file_offset, offset_after_first);

  wal_shutdown(&wal);
}

// ============================================================================
// Stress: many updates in one transaction
// ============================================================================

TEST_F(WalTest, StressManyUpdates)
{
  init_wal();

  uint32_t tx = wal_log_begin(&wal);

  uint8_t old_data[16];
  uint8_t new_data[16];
  memset(old_data, 0, 16);

  for (uint32_t i = 0; i < 200; i++) {
    memset(new_data, (uint8_t)(i & 0xFF), 16);
    lsn_t lsn = wal_log_update(&wal, tx, (page_id_t)(i + 1), 0, 16,
                                old_data, new_data);
    EXPECT_NE(lsn, LSN_NULL);
  }

  EXPECT_EQ(wal_log_commit(&wal, tx), NOSTR_DB_OK);

  wal_shutdown(&wal);
}

// ============================================================================
// Mixed commit and abort
// ============================================================================

TEST_F(WalTest, MixedCommitAbort)
{
  init_disk();
  init_pool();
  init_wal();

  // Allocate pages
  PageData* pd1  = nullptr;
  PageData* pd2  = nullptr;
  page_id_t pid1 = buffer_pool_alloc_page(&pool, &pd1);
  page_id_t pid2 = buffer_pool_alloc_page(&pool, &pd2);
  ASSERT_NE(pid1, PAGE_ID_NULL);
  ASSERT_NE(pid2, PAGE_ID_NULL);

  // Write original data
  uint8_t orig1[4] = {0x01, 0x02, 0x03, 0x04};
  uint8_t orig2[4] = {0x05, 0x06, 0x07, 0x08};
  memcpy(pd1->data, orig1, 4);
  memcpy(pd2->data, orig2, 4);
  buffer_pool_mark_dirty(&pool, pid1, 0);
  buffer_pool_mark_dirty(&pool, pid2, 0);
  buffer_pool_flush_all(&pool);

  // TX1: commit
  uint32_t tx1 = wal_log_begin(&wal);
  uint8_t  new1[4] = {0xAA, 0xBB, 0xCC, 0xDD};
  wal_log_update(&wal, tx1, pid1, 0, 4, orig1, new1);
  memcpy(pd1->data, new1, 4);
  buffer_pool_mark_dirty(&pool, pid1, 0);
  wal_log_commit(&wal, tx1);

  // TX2: not committed (simulating crash)
  uint32_t tx2 = wal_log_begin(&wal);
  uint8_t  new2[4] = {0xEE, 0xFF, 0x00, 0x11};
  wal_log_update(&wal, tx2, pid2, 0, 4, orig2, new2);
  memcpy(pd2->data, new2, 4);
  buffer_pool_mark_dirty(&pool, pid2, 0);

  // Flush dirty pages before crash
  buffer_pool_flush_all(&pool);
  wal_flush(&wal);

  // Simulate crash
  wal_shutdown(&wal);
  buffer_pool_shutdown(&pool);
  disk_manager_close(&disk);

  // Recover
  ASSERT_EQ(disk_manager_open(&disk, TEST_DB_PATH), NOSTR_DB_OK);
  ASSERT_EQ(wal_init(&wal, TEST_WAL_PATH), NOSTR_DB_OK);
  EXPECT_EQ(wal_recover(&wal, &disk), NOSTR_DB_OK);

  // pid1: committed -> should have new1 data
  PageData r1;
  EXPECT_EQ(disk_read_page(&disk, pid1, &r1), NOSTR_DB_OK);
  EXPECT_EQ(memcmp(r1.data, new1, 4), 0);

  // pid2: uncommitted -> should be reverted to orig2
  PageData r2;
  EXPECT_EQ(disk_read_page(&disk, pid2, &r2), NOSTR_DB_OK);
  EXPECT_EQ(memcmp(r2.data, orig2, 4), 0);

  wal_shutdown(&wal);
  disk_manager_close(&disk);
}

// ============================================================================
// Record header size
// ============================================================================

TEST_F(WalTest, RecordHeaderSize)
{
  EXPECT_EQ(sizeof(WalRecordHeader), 32u);
}
