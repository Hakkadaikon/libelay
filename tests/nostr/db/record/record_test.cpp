#include <gtest/gtest.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>

extern "C" {

// --- Redefine types to avoid _Static_assert in C++ ---
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

// Page types
typedef enum {
  PAGE_TYPE_FREE        = 0,
  PAGE_TYPE_FILE_HEADER = 1,
  PAGE_TYPE_RECORD      = 2,
  PAGE_TYPE_BTREE_LEAF  = 3,
  PAGE_TYPE_BTREE_INNER = 4,
  PAGE_TYPE_OVERFLOW    = 5,
} PageType;

typedef struct {
  page_id_t page_id;
  page_id_t overflow_page;
  uint16_t  slot_count;
  uint16_t  free_space_start;
  uint16_t  free_space_end;
  uint16_t  fragmented_space;
  uint8_t   page_type;
  uint8_t   flags;
  uint8_t   reserved[6];
} SlotPageHeader;

typedef struct {
  uint16_t offset;
  uint16_t length;
} SlotEntry;

typedef struct {
  page_id_t page_id;
  uint16_t  slot_index;
} RecordId;

typedef struct {
  uint8_t  id[32];
  uint8_t  pubkey[32];
  uint8_t  sig[64];
  int64_t  created_at;
  uint32_t kind;
  uint32_t flags;
  uint16_t content_length;
  uint16_t tags_length;
} EventRecord;

typedef struct {
  page_id_t next_page;
  uint16_t  data_length;
  uint16_t  reserved;
} OverflowHeader;

typedef struct {
  uint16_t  total_length;
  uint16_t  inline_length;
  page_id_t overflow_page;
} SpannedPrefix;

#define SLOT_PAGE_HEADER_SIZE sizeof(SlotPageHeader)
#define SLOT_ENTRY_SIZE sizeof(SlotEntry)
#define SPANNED_MARKER ((uint16_t)0xFFFF)
#define SPANNED_PREFIX_SIZE sizeof(SpannedPrefix)
#define OVERFLOW_DATA_SPACE (DB_PAGE_SIZE - sizeof(OverflowHeader))

#define NOSTR_EVENT_TAG_LENGTH (2 * 1024)
#define NOSTR_EVENT_TAG_VALUE_COUNT 16
#define NOSTR_EVENT_TAG_VALUE_LENGTH 512
#define NOSTR_EVENT_CONTENT_LENGTH (1 * 1024 * 1024)

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
  time_t         created_at;
  NostrTagEntity tags[NOSTR_EVENT_TAG_LENGTH];
  char           content[NOSTR_EVENT_CONTENT_LENGTH];
  char           sig[129];
  char           dummy3[7];
} NostrEventEntity;

// Disk manager API
NostrDBError disk_manager_create(DiskManager* dm, const char* path,
                                 uint32_t initial_pages);
void         disk_manager_close(DiskManager* dm);
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
NostrDBError buffer_pool_flush_all(BufferPool* pool);

// Slot page API
void         slot_page_init(PageData* page, page_id_t page_id);
NostrDBError slot_page_insert(PageData* page, const void* data,
                              uint16_t length, uint16_t* slot_index);
NostrDBError slot_page_read(const PageData* page, uint16_t slot_index,
                            void* out, uint16_t* length);
NostrDBError slot_page_delete(PageData* page, uint16_t slot_index);
uint16_t     slot_page_free_space(const PageData* page);
void         slot_page_compact(PageData* page);
uint16_t     slot_page_slot_count(const PageData* page);

// Record manager API
NostrDBError record_insert(BufferPool* pool, const void* data,
                           uint16_t length, RecordId* out_rid);
NostrDBError record_read(BufferPool* pool, RecordId rid, void* out,
                         uint16_t* length);
NostrDBError record_delete(BufferPool* pool, RecordId rid);
NostrDBError record_update(BufferPool* pool, RecordId* rid, const void* data,
                           uint16_t length);

// Overflow API
bool overflow_is_spanned(const PageData* page, uint16_t slot_index);

// Event serializer API
int32_t      event_serialize(const NostrEventEntity* event, uint8_t* buffer,
                             uint16_t capacity);
NostrDBError event_deserialize(const uint8_t* buffer, uint16_t length,
                               NostrEventEntity* event);

}  // extern "C"

namespace fs = std::filesystem;

// ============================================================================
// Slot page tests
// ============================================================================

class SlotPageTest : public ::testing::Test {
 protected:
  PageData page;

  void SetUp() override
  {
    memset(&page, 0, sizeof(page));
    slot_page_init(&page, 1);
  }
};

TEST_F(SlotPageTest, Init)
{
  SlotPageHeader* hdr = (SlotPageHeader*)page.data;
  EXPECT_EQ(hdr->page_id, 1u);
  EXPECT_EQ(hdr->page_type, PAGE_TYPE_RECORD);
  EXPECT_EQ(hdr->slot_count, 0u);
  EXPECT_EQ(hdr->free_space_start, (uint16_t)SLOT_PAGE_HEADER_SIZE);
  EXPECT_EQ(hdr->free_space_end, (uint16_t)DB_PAGE_SIZE);
  EXPECT_EQ(hdr->fragmented_space, 0u);
}

TEST_F(SlotPageTest, InsertAndRead)
{
  uint8_t  data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint16_t slot_idx = 0;

  EXPECT_EQ(slot_page_insert(&page, data, 10, &slot_idx), NOSTR_DB_OK);
  EXPECT_EQ(slot_idx, 0u);
  EXPECT_EQ(slot_page_slot_count(&page), 1u);

  uint8_t  out[10];
  uint16_t len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, 0, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 10u);
  EXPECT_EQ(memcmp(out, data, 10), 0);
}

TEST_F(SlotPageTest, InsertMultiple)
{
  uint8_t  data1[5] = {1, 2, 3, 4, 5};
  uint8_t  data2[8] = {10, 20, 30, 40, 50, 60, 70, 80};
  uint16_t s1, s2;

  EXPECT_EQ(slot_page_insert(&page, data1, 5, &s1), NOSTR_DB_OK);
  EXPECT_EQ(slot_page_insert(&page, data2, 8, &s2), NOSTR_DB_OK);
  EXPECT_EQ(s1, 0u);
  EXPECT_EQ(s2, 1u);

  uint8_t  out[10];
  uint16_t len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, 0, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 5u);
  EXPECT_EQ(memcmp(out, data1, 5), 0);

  len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, 1, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 8u);
  EXPECT_EQ(memcmp(out, data2, 8), 0);
}

TEST_F(SlotPageTest, Delete)
{
  uint8_t  data[5] = {1, 2, 3, 4, 5};
  uint16_t slot;
  EXPECT_EQ(slot_page_insert(&page, data, 5, &slot), NOSTR_DB_OK);

  EXPECT_EQ(slot_page_delete(&page, slot), NOSTR_DB_OK);

  uint16_t len = 5;
  EXPECT_EQ(slot_page_read(&page, slot, data, &len), NOSTR_DB_ERROR_NOT_FOUND);

  // Delete again should fail
  EXPECT_EQ(slot_page_delete(&page, slot), NOSTR_DB_ERROR_NOT_FOUND);
}

TEST_F(SlotPageTest, DeleteAndReuse)
{
  uint8_t  data1[5] = {1, 2, 3, 4, 5};
  uint8_t  data2[3] = {10, 20, 30};
  uint16_t s1, s2;

  EXPECT_EQ(slot_page_insert(&page, data1, 5, &s1), NOSTR_DB_OK);
  EXPECT_EQ(slot_page_delete(&page, s1), NOSTR_DB_OK);

  // New insert should reuse the deleted slot
  EXPECT_EQ(slot_page_insert(&page, data2, 3, &s2), NOSTR_DB_OK);
  EXPECT_EQ(s2, s1);  // Reused slot index

  uint8_t  out[5];
  uint16_t len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, s2, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 3u);
  EXPECT_EQ(memcmp(out, data2, 3), 0);
}

TEST_F(SlotPageTest, FreeSpace)
{
  uint16_t initial_free = slot_page_free_space(&page);
  EXPECT_GT(initial_free, 0u);

  uint8_t  data[100];
  uint16_t slot;
  memset(data, 0xAA, 100);
  EXPECT_EQ(slot_page_insert(&page, data, 100, &slot), NOSTR_DB_OK);

  uint16_t after_insert = slot_page_free_space(&page);
  // Should have decreased by at least 100 + slot entry size
  EXPECT_LT(after_insert, initial_free);
}

TEST_F(SlotPageTest, Compact)
{
  uint8_t  data1[100], data2[200], data3[150];
  uint16_t s1, s2, s3;
  memset(data1, 0x11, 100);
  memset(data2, 0x22, 200);
  memset(data3, 0x33, 150);

  EXPECT_EQ(slot_page_insert(&page, data1, 100, &s1), NOSTR_DB_OK);
  EXPECT_EQ(slot_page_insert(&page, data2, 200, &s2), NOSTR_DB_OK);
  EXPECT_EQ(slot_page_insert(&page, data3, 150, &s3), NOSTR_DB_OK);

  // Delete middle record
  EXPECT_EQ(slot_page_delete(&page, s2), NOSTR_DB_OK);

  SlotPageHeader* hdr = (SlotPageHeader*)page.data;
  EXPECT_EQ(hdr->fragmented_space, 200u);

  // Compact
  slot_page_compact(&page);
  EXPECT_EQ(hdr->fragmented_space, 0u);

  // Verify data1 and data3 are still readable
  uint8_t  out[200];
  uint16_t len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, s1, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 100u);
  EXPECT_EQ(memcmp(out, data1, 100), 0);

  len = sizeof(out);
  EXPECT_EQ(slot_page_read(&page, s3, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 150u);
  EXPECT_EQ(memcmp(out, data3, 150), 0);
}

TEST_F(SlotPageTest, PageFull)
{
  // Fill the page
  uint8_t  big_data[3000];
  uint16_t slot;
  memset(big_data, 0xFF, sizeof(big_data));

  EXPECT_EQ(slot_page_insert(&page, big_data, 3000, &slot), NOSTR_DB_OK);

  // Second large insert should fail
  EXPECT_EQ(slot_page_insert(&page, big_data, 3000, &slot),
            NOSTR_DB_ERROR_FULL);
}

TEST_F(SlotPageTest, ReadQueryLength)
{
  uint8_t  data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  uint16_t slot;
  EXPECT_EQ(slot_page_insert(&page, data, 10, &slot), NOSTR_DB_OK);

  // Query length only (out = NULL)
  uint16_t len = 0;
  EXPECT_EQ(slot_page_read(&page, slot, nullptr, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 10u);
}

TEST_F(SlotPageTest, ReadInvalidSlot)
{
  uint16_t len = 10;
  uint8_t  out[10];
  EXPECT_EQ(slot_page_read(&page, 0, out, &len), NOSTR_DB_ERROR_NOT_FOUND);
  EXPECT_EQ(slot_page_read(&page, 100, out, &len), NOSTR_DB_ERROR_NOT_FOUND);
}

// ============================================================================
// Record manager tests (with buffer pool)
// ============================================================================

class RecordManagerTest : public ::testing::Test {
 protected:
  static constexpr const char* TEST_DB_PATH = "/tmp/record_test_db.dat";
  static constexpr uint32_t    POOL_SIZE    = 32;

  DiskManager disk;
  BufferPool  pool;

  void SetUp() override
  {
    fs::remove(TEST_DB_PATH);
    memset(&disk, 0, sizeof(disk));
    memset(&pool, 0, sizeof(pool));
  }

  void TearDown() override
  {
    buffer_pool_shutdown(&pool);
    disk_manager_close(&disk);
    fs::remove(TEST_DB_PATH);
  }

  void init_all()
  {
    ASSERT_EQ(disk_manager_create(&disk, TEST_DB_PATH, 256), NOSTR_DB_OK);
    ASSERT_EQ(buffer_pool_init(&pool, &disk, POOL_SIZE), NOSTR_DB_OK);
  }
};

TEST_F(RecordManagerTest, InsertAndRead)
{
  init_all();

  uint8_t  data[50];
  RecordId rid;
  memset(data, 0xAB, 50);

  EXPECT_EQ(record_insert(&pool, data, 50, &rid), NOSTR_DB_OK);
  EXPECT_NE(rid.page_id, PAGE_ID_NULL);

  uint8_t  out[50];
  uint16_t len = sizeof(out);
  EXPECT_EQ(record_read(&pool, rid, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 50u);
  EXPECT_EQ(memcmp(out, data, 50), 0);
}

TEST_F(RecordManagerTest, InsertMultiple)
{
  init_all();

  RecordId rids[10];
  uint8_t  data[10][20];

  for (int i = 0; i < 10; i++) {
    memset(data[i], (uint8_t)i, 20);
    EXPECT_EQ(record_insert(&pool, data[i], 20, &rids[i]), NOSTR_DB_OK);
  }

  // Read all back
  for (int i = 0; i < 10; i++) {
    uint8_t  out[20];
    uint16_t len = sizeof(out);
    EXPECT_EQ(record_read(&pool, rids[i], out, &len), NOSTR_DB_OK);
    EXPECT_EQ(len, 20u);
    EXPECT_EQ(memcmp(out, data[i], 20), 0);
  }
}

TEST_F(RecordManagerTest, Delete)
{
  init_all();

  uint8_t  data[30];
  RecordId rid;
  memset(data, 0xCC, 30);

  EXPECT_EQ(record_insert(&pool, data, 30, &rid), NOSTR_DB_OK);
  EXPECT_EQ(record_delete(&pool, rid), NOSTR_DB_OK);

  uint16_t len = 30;
  EXPECT_EQ(record_read(&pool, rid, data, &len), NOSTR_DB_ERROR_NOT_FOUND);
}

TEST_F(RecordManagerTest, UpdateInPlace)
{
  init_all();

  uint8_t  data1[40];
  uint8_t  data2[30];  // Smaller - fits in place
  RecordId rid;
  memset(data1, 0xAA, 40);
  memset(data2, 0xBB, 30);

  EXPECT_EQ(record_insert(&pool, data1, 40, &rid), NOSTR_DB_OK);
  page_id_t orig_page = rid.page_id;
  uint16_t  orig_slot = rid.slot_index;

  EXPECT_EQ(record_update(&pool, &rid, data2, 30), NOSTR_DB_OK);
  // Should be in same location (in-place)
  EXPECT_EQ(rid.page_id, orig_page);
  EXPECT_EQ(rid.slot_index, orig_slot);

  uint8_t  out[40];
  uint16_t len = sizeof(out);
  EXPECT_EQ(record_read(&pool, rid, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 30u);
  EXPECT_EQ(memcmp(out, data2, 30), 0);
}

TEST_F(RecordManagerTest, UpdateRelocate)
{
  init_all();

  uint8_t  data1[20];
  uint8_t  data2[100];  // Much larger - needs relocate
  RecordId rid;
  memset(data1, 0xAA, 20);
  memset(data2, 0xBB, 100);

  EXPECT_EQ(record_insert(&pool, data1, 20, &rid), NOSTR_DB_OK);
  EXPECT_EQ(record_update(&pool, &rid, data2, 100), NOSTR_DB_OK);

  uint8_t  out[100];
  uint16_t len = sizeof(out);
  EXPECT_EQ(record_read(&pool, rid, out, &len), NOSTR_DB_OK);
  EXPECT_EQ(len, 100u);
  EXPECT_EQ(memcmp(out, data2, 100), 0);
}

TEST_F(RecordManagerTest, ManyRecordsFillPages)
{
  init_all();

  // Insert many records to force allocation of multiple pages
  RecordId rids[100];
  uint8_t  data[100];
  memset(data, 0x55, 100);

  for (int i = 0; i < 100; i++) {
    data[0] = (uint8_t)i;  // Unique marker
    EXPECT_EQ(record_insert(&pool, data, 100, &rids[i]), NOSTR_DB_OK);
  }

  // Read all back and verify
  for (int i = 0; i < 100; i++) {
    uint8_t  out[100];
    uint16_t len = sizeof(out);
    EXPECT_EQ(record_read(&pool, rids[i], out, &len), NOSTR_DB_OK);
    EXPECT_EQ(len, 100u);
    EXPECT_EQ(out[0], (uint8_t)i);
  }
}

// ============================================================================
// Event serializer tests
// ============================================================================

class EventSerializerTest : public ::testing::Test {
 protected:
  // Use static allocation since NostrEventEntity is huge (>1MB)
  static NostrEventEntity event;

  void SetUp() override
  {
    memset(&event, 0, sizeof(event));

    // Fill with valid hex data
    memset(event.id, 'a', 64);
    event.id[64] = '\0';

    memset(event.pubkey, 'b', 64);
    event.pubkey[64] = '\0';

    memset(event.sig, 'c', 128);
    event.sig[128] = '\0';

    event.kind       = 1;
    event.created_at = 1700000000;

    strcpy(event.content, "Hello, Nostr!");
    event.tag_count = 1;
    strcpy(event.tags[0].key, "p");
    event.tags[0].item_count = 1;
    memset(event.tags[0].values[0], 'd', 64);
    event.tags[0].values[0][64] = '\0';
  }
};

NostrEventEntity EventSerializerTest::event;

TEST_F(EventSerializerTest, SerializeDeserialize)
{
  uint8_t buffer[4096];
  int32_t written = event_serialize(&event, buffer, sizeof(buffer));
  EXPECT_GT(written, (int32_t)sizeof(EventRecord));

  static NostrEventEntity restored;
  memset(&restored, 0, sizeof(restored));
  EXPECT_EQ(event_deserialize(buffer, (uint16_t)written, &restored),
            NOSTR_DB_OK);

  // Compare fields
  EXPECT_STREQ(restored.id, event.id);
  EXPECT_STREQ(restored.pubkey, event.pubkey);
  EXPECT_STREQ(restored.sig, event.sig);
  EXPECT_EQ(restored.kind, event.kind);
  EXPECT_EQ(restored.created_at, event.created_at);
  EXPECT_STREQ(restored.content, event.content);
  EXPECT_EQ(restored.tag_count, event.tag_count);
  EXPECT_STREQ(restored.tags[0].key, event.tags[0].key);
  EXPECT_EQ(restored.tags[0].item_count, event.tags[0].item_count);
  EXPECT_STREQ(restored.tags[0].values[0], event.tags[0].values[0]);
}

TEST_F(EventSerializerTest, RecordHeaderSize)
{
  EXPECT_EQ(sizeof(EventRecord), 152u);
}

TEST_F(EventSerializerTest, SerializeBufferTooSmall)
{
  uint8_t small_buf[10];
  int32_t written = event_serialize(&event, small_buf, sizeof(small_buf));
  EXPECT_EQ(written, -1);
}

TEST_F(EventSerializerTest, EmptyContent)
{
  event.content[0] = '\0';
  event.tag_count  = 0;

  uint8_t buffer[4096];
  int32_t written = event_serialize(&event, buffer, sizeof(buffer));
  EXPECT_GT(written, 0);

  static NostrEventEntity restored;
  memset(&restored, 0, sizeof(restored));
  EXPECT_EQ(event_deserialize(buffer, (uint16_t)written, &restored),
            NOSTR_DB_OK);
  EXPECT_STREQ(restored.content, "");
  EXPECT_EQ(restored.tag_count, 0u);
}

// ============================================================================
// Integration: serialize event, insert as record, read back, deserialize
// ============================================================================

class RecordEventIntegrationTest : public ::testing::Test {
 protected:
  static constexpr const char* TEST_DB_PATH = "/tmp/record_event_test.dat";
  static constexpr uint32_t    POOL_SIZE    = 32;

  DiskManager disk;
  BufferPool  pool;
  static NostrEventEntity event;

  void SetUp() override
  {
    fs::remove(TEST_DB_PATH);
    memset(&disk, 0, sizeof(disk));
    memset(&pool, 0, sizeof(pool));

    memset(&event, 0, sizeof(event));
    memset(event.id, 'a', 64);
    event.id[64] = '\0';
    memset(event.pubkey, 'b', 64);
    event.pubkey[64] = '\0';
    memset(event.sig, 'c', 128);
    event.sig[128] = '\0';
    event.kind       = 1;
    event.created_at = 1700000000;
    strcpy(event.content, "Test content for record integration");
    event.tag_count = 0;
  }

  void TearDown() override
  {
    buffer_pool_shutdown(&pool);
    disk_manager_close(&disk);
    fs::remove(TEST_DB_PATH);
  }

  void init_all()
  {
    ASSERT_EQ(disk_manager_create(&disk, TEST_DB_PATH, 256), NOSTR_DB_OK);
    ASSERT_EQ(buffer_pool_init(&pool, &disk, POOL_SIZE), NOSTR_DB_OK);
  }
};

NostrEventEntity RecordEventIntegrationTest::event;

TEST_F(RecordEventIntegrationTest, SerializeInsertReadDeserialize)
{
  init_all();

  // Serialize event
  uint8_t buffer[4096];
  int32_t written = event_serialize(&event, buffer, sizeof(buffer));
  ASSERT_GT(written, 0);

  // Insert as record
  RecordId rid;
  EXPECT_EQ(record_insert(&pool, buffer, (uint16_t)written, &rid),
            NOSTR_DB_OK);

  // Read record back
  uint8_t  read_buf[4096];
  uint16_t read_len = sizeof(read_buf);
  EXPECT_EQ(record_read(&pool, rid, read_buf, &read_len), NOSTR_DB_OK);
  EXPECT_EQ(read_len, (uint16_t)written);

  // Deserialize
  static NostrEventEntity restored;
  memset(&restored, 0, sizeof(restored));
  EXPECT_EQ(event_deserialize(read_buf, read_len, &restored), NOSTR_DB_OK);

  EXPECT_STREQ(restored.id, event.id);
  EXPECT_STREQ(restored.pubkey, event.pubkey);
  EXPECT_STREQ(restored.sig, event.sig);
  EXPECT_EQ(restored.kind, event.kind);
  EXPECT_EQ(restored.created_at, event.created_at);
  EXPECT_STREQ(restored.content, event.content);
}

TEST_F(RecordEventIntegrationTest, MultipleEvents)
{
  init_all();

  RecordId rids[5];

  for (int i = 0; i < 5; i++) {
    // Change ID for each event
    char hex_char = 'a' + i;
    memset(event.id, hex_char, 64);
    event.kind = (uint32_t)i;

    uint8_t buffer[4096];
    int32_t written = event_serialize(&event, buffer, sizeof(buffer));
    ASSERT_GT(written, 0);
    EXPECT_EQ(record_insert(&pool, buffer, (uint16_t)written, &rids[i]),
              NOSTR_DB_OK);
  }

  // Read all back
  for (int i = 0; i < 5; i++) {
    uint8_t  read_buf[4096];
    uint16_t read_len = sizeof(read_buf);
    EXPECT_EQ(record_read(&pool, rids[i], read_buf, &read_len), NOSTR_DB_OK);

    static NostrEventEntity restored;
    memset(&restored, 0, sizeof(restored));
    EXPECT_EQ(event_deserialize(read_buf, read_len, &restored), NOSTR_DB_OK);
    EXPECT_EQ(restored.kind, (uint32_t)i);

    char expected_char = 'a' + i;
    char expected_id[65];
    memset(expected_id, expected_char, 64);
    expected_id[64] = '\0';
    EXPECT_STREQ(restored.id, expected_id);
  }
}
