#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <numeric>
#include <unistd.h>
#include <vector>

extern "C" {

// ============================================================================
// Type redefinitions (avoid _Static_assert in C++ compilation)
// ============================================================================

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

#define SLOT_PAGE_HEADER_SIZE sizeof(SlotPageHeader)

typedef struct {
  page_id_t page_id;
  uint16_t  slot_index;
} RecordId;

#define RECORD_ID_NULL (RecordId){PAGE_ID_NULL, 0}

// B+ tree types
typedef int32_t (*BTreeKeyCompare)(const void* a, const void* b,
                                   uint16_t key_size);

typedef enum {
  BTREE_KEY_BYTES32  = 0,
  BTREE_KEY_INT64    = 1,
  BTREE_KEY_UINT32   = 2,
  BTREE_KEY_COMPOSITE = 3,
} BTreeKeyType;

typedef struct {
  page_id_t root_page;
  uint32_t  height;
  uint32_t  entry_count;
  uint32_t  leaf_count;
  uint32_t  inner_count;
  uint16_t  key_size;
  uint16_t  value_size;
  uint8_t   key_type;
  uint8_t   flags;
  uint8_t   reserved[10];
} BTreeMeta;

typedef struct {
  uint16_t  key_count;
  uint8_t   is_leaf;
  uint8_t   reserved;
  page_id_t right_sibling;
} BTreeNodeHeader;

#define BTREE_NODE_HEADER_OFFSET SLOT_PAGE_HEADER_SIZE
#define BTREE_DATA_OFFSET (SLOT_PAGE_HEADER_SIZE + sizeof(BTreeNodeHeader))
#define BTREE_NODE_SPACE (DB_PAGE_SIZE - BTREE_DATA_OFFSET)

#define BTREE_LEAF_MAX_KEYS(ks, vs) \
  ((uint16_t)(BTREE_NODE_SPACE / ((ks) + (vs))))

#define BTREE_INNER_MAX_KEYS(ks) \
  ((uint16_t)((BTREE_NODE_SPACE - sizeof(page_id_t)) / ((ks) + sizeof(page_id_t))))

typedef struct {
  BufferPool*     pool;
  page_id_t       meta_page;
  BTreeMeta       meta;
  BTreeKeyCompare compare;
} BTree;

typedef bool (*BTreeScanCallback)(const void* key, const void* value,
                                  void* user_data);

#define BTREE_MAX_HEIGHT 16

typedef struct {
  page_id_t page_ids[BTREE_MAX_HEIGHT];
  uint16_t  positions[BTREE_MAX_HEIGHT];
  uint16_t  depth;
} BTreePath;

typedef struct {
  page_id_t next_page;
  uint16_t  entry_count;
  uint16_t  reserved;
} BTreeOverflowHeader;

#define BTREE_OVERFLOW_ENTRY_SIZE sizeof(RecordId)
#define BTREE_OVERFLOW_MAX_ENTRIES \
  ((DB_PAGE_SIZE - sizeof(BTreeOverflowHeader)) / BTREE_OVERFLOW_ENTRY_SIZE)

// Disk manager API
NostrDBError disk_manager_create(DiskManager* dm, const char* path,
                                 uint32_t initial_pages);
void         disk_manager_close(DiskManager* dm);
NostrDBError disk_free_page(DiskManager* dm, page_id_t page_id);

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

// B+ tree API
NostrDBError btree_create(BTree* tree, BufferPool* pool, uint16_t key_size,
                          uint16_t value_size, BTreeKeyType key_type);
NostrDBError btree_open(BTree* tree, BufferPool* pool, page_id_t meta_page);
NostrDBError btree_flush_meta(BTree* tree);
NostrDBError btree_insert(BTree* tree, const void* key, const void* value);
NostrDBError btree_search(BTree* tree, const void* key, void* value_out);
NostrDBError btree_delete(BTree* tree, const void* key);
NostrDBError btree_range_scan(BTree* tree, const void* min_key,
                              const void* max_key,
                              BTreeScanCallback callback, void* user_data);
NostrDBError btree_insert_dup(BTree* tree, const void* key, RecordId rid);
NostrDBError btree_scan_key(BTree* tree, const void* key,
                            BTreeScanCallback callback, void* user_data);
NostrDBError btree_delete_dup(BTree* tree, const void* key, RecordId rid);

void btree_node_init_leaf(PageData* page, page_id_t page_id);
void btree_node_init_inner(PageData* page, page_id_t page_id);
uint16_t btree_node_search_key(const PageData* page, const void* key,
                               uint16_t key_size, BTreeKeyCompare compare);
const void* btree_node_key_at(const PageData* page, uint16_t pos,
                              uint16_t key_size);
const void* btree_node_value_at(const PageData* page, uint16_t pos,
                                uint16_t key_size, uint16_t value_size,
                                uint16_t max_keys);
page_id_t btree_node_child_at(const PageData* page, uint16_t pos,
                              uint16_t key_size, uint16_t max_keys);

int32_t btree_compare_bytes32(const void* a, const void* b,
                              uint16_t key_size);
int32_t btree_compare_int64(const void* a, const void* b,
                            uint16_t key_size);
int32_t btree_compare_uint32(const void* a, const void* b,
                             uint16_t key_size);
int32_t btree_compare_composite_pk_kind(const void* a, const void* b,
                                        uint16_t key_size);
int32_t btree_compare_composite_tag(const void* a, const void* b,
                                    uint16_t key_size);
BTreeKeyCompare btree_get_comparator(BTreeKeyType key_type);

}  // extern "C"

// ============================================================================
// Test fixture
// ============================================================================
class BTreeTest : public ::testing::Test {
 protected:
  DiskManager dm;
  BufferPool  pool;
  BTree       tree;
  char        path[256];

  void SetUp() override {
    snprintf(path, sizeof(path), "/tmp/nostr_btree_test_%d.dat", getpid());
    ASSERT_EQ(NOSTR_DB_OK, disk_manager_create(&dm, path, 4096));
    ASSERT_EQ(NOSTR_DB_OK, buffer_pool_init(&pool, &dm, 512));
  }

  void TearDown() override {
    buffer_pool_shutdown(&pool);
    disk_manager_close(&dm);
    unlink(path);
  }
};

// ============================================================================
// Compare function tests
// ============================================================================
TEST(BTreeCompareTest, Bytes32) {
  uint8_t a[32], b[32];
  memset(a, 0, 32);
  memset(b, 0, 32);
  EXPECT_EQ(0, btree_compare_bytes32(a, b, 32));
  a[0] = 1;
  EXPECT_GT(btree_compare_bytes32(a, b, 32), 0);
  EXPECT_LT(btree_compare_bytes32(b, a, 32), 0);
}

TEST(BTreeCompareTest, Int64) {
  int64_t a = 100, b = 200;
  EXPECT_LT(btree_compare_int64(&a, &b, 8), 0);
  EXPECT_GT(btree_compare_int64(&b, &a, 8), 0);
  EXPECT_EQ(0, btree_compare_int64(&a, &a, 8));
}

TEST(BTreeCompareTest, Uint32) {
  uint32_t a = 10, b = 20;
  EXPECT_LT(btree_compare_uint32(&a, &b, 4), 0);
  EXPECT_GT(btree_compare_uint32(&b, &a, 4), 0);
  EXPECT_EQ(0, btree_compare_uint32(&a, &a, 4));
}

TEST(BTreeCompareTest, CompositePkKind) {
  uint8_t a[36], b[36];
  memset(a, 0, 36);
  memset(b, 0, 36);
  EXPECT_EQ(0, btree_compare_composite_pk_kind(a, b, 36));

  // Different pubkey
  a[0] = 1;
  EXPECT_GT(btree_compare_composite_pk_kind(a, b, 36), 0);

  // Same pubkey, different kind
  memset(a, 0, 36);
  uint32_t kind_a = 10, kind_b = 20;
  memcpy(a + 32, &kind_a, 4);
  memcpy(b + 32, &kind_b, 4);
  EXPECT_LT(btree_compare_composite_pk_kind(a, b, 36), 0);
}

TEST(BTreeCompareTest, CompositeTag) {
  uint8_t a[33], b[33];
  memset(a, 0, 33);
  memset(b, 0, 33);
  a[0] = 'e';
  b[0] = 'p';
  EXPECT_LT(btree_compare_composite_tag(a, b, 33), 0);

  // Same tag name, different value
  b[0] = 'e';
  a[1] = 0x01;
  EXPECT_GT(btree_compare_composite_tag(a, b, 33), 0);
}

// ============================================================================
// Node operation tests
// ============================================================================
TEST(BTreeNodeTest, InitLeaf) {
  PageData page;
  btree_node_init_leaf(&page, 42);

  SlotPageHeader* hdr = (SlotPageHeader*)page.data;
  EXPECT_EQ(42u, hdr->page_id);
  EXPECT_EQ(PAGE_TYPE_BTREE_LEAF, hdr->page_type);

  BTreeNodeHeader* node =
    (BTreeNodeHeader*)(page.data + BTREE_NODE_HEADER_OFFSET);
  EXPECT_EQ(0, node->key_count);
  EXPECT_EQ(1, node->is_leaf);
  EXPECT_EQ(PAGE_ID_NULL, node->right_sibling);
}

TEST(BTreeNodeTest, InitInner) {
  PageData page;
  btree_node_init_inner(&page, 99);

  SlotPageHeader* hdr = (SlotPageHeader*)page.data;
  EXPECT_EQ(99u, hdr->page_id);
  EXPECT_EQ(PAGE_TYPE_BTREE_INNER, hdr->page_type);

  BTreeNodeHeader* node =
    (BTreeNodeHeader*)(page.data + BTREE_NODE_HEADER_OFFSET);
  EXPECT_EQ(0, node->key_count);
  EXPECT_EQ(0, node->is_leaf);
}

TEST(BTreeNodeTest, BinarySearch) {
  PageData page;
  btree_node_init_leaf(&page, 1);

  // Manually insert sorted uint32 keys: 10, 20, 30
  uint16_t key_size = 4;
  uint8_t* keys = page.data + BTREE_DATA_OFFSET;
  BTreeNodeHeader* node =
    (BTreeNodeHeader*)(page.data + BTREE_NODE_HEADER_OFFSET);

  uint32_t k1 = 10, k2 = 20, k3 = 30;
  memcpy(keys + 0, &k1, 4);
  memcpy(keys + 4, &k2, 4);
  memcpy(keys + 8, &k3, 4);
  node->key_count = 3;

  // Search for existing key
  uint16_t pos = btree_node_search_key(&page, &k2, key_size,
                                       btree_compare_uint32);
  EXPECT_EQ(1, pos);

  // Search for non-existing key (between 20 and 30)
  uint32_t k25 = 25;
  pos = btree_node_search_key(&page, &k25, key_size, btree_compare_uint32);
  EXPECT_EQ(2, pos);

  // Search for key smaller than all
  uint32_t k5 = 5;
  pos = btree_node_search_key(&page, &k5, key_size, btree_compare_uint32);
  EXPECT_EQ(0, pos);

  // Search for key larger than all
  uint32_t k40 = 40;
  pos = btree_node_search_key(&page, &k40, key_size, btree_compare_uint32);
  EXPECT_EQ(3, pos);
}

// ============================================================================
// B+ tree CRUD tests
// ============================================================================
TEST_F(BTreeTest, CreateAndOpen) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));
  EXPECT_NE(PAGE_ID_NULL, tree.meta_page);
  EXPECT_EQ(0u, tree.meta.entry_count);
  EXPECT_EQ(0u, tree.meta.height);

  // Reopen
  BTree tree2;
  ASSERT_EQ(NOSTR_DB_OK, btree_open(&tree2, &pool, tree.meta_page));
  EXPECT_EQ(tree.meta.key_size, tree2.meta.key_size);
  EXPECT_EQ(tree.meta.value_size, tree2.meta.value_size);
}

TEST_F(BTreeTest, InsertAndSearch) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId rid = {10, 5};
  ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &key, &rid));

  RecordId result;
  ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &key, &result));
  EXPECT_EQ(10u, result.page_id);
  EXPECT_EQ(5, result.slot_index);
}

TEST_F(BTreeTest, SearchNotFound) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId result;
  EXPECT_EQ(NOSTR_DB_ERROR_NOT_FOUND, btree_search(&tree, &key, &result));
}

TEST_F(BTreeTest, DuplicateInsert) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId rid = {10, 5};
  ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &key, &rid));
  EXPECT_EQ(NOSTR_DB_ERROR_DUPLICATE, btree_insert(&tree, &key, &rid));
}

TEST_F(BTreeTest, InsertMultiple) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 100; i++) {
    RecordId rid = {i, (uint16_t)(i % 256)};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  EXPECT_EQ(100u, tree.meta.entry_count);

  // Verify all entries
  for (uint32_t i = 1; i <= 100; i++) {
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &i, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

TEST_F(BTreeTest, InsertReverseOrder) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 100; i >= 1; i--) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  for (uint32_t i = 1; i <= 100; i++) {
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &i, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

TEST_F(BTreeTest, Delete) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId rid = {10, 5};
  ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &key, &rid));
  ASSERT_EQ(NOSTR_DB_OK, btree_delete(&tree, &key));

  RecordId result;
  EXPECT_EQ(NOSTR_DB_ERROR_NOT_FOUND, btree_search(&tree, &key, &result));
  EXPECT_EQ(0u, tree.meta.entry_count);
}

TEST_F(BTreeTest, DeleteNotFound) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t key = 42;
  EXPECT_EQ(NOSTR_DB_ERROR_NOT_FOUND, btree_delete(&tree, &key));
}

TEST_F(BTreeTest, DeleteMultiple) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 50; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  // Delete even numbers
  for (uint32_t i = 2; i <= 50; i += 2) {
    ASSERT_EQ(NOSTR_DB_OK, btree_delete(&tree, &i));
  }

  // Verify odd numbers still exist, even numbers gone
  for (uint32_t i = 1; i <= 50; i++) {
    RecordId result;
    if (i % 2 == 1) {
      ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &i, &result));
      EXPECT_EQ(i, result.page_id);
    } else {
      EXPECT_EQ(NOSTR_DB_ERROR_NOT_FOUND, btree_search(&tree, &i, &result));
    }
  }
}

// ============================================================================
// Split tests (force splits by inserting many entries)
// ============================================================================
TEST_F(BTreeTest, LeafSplit) {
  // uint32 key (4 bytes) + RecordId value (6 bytes) = 10 bytes per entry
  // max_leaf = 4064 / 10 = 406
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  // Insert enough to force at least one split
  uint32_t count = 500;
  for (uint32_t i = 1; i <= count; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  EXPECT_GT(tree.meta.height, 1u);
  EXPECT_GT(tree.meta.leaf_count, 1u);
  EXPECT_EQ(count, tree.meta.entry_count);

  // Verify all entries
  for (uint32_t i = 1; i <= count; i++) {
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &i, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

TEST_F(BTreeTest, MultipleSplits) {
  // 32-byte key + 6-byte value = 38 bytes per entry
  // max_leaf = 4064 / 38 = 106
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 32, sizeof(RecordId),
                         BTREE_KEY_BYTES32));

  uint32_t count = 500;
  for (uint32_t i = 1; i <= count; i++) {
    uint8_t  key[32];
    memset(key, 0, 32);
    // Use big-endian format for ordered insertion
    key[0] = (uint8_t)(i >> 24);
    key[1] = (uint8_t)(i >> 16);
    key[2] = (uint8_t)(i >> 8);
    key[3] = (uint8_t)(i);
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, key, &rid));
  }

  EXPECT_GT(tree.meta.leaf_count, 4u);
  EXPECT_EQ(count, tree.meta.entry_count);

  // Verify all entries
  for (uint32_t i = 1; i <= count; i++) {
    uint8_t key[32];
    memset(key, 0, 32);
    key[0] = (uint8_t)(i >> 24);
    key[1] = (uint8_t)(i >> 16);
    key[2] = (uint8_t)(i >> 8);
    key[3] = (uint8_t)(i);
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, key, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

// ============================================================================
// Range scan tests
// ============================================================================
struct ScanResult {
  std::vector<uint32_t> keys;
  std::vector<RecordId> values;
};

static bool scan_callback(const void* key, const void* value,
                          void* user_data)
{
  auto* result = (ScanResult*)user_data;
  uint32_t k;
  memcpy(&k, key, sizeof(uint32_t));
  result->keys.push_back(k);

  RecordId v;
  memcpy(&v, value, sizeof(RecordId));
  result->values.push_back(v);
  return true;
}

TEST_F(BTreeTest, RangeScanAll) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 50; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  ScanResult result;
  ASSERT_EQ(NOSTR_DB_OK,
            btree_range_scan(&tree, nullptr, nullptr, scan_callback, &result));
  EXPECT_EQ(50u, result.keys.size());

  // Verify sorted order
  for (size_t i = 0; i < result.keys.size(); i++) {
    EXPECT_EQ(i + 1, result.keys[i]);
  }
}

TEST_F(BTreeTest, RangeScanBounded) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 100; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  uint32_t min = 20, max = 30;
  ScanResult result;
  ASSERT_EQ(NOSTR_DB_OK,
            btree_range_scan(&tree, &min, &max, scan_callback, &result));

  EXPECT_EQ(11u, result.keys.size());  // 20..30 inclusive
  EXPECT_EQ(20u, result.keys.front());
  EXPECT_EQ(30u, result.keys.back());
}

TEST_F(BTreeTest, RangeScanEmpty) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  ScanResult result;
  ASSERT_EQ(NOSTR_DB_OK,
            btree_range_scan(&tree, nullptr, nullptr, scan_callback, &result));
  EXPECT_EQ(0u, result.keys.size());
}

static bool scan_limit_callback(const void* key, const void* value,
                                void* user_data)
{
  auto* result = (ScanResult*)user_data;
  uint32_t k;
  memcpy(&k, key, sizeof(uint32_t));
  result->keys.push_back(k);
  return result->keys.size() < 5;  // Stop after 5 entries
}

TEST_F(BTreeTest, RangeScanWithLimit) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 100; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  ScanResult result;
  ASSERT_EQ(NOSTR_DB_OK, btree_range_scan(&tree, nullptr, nullptr,
                                           scan_limit_callback, &result));
  EXPECT_EQ(5u, result.keys.size());
}

// ============================================================================
// Int64 key tests (for timeline index)
// ============================================================================
TEST_F(BTreeTest, Int64Keys) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 8, sizeof(RecordId), BTREE_KEY_INT64));

  for (int64_t i = -50; i <= 50; i++) {
    RecordId rid = {(uint32_t)(i + 100), 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  // Search
  int64_t key = -25;
  RecordId result;
  ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &key, &result));
  EXPECT_EQ(75u, result.page_id);

  key = 0;
  ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &key, &result));
  EXPECT_EQ(100u, result.page_id);
}

// ============================================================================
// 32-byte key tests (for ID/pubkey indexes)
// ============================================================================
TEST_F(BTreeTest, Bytes32Keys) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 32, sizeof(RecordId),
                         BTREE_KEY_BYTES32));

  for (uint32_t i = 0; i < 200; i++) {
    uint8_t key[32];
    memset(key, 0, 32);
    key[31] = (uint8_t)(i & 0xFF);
    key[30] = (uint8_t)((i >> 8) & 0xFF);
    RecordId rid = {i + 1, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, key, &rid));
  }

  EXPECT_EQ(200u, tree.meta.entry_count);

  for (uint32_t i = 0; i < 200; i++) {
    uint8_t key[32];
    memset(key, 0, 32);
    key[31] = (uint8_t)(i & 0xFF);
    key[30] = (uint8_t)((i >> 8) & 0xFF);
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, key, &result));
    EXPECT_EQ(i + 1, result.page_id);
  }
}

// ============================================================================
// Duplicate key overflow tests
// ============================================================================
struct DupScanResult {
  std::vector<RecordId> rids;
};

static bool dup_scan_callback(const void* key, const void* value,
                              void* user_data)
{
  auto*          result = (DupScanResult*)user_data;
  const RecordId* rid = (const RecordId*)value;
  result->rids.push_back(*rid);
  return true;
}

TEST_F(BTreeTest, DupInsertAndScan) {
  // value_size = sizeof(page_id_t) for overflow chain head
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(page_id_t),
                         BTREE_KEY_UINT32));

  uint32_t key = 42;
  for (uint32_t i = 1; i <= 10; i++) {
    RecordId rid = {i, (uint16_t)i};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid));
  }

  DupScanResult result;
  ASSERT_EQ(NOSTR_DB_OK,
            btree_scan_key(&tree, &key, dup_scan_callback, &result));
  EXPECT_EQ(10u, result.rids.size());
}

TEST_F(BTreeTest, DupDeleteSingle) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(page_id_t),
                         BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId rid1 = {1, 1};
  RecordId rid2 = {2, 2};
  RecordId rid3 = {3, 3};

  ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid1));
  ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid2));
  ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid3));

  // Delete rid2
  ASSERT_EQ(NOSTR_DB_OK, btree_delete_dup(&tree, &key, rid2));

  DupScanResult result;
  ASSERT_EQ(NOSTR_DB_OK,
            btree_scan_key(&tree, &key, dup_scan_callback, &result));
  EXPECT_EQ(2u, result.rids.size());
}

TEST_F(BTreeTest, DupDeleteAll) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(page_id_t),
                         BTREE_KEY_UINT32));

  uint32_t key = 42;
  RecordId rid1 = {1, 1};
  RecordId rid2 = {2, 2};

  ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid1));
  ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &key, rid2));

  ASSERT_EQ(NOSTR_DB_OK, btree_delete_dup(&tree, &key, rid1));
  ASSERT_EQ(NOSTR_DB_OK, btree_delete_dup(&tree, &key, rid2));

  // Key should be removed from the B+ tree
  page_id_t dummy;
  EXPECT_EQ(NOSTR_DB_ERROR_NOT_FOUND, btree_search(&tree, &key, &dummy));
}

TEST_F(BTreeTest, DupMultipleKeys) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(page_id_t),
                         BTREE_KEY_UINT32));

  // Insert under 3 different keys
  for (uint32_t k = 1; k <= 3; k++) {
    for (uint32_t i = 1; i <= 5; i++) {
      RecordId rid = {k * 100 + i, 0};
      ASSERT_EQ(NOSTR_DB_OK, btree_insert_dup(&tree, &k, rid));
    }
  }

  for (uint32_t k = 1; k <= 3; k++) {
    DupScanResult result;
    ASSERT_EQ(NOSTR_DB_OK,
              btree_scan_key(&tree, &k, dup_scan_callback, &result));
    EXPECT_EQ(5u, result.rids.size());
  }
}

// ============================================================================
// Stress test: large number of entries forcing many splits
// ============================================================================
TEST_F(BTreeTest, StressInsertSearch) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  uint32_t count = 2000;
  for (uint32_t i = 1; i <= count; i++) {
    RecordId rid = {i, (uint16_t)(i & 0xFFFF)};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  EXPECT_EQ(count, tree.meta.entry_count);
  EXPECT_GE(tree.meta.height, 2u);

  // Verify all entries
  for (uint32_t i = 1; i <= count; i++) {
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree, &i, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

// ============================================================================
// Reopen test: metadata persistence
// ============================================================================
TEST_F(BTreeTest, Reopen) {
  ASSERT_EQ(NOSTR_DB_OK,
            btree_create(&tree, &pool, 4, sizeof(RecordId), BTREE_KEY_UINT32));

  for (uint32_t i = 1; i <= 50; i++) {
    RecordId rid = {i, 0};
    ASSERT_EQ(NOSTR_DB_OK, btree_insert(&tree, &i, &rid));
  }

  page_id_t meta_page = tree.meta_page;

  // Reopen
  BTree tree2;
  ASSERT_EQ(NOSTR_DB_OK, btree_open(&tree2, &pool, meta_page));
  EXPECT_EQ(50u, tree2.meta.entry_count);
  EXPECT_EQ(tree.meta.height, tree2.meta.height);

  // Verify data
  for (uint32_t i = 1; i <= 50; i++) {
    RecordId result;
    ASSERT_EQ(NOSTR_DB_OK, btree_search(&tree2, &i, &result));
    EXPECT_EQ(i, result.page_id);
  }
}

// ============================================================================
// Type size assertions
// ============================================================================
TEST(BTreeTypeSizeTest, Sizes) {
  EXPECT_EQ(36u, sizeof(BTreeMeta));
  EXPECT_EQ(8u, sizeof(BTreeNodeHeader));
  EXPECT_EQ(8u, sizeof(BTreeOverflowHeader));
}

TEST(BTreeTypeSizeTest, LeafCapacity) {
  // uint32 key (4B) + RecordId value (6B) = 10B per entry
  uint16_t max = BTREE_LEAF_MAX_KEYS(4, 6);
  EXPECT_GT(max, 300u);  // Should be ~406

  // bytes32 key (32B) + RecordId value (6B) = 38B per entry
  max = BTREE_LEAF_MAX_KEYS(32, 6);
  EXPECT_GT(max, 100u);  // Should be ~106
}

TEST(BTreeTypeSizeTest, InnerCapacity) {
  // uint32 key (4B) → inner = (4064-4)/(4+4) = 507
  uint16_t max = BTREE_INNER_MAX_KEYS(4);
  EXPECT_GT(max, 400u);

  // bytes32 key (32B) → inner = (4064-4)/(32+4) = 112
  max = BTREE_INNER_MAX_KEYS(32);
  EXPECT_GT(max, 100u);
}
