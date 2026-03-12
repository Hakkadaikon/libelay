#include "query_engine.h"

#include "../../../arch/memory.h"
#include "../../../arch/mmap.h"

// ============================================================================
// Internal: Allocate memory via anonymous mmap
// ============================================================================
static void* qr_alloc(size_t size)
{
  if (size == 0) return NULL;
  void* ptr = internal_mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) return NULL;
  return ptr;
}

static void qr_free(void* ptr, size_t size)
{
  if (!is_null(ptr) && size > 0) {
    internal_munmap(ptr, size);
  }
}

// ============================================================================
// Internal: Bloom filter hash (two independent hashes from RecordId)
// ============================================================================
static inline uint32_t bloom_hash1(RecordId rid)
{
  return (uint32_t)(rid.page_id * 2654435761u + rid.slot_index * 40503u);
}

static inline uint32_t bloom_hash2(RecordId rid)
{
  return (uint32_t)(rid.page_id * 1103515245u + rid.slot_index * 12345u);
}

static inline bool bloom_test(const uint64_t bloom[64], RecordId rid)
{
  uint32_t h1  = bloom_hash1(rid) % 4096;
  uint32_t h2  = bloom_hash2(rid) % 4096;
  uint32_t i1  = h1 / 64;
  uint32_t b1  = h1 % 64;
  uint32_t i2  = h2 / 64;
  uint32_t b2  = h2 % 64;
  return (bloom[i1] & (1ULL << b1)) && (bloom[i2] & (1ULL << b2));
}

static inline void bloom_set(uint64_t bloom[64], RecordId rid)
{
  uint32_t h1 = bloom_hash1(rid) % 4096;
  uint32_t h2 = bloom_hash2(rid) % 4096;
  bloom[h1 / 64] |= (1ULL << (h1 % 64));
  bloom[h2 / 64] |= (1ULL << (h2 % 64));
}

// ============================================================================
// query_result_create
// ============================================================================
QueryResultSet* query_result_create(uint32_t capacity)
{
  if (capacity == 0) capacity = NOSTR_DB_RESULT_DEFAULT_CAPACITY;

  QueryResultSet* rs = (QueryResultSet*)qr_alloc(sizeof(QueryResultSet));
  if (is_null(rs)) return NULL;

  rs->rids = (RecordId*)qr_alloc(capacity * sizeof(RecordId));
  if (is_null(rs->rids)) {
    qr_free(rs, sizeof(QueryResultSet));
    return NULL;
  }

  rs->created_at = (int64_t*)qr_alloc(capacity * sizeof(int64_t));
  if (is_null(rs->created_at)) {
    qr_free(rs->rids, capacity * sizeof(RecordId));
    qr_free(rs, sizeof(QueryResultSet));
    return NULL;
  }

  rs->count    = 0;
  rs->capacity = capacity;
  internal_memset(rs->bloom, 0, sizeof(rs->bloom));

  return rs;
}

// ============================================================================
// query_result_free
// ============================================================================
void query_result_free(QueryResultSet* rs)
{
  if (is_null(rs)) return;

  if (!is_null(rs->rids)) {
    qr_free(rs->rids, rs->capacity * sizeof(RecordId));
  }
  if (!is_null(rs->created_at)) {
    qr_free(rs->created_at, rs->capacity * sizeof(int64_t));
  }
  qr_free(rs, sizeof(QueryResultSet));
}

// ============================================================================
// query_result_add: Add RecordId with bloom-filter dedup
// Returns 0 on success, 1 if duplicate, -1 on error
// ============================================================================
int32_t query_result_add(QueryResultSet* rs, RecordId rid, int64_t created_at)
{
  require_not_null(rs, -1);

  // Bloom filter quick check
  if (bloom_test(rs->bloom, rid)) {
    // Possible duplicate — linear scan to confirm
    for (uint32_t i = 0; i < rs->count; i++) {
      if (rs->rids[i].page_id == rid.page_id &&
          rs->rids[i].slot_index == rid.slot_index) {
        return 1;
      }
    }
  }

  // Grow if needed
  if (rs->count >= rs->capacity) {
    uint32_t  new_cap  = rs->capacity * 2;
    RecordId* new_rids = (RecordId*)qr_alloc(new_cap * sizeof(RecordId));
    if (is_null(new_rids)) return -1;

    int64_t* new_ca = (int64_t*)qr_alloc(new_cap * sizeof(int64_t));
    if (is_null(new_ca)) {
      qr_free(new_rids, new_cap * sizeof(RecordId));
      return -1;
    }

    internal_memcpy(new_rids, rs->rids, rs->count * sizeof(RecordId));
    internal_memcpy(new_ca, rs->created_at, rs->count * sizeof(int64_t));

    qr_free(rs->rids, rs->capacity * sizeof(RecordId));
    qr_free(rs->created_at, rs->capacity * sizeof(int64_t));

    rs->rids       = new_rids;
    rs->created_at = new_ca;
    rs->capacity   = new_cap;
  }

  rs->rids[rs->count]       = rid;
  rs->created_at[rs->count] = created_at;
  rs->count++;
  bloom_set(rs->bloom, rid);

  return 0;
}

// ============================================================================
// query_result_sort: Descending by created_at (newest first)
// ============================================================================
int32_t query_result_sort(QueryResultSet* rs)
{
  require_not_null(rs, -1);

  // Insertion sort (good for typically small result sets)
  for (uint32_t i = 1; i < rs->count; i++) {
    int64_t  key_time = rs->created_at[i];
    RecordId key_rid  = rs->rids[i];
    int32_t  j        = (int32_t)i - 1;

    while (j >= 0 && rs->created_at[j] < key_time) {
      rs->created_at[j + 1] = rs->created_at[j];
      rs->rids[j + 1]       = rs->rids[j];
      j--;
    }

    rs->created_at[j + 1] = key_time;
    rs->rids[j + 1]       = key_rid;
  }

  return 0;
}

// ============================================================================
// query_result_apply_limit
// ============================================================================
void query_result_apply_limit(QueryResultSet* rs, uint32_t limit)
{
  if (is_null(rs) || limit == 0) return;
  if (rs->count > limit) rs->count = limit;
}
