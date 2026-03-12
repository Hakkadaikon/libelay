#include "../../arch/memory.h"
#include "../../arch/mmap.h"
#include "db.h"
#include "db_internal.h"
#include "query/db_query_types.h"
#include "query/query_engine.h"

// ============================================================================
// nostr_db_filter_init
// ============================================================================
void nostr_db_filter_init(NostrDBFilter* filter)
{
  if (is_null(filter)) return;
  internal_memset(filter, 0, sizeof(NostrDBFilter));
}

// ============================================================================
// nostr_db_filter_validate
// ============================================================================
bool nostr_db_filter_validate(const NostrDBFilter* filter)
{
  require_not_null(filter, false);

  // Check time range consistency
  if (filter->since > 0 && filter->until > 0 && filter->since > filter->until) {
    return false;
  }

  return true;
}

// ============================================================================
// nostr_db_filter_is_empty
// ============================================================================
bool nostr_db_filter_is_empty(const NostrDBFilter* filter)
{
  require_not_null(filter, true);

  return filter->ids_count == 0 && filter->authors_count == 0 &&
         filter->kinds_count == 0 && filter->tags_count == 0 &&
         filter->since == 0 && filter->until == 0;
}

// ============================================================================
// nostr_db_query_select_strategy
// ============================================================================
NostrDBQueryStrategy nostr_db_query_select_strategy(const NostrDBFilter* filter)
{
  if (is_null(filter)) return NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN;

  if (filter->ids_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_ID;
  if (filter->tags_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_TAG;
  if (filter->authors_count > 0 && filter->kinds_count > 0)
    return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND;
  if (filter->authors_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY;
  if (filter->kinds_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_KIND;

  return NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN;
}

// ============================================================================
// Helper: Pack RecordId into uint64_t offset
// ============================================================================
static inline uint64_t pack_record_id(RecordId rid)
{
  return ((uint64_t)rid.page_id << 16) | (uint64_t)rid.slot_index;
}

// ============================================================================
// Result set operations using mmap-based allocation (old API compatibility)
// ============================================================================

static void* rs_alloc(size_t size)
{
  if (size == 0) return NULL;
  void* ptr = internal_mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) return NULL;
  return ptr;
}

static void rs_free(void* ptr, size_t size)
{
  if (!is_null(ptr) && size > 0) {
    internal_munmap(ptr, size);
  }
}

NostrDBResultSet* nostr_db_result_create(uint32_t capacity)
{
  if (capacity == 0) capacity = NOSTR_DB_RESULT_DEFAULT_CAPACITY;

  NostrDBResultSet* rs = (NostrDBResultSet*)rs_alloc(sizeof(NostrDBResultSet));
  if (is_null(rs)) return NULL;

  rs->offsets = (uint64_t*)rs_alloc(capacity * sizeof(uint64_t));
  if (is_null(rs->offsets)) {
    rs_free(rs, sizeof(NostrDBResultSet));
    return NULL;
  }

  rs->created_at = (int64_t*)rs_alloc(capacity * sizeof(int64_t));
  if (is_null(rs->created_at)) {
    rs_free(rs->offsets, capacity * sizeof(uint64_t));
    rs_free(rs, sizeof(NostrDBResultSet));
    return NULL;
  }

  rs->count    = 0;
  rs->capacity = capacity;
  return rs;
}

void nostr_db_result_free(NostrDBResultSet* rs)
{
  if (is_null(rs)) return;
  if (!is_null(rs->offsets))
    rs_free(rs->offsets, rs->capacity * sizeof(uint64_t));
  if (!is_null(rs->created_at))
    rs_free(rs->created_at, rs->capacity * sizeof(int64_t));
  rs_free(rs, sizeof(NostrDBResultSet));
}

int32_t nostr_db_result_add(NostrDBResultSet* rs, uint64_t offset,
                            int64_t created_at)
{
  require_not_null(rs, -1);

  // Check for duplicate
  for (uint32_t i = 0; i < rs->count; i++) {
    if (rs->offsets[i] == offset) return 1;
  }

  // Grow if needed
  if (rs->count >= rs->capacity) {
    uint32_t  new_cap     = rs->capacity * 2;
    uint64_t* new_offsets = (uint64_t*)rs_alloc(new_cap * sizeof(uint64_t));
    if (is_null(new_offsets)) return -1;

    int64_t* new_ca = (int64_t*)rs_alloc(new_cap * sizeof(int64_t));
    if (is_null(new_ca)) {
      rs_free(new_offsets, new_cap * sizeof(uint64_t));
      return -1;
    }

    internal_memcpy(new_offsets, rs->offsets, rs->count * sizeof(uint64_t));
    internal_memcpy(new_ca, rs->created_at, rs->count * sizeof(int64_t));

    rs_free(rs->offsets, rs->capacity * sizeof(uint64_t));
    rs_free(rs->created_at, rs->capacity * sizeof(int64_t));

    rs->offsets    = new_offsets;
    rs->created_at = new_ca;
    rs->capacity   = new_cap;
  }

  rs->offsets[rs->count]    = offset;
  rs->created_at[rs->count] = created_at;
  rs->count++;

  return 0;
}

int32_t nostr_db_result_sort(NostrDBResultSet* rs)
{
  require_not_null(rs, -1);

  // Insertion sort descending by created_at
  for (uint32_t i = 1; i < rs->count; i++) {
    int64_t  key_time   = rs->created_at[i];
    uint64_t key_offset = rs->offsets[i];
    int32_t  j          = (int32_t)i - 1;

    while (j >= 0 && rs->created_at[j] < key_time) {
      rs->created_at[j + 1] = rs->created_at[j];
      rs->offsets[j + 1]    = rs->offsets[j];
      j--;
    }

    rs->created_at[j + 1] = key_time;
    rs->offsets[j + 1]    = key_offset;
  }

  return 0;
}

void nostr_db_result_apply_limit(NostrDBResultSet* rs, uint32_t limit)
{
  if (is_null(rs) || limit == 0) return;
  if (rs->count > limit) rs->count = limit;
}

// ============================================================================
// nostr_db_query_execute: Execute query using new B+ tree query engine
// ============================================================================
NostrDBError nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter,
                                    NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  // Execute using new query engine
  QueryResultSet* rs = query_result_create(result->capacity);
  if (is_null(rs)) return NOSTR_DB_ERROR_MMAP_FAILED;

  NostrDBError err =
    query_execute(&db->indexes, &db->buffer_pool, filter, rs);
  if (err != NOSTR_DB_OK) {
    query_result_free(rs);
    return err;
  }

  // Convert RecordId results to packed offsets
  for (uint32_t i = 0; i < rs->count; i++) {
    uint64_t offset = pack_record_id(rs->rids[i]);
    nostr_db_result_add(result, offset, rs->created_at[i]);
  }

  query_result_free(rs);
  return NOSTR_DB_OK;
}
