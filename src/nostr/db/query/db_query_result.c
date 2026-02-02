#include "db_query_result.h"

#include "../../../arch/memory.h"
#include "../../../arch/mmap.h"
#include "../../../util/string.h"

// ============================================================================
// Helper: Allocate memory using anonymous mmap
// ============================================================================
static void* query_alloc(size_t size)
{
  if (size == 0) {
    return NULL;
  }
  void* ptr = internal_mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    return NULL;
  }
  return ptr;
}

// ============================================================================
// Helper: Free memory allocated by query_alloc
// ============================================================================
static void query_free(void* ptr, size_t size)
{
  if (ptr != NULL && size > 0) {
    internal_munmap(ptr, size);
  }
}

// ============================================================================
// nostr_db_result_create
// ============================================================================
NostrDBResultSet* nostr_db_result_create(uint32_t capacity)
{
  if (capacity == 0) {
    capacity = NOSTR_DB_RESULT_DEFAULT_CAPACITY;
  }

  // Allocate result set structure
  size_t            struct_size = sizeof(NostrDBResultSet);
  NostrDBResultSet* result      = (NostrDBResultSet*)query_alloc(struct_size);
  if (is_null(result)) {
    return NULL;
  }

  // Allocate offsets array
  size_t offsets_size = capacity * sizeof(uint64_t);
  result->offsets     = (uint64_t*)query_alloc(offsets_size);
  if (is_null(result->offsets)) {
    query_free(result, struct_size);
    return NULL;
  }

  // Allocate created_at array
  size_t created_at_size = capacity * sizeof(int64_t);
  result->created_at     = (int64_t*)query_alloc(created_at_size);
  if (is_null(result->created_at)) {
    query_free(result->offsets, offsets_size);
    query_free(result, struct_size);
    return NULL;
  }

  result->count    = 0;
  result->capacity = capacity;

  return result;
}

// ============================================================================
// nostr_db_result_add
// ============================================================================
int32_t nostr_db_result_add(NostrDBResultSet* result, uint64_t offset, int64_t created_at)
{
  require_not_null(result, -1);
  require_not_null(result->offsets, -1);
  require_not_null(result->created_at, -1);

  // Check for duplicate
  for (uint32_t i = 0; i < result->count; i++) {
    if (result->offsets[i] == offset) {
      return 1;  // Duplicate
    }
  }

  // Check capacity
  if (result->count >= result->capacity) {
    // Need to grow - allocate new arrays with double capacity
    uint32_t new_capacity        = result->capacity * 2;
    size_t   new_offsets_size    = new_capacity * sizeof(uint64_t);
    size_t   new_created_at_size = new_capacity * sizeof(int64_t);

    uint64_t* new_offsets = (uint64_t*)query_alloc(new_offsets_size);
    if (is_null(new_offsets)) {
      return -1;
    }

    int64_t* new_created_at = (int64_t*)query_alloc(new_created_at_size);
    if (is_null(new_created_at)) {
      query_free(new_offsets, new_offsets_size);
      return -1;
    }

    // Copy old data
    internal_memcpy(new_offsets, result->offsets, result->count * sizeof(uint64_t));
    internal_memcpy(new_created_at, result->created_at, result->count * sizeof(int64_t));

    // Free old arrays
    query_free(result->offsets, result->capacity * sizeof(uint64_t));
    query_free(result->created_at, result->capacity * sizeof(int64_t));

    // Update pointers
    result->offsets    = new_offsets;
    result->created_at = new_created_at;
    result->capacity   = new_capacity;
  }

  // Add new entry
  result->offsets[result->count]    = offset;
  result->created_at[result->count] = created_at;
  result->count++;

  return 0;
}

// ============================================================================
// nostr_db_result_sort (descending by created_at - newest first)
// ============================================================================
int32_t nostr_db_result_sort(NostrDBResultSet* result)
{
  require_not_null(result, -1);
  require_not_null(result->offsets, -1);
  require_not_null(result->created_at, -1);

  // Simple insertion sort (good for small to medium sized arrays)
  for (uint32_t i = 1; i < result->count; i++) {
    int64_t  key_time   = result->created_at[i];
    uint64_t key_offset = result->offsets[i];
    int32_t  j          = (int32_t)i - 1;

    // Sort descending (newest first)
    while (j >= 0 && result->created_at[j] < key_time) {
      result->created_at[j + 1] = result->created_at[j];
      result->offsets[j + 1]    = result->offsets[j];
      j--;
    }

    result->created_at[j + 1] = key_time;
    result->offsets[j + 1]    = key_offset;
  }

  return 0;
}

// ============================================================================
// nostr_db_result_apply_limit
// ============================================================================
void nostr_db_result_apply_limit(NostrDBResultSet* result, uint32_t limit)
{
  if (is_null(result) || limit == 0) {
    return;
  }

  if (result->count > limit) {
    result->count = limit;
  }
}

// ============================================================================
// nostr_db_result_free
// ============================================================================
void nostr_db_result_free(NostrDBResultSet* result)
{
  if (is_null(result)) {
    return;
  }

  if (result->offsets != NULL) {
    query_free(result->offsets, result->capacity * sizeof(uint64_t));
  }

  if (result->created_at != NULL) {
    query_free(result->created_at, result->capacity * sizeof(int64_t));
  }

  query_free(result, sizeof(NostrDBResultSet));
}
