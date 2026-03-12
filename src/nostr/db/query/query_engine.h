#ifndef NOSTR_DB_QUERY_ENGINE_H_
#define NOSTR_DB_QUERY_ENGINE_H_

#include "../../../util/types.h"
#include "../buffer/buffer_pool.h"
#include "../db_types.h"
#include "../index/index_manager.h"
#include "../record/record_types.h"
#include "db_query_types.h"

// ============================================================================
// RecordId-based result set with bloom filter for dedup
// ============================================================================
typedef struct {
  RecordId* rids;        // RecordId array
  int64_t*  created_at;  // Parallel array for sorting
  uint32_t  count;
  uint32_t  capacity;
  uint64_t  bloom[64];   // 512-byte bloom filter for O(1) dedup
} QueryResultSet;

// ============================================================================
// Result set operations
// ============================================================================

QueryResultSet* query_result_create(uint32_t capacity);
void            query_result_free(QueryResultSet* rs);
int32_t         query_result_add(QueryResultSet* rs, RecordId rid,
                                 int64_t created_at);
int32_t         query_result_sort(QueryResultSet* rs);
void            query_result_apply_limit(QueryResultSet* rs, uint32_t limit);

// ============================================================================
// Query engine: execute queries against B+ tree indexes
// ============================================================================

NostrDBError query_execute(IndexManager* im, BufferPool* pool,
                           const NostrDBFilter* filter, QueryResultSet* rs);

NostrDBError query_by_ids(IndexManager* im, BufferPool* pool,
                          const NostrDBFilter* filter, QueryResultSet* rs);

NostrDBError query_by_pubkey(IndexManager* im, const NostrDBFilter* filter,
                             QueryResultSet* rs);

NostrDBError query_by_kind(IndexManager* im, const NostrDBFilter* filter,
                           QueryResultSet* rs);

NostrDBError query_by_pubkey_kind(IndexManager* im,
                                  const NostrDBFilter* filter,
                                  QueryResultSet*      rs);

NostrDBError query_by_tag(IndexManager* im, const NostrDBFilter* filter,
                          QueryResultSet* rs);

NostrDBError query_timeline_scan(IndexManager* im, const NostrDBFilter* filter,
                                 QueryResultSet* rs);

// ============================================================================
// Post-filter: verify results against full filter criteria
// Reads EventRecord from BufferPool to check fields not covered by index
// ============================================================================
NostrDBError query_post_filter(BufferPool* pool, QueryResultSet* rs,
                               const NostrDBFilter* filter);

#endif
