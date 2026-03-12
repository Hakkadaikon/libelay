#include "query_engine.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "../record/record_manager.h"

// ============================================================================
// Callback context for collecting RecordIds from overflow chain scans
// ============================================================================
typedef struct {
  QueryResultSet* rs;
  uint32_t        limit;
  int64_t         since;
  int64_t         until;
  BufferPool*     pool;  // For reading created_at from records
} ScanCtx;

// ============================================================================
// Internal: Read created_at from an EventRecord via RecordId
// ============================================================================
static int64_t read_created_at(BufferPool* pool, RecordId rid)
{
  if (is_null(pool)) return 0;

  PageData* page = buffer_pool_pin(pool, rid.page_id);
  if (is_null(page)) return 0;

  // Read the slot entry to get offset
  SlotPageHeader* hdr  = (SlotPageHeader*)page->data;
  SlotEntry*      slot = (SlotEntry*)(page->data + SLOT_PAGE_HEADER_SIZE);

  if (rid.slot_index >= hdr->slot_count) {
    buffer_pool_unpin(pool, rid.page_id);
    return 0;
  }

  uint16_t offset = slot[rid.slot_index].offset;
  if (offset == 0) {
    buffer_pool_unpin(pool, rid.page_id);
    return 0;
  }

  // EventRecord starts at the offset within the page
  EventRecord* rec = (EventRecord*)(page->data + offset);
  int64_t      ts  = rec->created_at;
  buffer_pool_unpin(pool, rid.page_id);
  return ts;
}

// ============================================================================
// Callback for btree_scan_key: collect RecordIds with time filtering
// ============================================================================
static bool scan_collect_cb(const void* key, const void* value, void* ud)
{
  (void)key;
  ScanCtx*        ctx = (ScanCtx*)ud;
  const RecordId* rid = (const RecordId*)value;

  if (ctx->limit > 0 && ctx->rs->count >= ctx->limit) return false;

  // Read created_at for time filtering
  int64_t ts = 0;
  if (!is_null(ctx->pool)) {
    ts = read_created_at(ctx->pool, *rid);
  }

  // Apply time filters
  if (ctx->since > 0 && ts < ctx->since) return true;   // skip, continue
  if (ctx->until > 0 && ts > ctx->until) return true;   // skip, continue

  query_result_add(ctx->rs, *rid, ts);

  if (ctx->limit > 0 && ctx->rs->count >= ctx->limit) return false;
  return true;
}

// ============================================================================
// query_by_ids: Look up each ID in the unique ID index
// ============================================================================
NostrDBError query_by_ids(IndexManager* im, BufferPool* pool,
                          const NostrDBFilter* filter, QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  for (size_t i = 0; i < filter->ids_count && rs->count < limit; i++) {
    RecordId     rid;
    NostrDBError err =
      index_id_lookup(&im->id_index, filter->ids[i].value, &rid);
    if (err != NOSTR_DB_OK) continue;

    int64_t ts = read_created_at(pool, rid);

    // Apply time filters
    if (filter->since > 0 && ts < filter->since) continue;
    if (filter->until > 0 && ts > filter->until) continue;

    query_result_add(rs, rid, ts);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// query_by_pubkey: Scan pubkey index for each author
// ============================================================================
NostrDBError query_by_pubkey(IndexManager* im, const NostrDBFilter* filter,
                             QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  ScanCtx ctx = {rs, limit, filter->since, filter->until, im->pool};

  for (size_t i = 0; i < filter->authors_count && rs->count < limit; i++) {
    ctx.limit = limit - rs->count;
    btree_scan_key(&im->pubkey_index, filter->authors[i].value,
                   scan_collect_cb, &ctx);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// query_by_kind: Scan kind index for each kind
// ============================================================================
NostrDBError query_by_kind(IndexManager* im, const NostrDBFilter* filter,
                           QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  ScanCtx ctx = {rs, limit, filter->since, filter->until, im->pool};

  for (size_t i = 0; i < filter->kinds_count && rs->count < limit; i++) {
    ctx.limit = limit - rs->count;
    btree_scan_key(&im->kind_index, &filter->kinds[i], scan_collect_cb, &ctx);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// query_by_pubkey_kind: Scan composite pubkey+kind index
// ============================================================================
NostrDBError query_by_pubkey_kind(IndexManager* im,
                                  const NostrDBFilter* filter,
                                  QueryResultSet*      rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  ScanCtx ctx = {rs, limit, filter->since, filter->until, im->pool};

  for (size_t i = 0; i < filter->authors_count && rs->count < limit; i++) {
    for (size_t j = 0; j < filter->kinds_count && rs->count < limit; j++) {
      uint8_t key[36];
      internal_memcpy(key, filter->authors[i].value, 32);
      internal_memcpy(key + 32, &filter->kinds[j], sizeof(uint32_t));

      ctx.limit = limit - rs->count;
      btree_scan_key(&im->pubkey_kind_index, key, scan_collect_cb, &ctx);
    }
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// query_by_tag: Scan tag index for each tag name+value combination
// ============================================================================
NostrDBError query_by_tag(IndexManager* im, const NostrDBFilter* filter,
                          QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  ScanCtx ctx = {rs, limit, filter->since, filter->until, im->pool};

  for (size_t i = 0; i < filter->tags_count && rs->count < limit; i++) {
    const NostrDBFilterTag* tag = &filter->tags[i];

    for (size_t j = 0; j < tag->values_count && rs->count < limit; j++) {
      uint8_t key[33];
      key[0] = (uint8_t)tag->name;
      internal_memcpy(key + 1, tag->values[j], 32);

      ctx.limit = limit - rs->count;
      btree_scan_key(&im->tag_index, key, scan_collect_cb, &ctx);
    }
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// Callback for timeline range scan (btree_range_scan)
// The value in timeline B+ tree leaf is page_id_t (overflow chain head)
// We need to walk the overflow chain for each key
// ============================================================================
typedef struct {
  QueryResultSet* rs;
  uint32_t        limit;
  IndexManager*   im;
} TimelineScanCtx;

static bool timeline_range_cb(const void* key, const void* value, void* ud)
{
  TimelineScanCtx* ctx = (TimelineScanCtx*)ud;
  if (ctx->limit > 0 && ctx->rs->count >= ctx->limit) return false;

  int64_t encoded;
  internal_memcpy(&encoded, key, sizeof(int64_t));
  int64_t ts = timeline_key_decode(encoded);

  // value is page_id_t (overflow chain head)
  page_id_t chain_head;
  internal_memcpy(&chain_head, value, sizeof(page_id_t));

  if (chain_head == PAGE_ID_NULL) return true;

  // Walk overflow chain
  page_id_t pid = chain_head;
  while (pid != PAGE_ID_NULL) {
    PageData* page = buffer_pool_pin(ctx->im->pool, pid);
    if (is_null(page)) break;

    const BTreeOverflowHeader* hdr =
      (const BTreeOverflowHeader*)page->data;

    for (uint16_t i = 0; i < hdr->entry_count; i++) {
      const uint8_t* base =
        page->data + sizeof(BTreeOverflowHeader);
      RecordId rid;
      internal_memcpy(&rid, base + (uint32_t)i * sizeof(RecordId),
                      sizeof(RecordId));

      query_result_add(ctx->rs, rid, ts);

      if (ctx->limit > 0 && ctx->rs->count >= ctx->limit) {
        buffer_pool_unpin(ctx->im->pool, pid);
        return false;
      }
    }

    page_id_t next = hdr->next_page;
    buffer_pool_unpin(ctx->im->pool, pid);
    pid = next;
  }

  return true;
}

// ============================================================================
// query_timeline_scan: Range scan over timeline index (descending order)
// ============================================================================
NostrDBError query_timeline_scan(IndexManager* im, const NostrDBFilter* filter,
                                 QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  // Encode time range for descending B+ tree
  // since=oldest, until=newest
  // Encoded: INT64_MAX - ts, so newer = smaller key
  // min_key = INT64_MAX - until (newest first)
  // max_key = INT64_MAX - since (oldest last)
  int64_t* min_key_ptr = NULL;
  int64_t* max_key_ptr = NULL;
  int64_t  min_key, max_key;

  if (filter->until > 0) {
    min_key     = timeline_key_encode(filter->until);
    min_key_ptr = &min_key;
  }
  if (filter->since > 0) {
    max_key     = timeline_key_encode(filter->since);
    max_key_ptr = &max_key;
  }

  TimelineScanCtx ctx = {rs, limit, im};
  return btree_range_scan(&im->timeline_index, min_key_ptr, max_key_ptr,
                          timeline_range_cb, &ctx);
}

// ============================================================================
// query_post_filter: Read EventRecord and verify against full filter
// ============================================================================
NostrDBError query_post_filter(BufferPool* pool, QueryResultSet* rs,
                               const NostrDBFilter* filter)
{
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t write = 0;
  uint8_t  buf[4096];

  for (uint32_t i = 0; i < rs->count; i++) {
    uint16_t     len = sizeof(buf);
    NostrDBError err = record_read(pool, rs->rids[i], buf, &len);
    if (err != NOSTR_DB_OK || len < sizeof(EventRecord)) continue;

    EventRecord* rec = (EventRecord*)buf;

    // Check deleted flag
    if ((rec->flags & NOSTR_DB_EVENT_FLAG_DELETED) != 0) continue;

    // Check time range
    if (filter->since > 0 && rec->created_at < filter->since) continue;
    if (filter->until > 0 && rec->created_at > filter->until) continue;

    // Check kinds filter
    if (filter->kinds_count > 0) {
      bool match = false;
      for (size_t k = 0; k < filter->kinds_count; k++) {
        if (rec->kind == filter->kinds[k]) {
          match = true;
          break;
        }
      }
      if (!match) continue;
    }

    // Check authors filter
    if (filter->authors_count > 0) {
      bool match = false;
      for (size_t k = 0; k < filter->authors_count; k++) {
        if (internal_memcmp(rec->pubkey, filter->authors[k].value, 32) == 0) {
          match = true;
          break;
        }
      }
      if (!match) continue;
    }

    // Check IDs filter
    if (filter->ids_count > 0) {
      bool match = false;
      for (size_t k = 0; k < filter->ids_count; k++) {
        if (internal_memcmp(rec->id, filter->ids[k].value, 32) == 0) {
          match = true;
          break;
        }
      }
      if (!match) continue;
    }

    // Keep this result
    if (write != i) {
      rs->rids[write]       = rs->rids[i];
      rs->created_at[write] = rs->created_at[i];
    }
    write++;
  }

  rs->count = write;
  return NOSTR_DB_OK;
}

// ============================================================================
// Internal: Validate filter for query execution
// ============================================================================
static bool filter_validate(const NostrDBFilter* filter)
{
  if (is_null(filter)) return false;
  if (filter->ids_count > NOSTR_DB_FILTER_MAX_IDS) return false;
  if (filter->authors_count > NOSTR_DB_FILTER_MAX_AUTHORS) return false;
  if (filter->kinds_count > NOSTR_DB_FILTER_MAX_KINDS) return false;
  if (filter->tags_count > NOSTR_DB_FILTER_MAX_TAGS) return false;
  if (filter->since > 0 && filter->until > 0 && filter->since > filter->until)
    return false;
  return true;
}

// ============================================================================
// Internal: Select optimal query strategy
// ============================================================================
static NostrDBQueryStrategy select_strategy(const NostrDBFilter* filter)
{
  if (filter->ids_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_ID;
  if (filter->tags_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_TAG;
  if (filter->authors_count > 0 && filter->kinds_count > 0)
    return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND;
  if (filter->authors_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY;
  if (filter->kinds_count > 0) return NOSTR_DB_QUERY_STRATEGY_BY_KIND;
  return NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN;
}

// ============================================================================
// query_execute: Select strategy and execute
// ============================================================================
NostrDBError query_execute(IndexManager* im, BufferPool* pool,
                           const NostrDBFilter* filter, QueryResultSet* rs)
{
  require_not_null(im, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(rs, NOSTR_DB_ERROR_NULL_PARAM);

  if (!filter_validate(filter)) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  NostrDBQueryStrategy strategy = select_strategy(filter);
  NostrDBError         err      = NOSTR_DB_OK;

  switch (strategy) {
    case NOSTR_DB_QUERY_STRATEGY_BY_ID:
      err = query_by_ids(im, pool, filter, rs);
      break;
    case NOSTR_DB_QUERY_STRATEGY_BY_TAG:
      err = query_by_tag(im, filter, rs);
      break;
    case NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND:
      err = query_by_pubkey_kind(im, filter, rs);
      break;
    case NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY:
      err = query_by_pubkey(im, filter, rs);
      break;
    case NOSTR_DB_QUERY_STRATEGY_BY_KIND:
      err = query_by_kind(im, filter, rs);
      break;
    case NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN:
    default:
      err = query_timeline_scan(im, filter, rs);
      break;
  }

  if (err != NOSTR_DB_OK) return err;

  // Post-filter if pool is available
  if (!is_null(pool)) {
    err = query_post_filter(pool, rs, filter);
    if (err != NOSTR_DB_OK) return err;
  }

  // Sort by created_at (newest first)
  query_result_sort(rs);

  // Apply limit
  uint32_t limit = filter->limit > 0 ? filter->limit
                                      : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  query_result_apply_limit(rs, limit);

  return NOSTR_DB_OK;
}
