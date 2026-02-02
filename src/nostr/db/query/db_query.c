#include "db_query.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "../db_internal.h"
#include "../index/db_index_id.h"
#include "../index/db_index_kind.h"
#include "../index/db_index_pubkey.h"
#include "../index/db_index_pubkey_kind.h"
#include "../index/db_index_tag.h"
#include "../index/db_index_timeline.h"

// ============================================================================
// nostr_db_filter_init
// ============================================================================
void nostr_db_filter_init(NostrDBFilter* filter)
{
  if (is_null(filter)) {
    return;
  }
  internal_memset(filter, 0, sizeof(NostrDBFilter));
}

// ============================================================================
// nostr_db_filter_is_empty
// ============================================================================
bool nostr_db_filter_is_empty(const NostrDBFilter* filter)
{
  require_not_null(filter, true);

  return (filter->ids_count == 0 &&
          filter->authors_count == 0 &&
          filter->kinds_count == 0 &&
          filter->tags_count == 0 &&
          filter->since == 0 &&
          filter->until == 0);
}

// ============================================================================
// nostr_db_filter_validate
// ============================================================================
bool nostr_db_filter_validate(const NostrDBFilter* filter)
{
  require_not_null(filter, false);

  // Check array bounds
  if (filter->ids_count > NOSTR_DB_FILTER_MAX_IDS) {
    return false;
  }
  if (filter->authors_count > NOSTR_DB_FILTER_MAX_AUTHORS) {
    return false;
  }
  if (filter->kinds_count > NOSTR_DB_FILTER_MAX_KINDS) {
    return false;
  }
  if (filter->tags_count > NOSTR_DB_FILTER_MAX_TAGS) {
    return false;
  }

  // Check time range validity
  if (filter->since > 0 && filter->until > 0 && filter->since > filter->until) {
    return false;
  }

  return true;
}

// ============================================================================
// nostr_db_query_select_strategy
// ============================================================================
NostrDBQueryStrategy nostr_db_query_select_strategy(const NostrDBFilter* filter)
{
  require_not_null(filter, NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN);

  // Priority 1: ID search (most selective)
  if (filter->ids_count > 0) {
    return NOSTR_DB_QUERY_STRATEGY_BY_ID;
  }

  // Priority 2: Tag search (usually selective)
  if (filter->tags_count > 0) {
    return NOSTR_DB_QUERY_STRATEGY_BY_TAG;
  }

  // Priority 3: Pubkey + Kind combined (selective)
  if (filter->authors_count > 0 && filter->kinds_count > 0) {
    return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND;
  }

  // Priority 4: Pubkey only
  if (filter->authors_count > 0) {
    return NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY;
  }

  // Priority 5: Kind only
  if (filter->kinds_count > 0) {
    return NOSTR_DB_QUERY_STRATEGY_BY_KIND;
  }

  // Fallback: Timeline scan
  return NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN;
}

// ============================================================================
// Helper: Validate event offset
// ============================================================================
static bool is_valid_event_offset(NostrDB* db, uint64_t offset)
{
  if (is_null(db) || is_null(db->events_header)) {
    return false;
  }

  // Check if offset is within valid range
  uint64_t header_size = sizeof(NostrDBEventsHeader);
  uint64_t max_offset  = db->events_header->next_write_offset;

  if (offset < header_size || offset >= max_offset) {
    return false;
  }

  return true;
}

// ============================================================================
// Callback for collecting results from index iteration
// ============================================================================
typedef struct {
  NostrDBResultSet* result;
  uint32_t          limit;
} QueryCallbackData;

static bool query_collect_callback(uint64_t event_offset, int64_t created_at, void* user_data)
{
  QueryCallbackData* data = (QueryCallbackData*)user_data;
  if (is_null(data) || is_null(data->result)) {
    return false;
  }

  // Add to result set
  int32_t ret = nostr_db_result_add(data->result, event_offset, created_at);
  if (ret < 0) {
    return false;  // Error
  }

  // Check limit
  if (data->limit > 0 && data->result->count >= data->limit) {
    return false;  // Stop iteration
  }

  return true;
}

// ============================================================================
// nostr_db_query_by_ids
// ============================================================================
NostrDBError nostr_db_query_by_ids(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  for (size_t i = 0; i < filter->ids_count && result->count < limit; i++) {
    const uint8_t* id = filter->ids[i].value;

    // Lookup in ID index
    nostr_db_offset_t offset = nostr_db_id_index_lookup(db, id);
    if (offset == NOSTR_DB_OFFSET_NOT_FOUND) {
      continue;
    }

    // Validate offset
    if (!is_valid_event_offset(db, offset)) {
      continue;
    }

    // Get created_at from event header
    NostrDBEventHeader* header = (NostrDBEventHeader*)((uint8_t*)db->events_map + offset);
    if ((header->flags & NOSTR_DB_EVENT_FLAG_DELETED) != 0) {
      continue;  // Skip deleted
    }

    // Apply time filters
    if (filter->since > 0 && header->created_at < filter->since) {
      continue;
    }
    if (filter->until > 0 && header->created_at > filter->until) {
      continue;
    }

    nostr_db_result_add(result, offset, header->created_at);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_by_pubkey
// ============================================================================
NostrDBError nostr_db_query_by_pubkey(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  QueryCallbackData callback_data = {
    .result = result,
    .limit  = limit};

  for (size_t i = 0; i < filter->authors_count && result->count < limit; i++) {
    const uint8_t* pubkey = filter->authors[i].value;

    nostr_db_pubkey_index_iterate(
      db, pubkey,
      filter->since, filter->until,
      limit - result->count,
      query_collect_callback,
      &callback_data);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_by_kind
// ============================================================================
NostrDBError nostr_db_query_by_kind(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  QueryCallbackData callback_data = {
    .result = result,
    .limit  = limit};

  for (size_t i = 0; i < filter->kinds_count && result->count < limit; i++) {
    uint32_t kind = filter->kinds[i];

    nostr_db_kind_index_iterate(
      db, kind,
      filter->since, filter->until,
      limit - result->count,
      query_collect_callback,
      &callback_data);
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_by_pubkey_kind
// ============================================================================
NostrDBError nostr_db_query_by_pubkey_kind(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  QueryCallbackData callback_data = {
    .result = result,
    .limit  = limit};

  // Iterate all pubkey+kind combinations
  for (size_t i = 0; i < filter->authors_count && result->count < limit; i++) {
    const uint8_t* pubkey = filter->authors[i].value;

    for (size_t j = 0; j < filter->kinds_count && result->count < limit; j++) {
      uint32_t kind = filter->kinds[j];

      nostr_db_pubkey_kind_index_iterate(
        db, pubkey, kind,
        filter->since, filter->until,
        limit - result->count,
        query_collect_callback,
        &callback_data);
    }
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_by_tag
// ============================================================================
NostrDBError nostr_db_query_by_tag(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  QueryCallbackData callback_data = {
    .result = result,
    .limit  = limit};

  // Use first tag filter for primary search
  for (size_t i = 0; i < filter->tags_count && result->count < limit; i++) {
    const NostrDBFilterTag* tag      = &filter->tags[i];
    uint8_t                 tag_name = (uint8_t)tag->name;

    for (size_t j = 0; j < tag->values_count && result->count < limit; j++) {
      const uint8_t* tag_value = tag->values[j];

      nostr_db_tag_index_iterate(
        db, tag_name, tag_value,
        filter->since, filter->until,
        limit - result->count,
        query_collect_callback,
        &callback_data);
    }
  }

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_timeline_scan
// ============================================================================
NostrDBError nostr_db_query_timeline_scan(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;

  QueryCallbackData callback_data = {
    .result = result,
    .limit  = limit};

  nostr_db_timeline_index_iterate(
    db,
    filter->since, filter->until,
    limit,
    query_collect_callback,
    &callback_data);

  return NOSTR_DB_OK;
}

// ============================================================================
// Helper: Check if event matches filter criteria
// ============================================================================
static bool event_matches_filter(NostrDB* db, uint64_t offset, const NostrDBFilter* filter)
{
  // Validate offset first
  if (!is_valid_event_offset(db, offset)) {
    return false;
  }

  NostrDBEventHeader* header = (NostrDBEventHeader*)((uint8_t*)db->events_map + offset);

  // Check deleted
  if ((header->flags & NOSTR_DB_EVENT_FLAG_DELETED) != 0) {
    return false;
  }

  // Check time range
  if (filter->since > 0 && header->created_at < filter->since) {
    return false;
  }
  if (filter->until > 0 && header->created_at > filter->until) {
    return false;
  }

  // Get event body
  NostrDBEventBody* body = (NostrDBEventBody*)((uint8_t*)header + sizeof(NostrDBEventHeader));

  // Check kinds filter
  if (filter->kinds_count > 0) {
    bool kind_match = false;
    for (size_t i = 0; i < filter->kinds_count; i++) {
      if (body->kind == filter->kinds[i]) {
        kind_match = true;
        break;
      }
    }
    if (!kind_match) {
      return false;
    }
  }

  // Check authors filter
  if (filter->authors_count > 0) {
    bool author_match = false;
    for (size_t i = 0; i < filter->authors_count; i++) {
      if (internal_memcmp(body->pubkey, filter->authors[i].value, 32) == 0) {
        author_match = true;
        break;
      }
    }
    if (!author_match) {
      return false;
    }
  }

  // Check IDs filter
  if (filter->ids_count > 0) {
    bool id_match = false;
    for (size_t i = 0; i < filter->ids_count; i++) {
      if (internal_memcmp(header->id, filter->ids[i].value, 32) == 0) {
        id_match = true;
        break;
      }
    }
    if (!id_match) {
      return false;
    }
  }

  // Note: Tag filtering is more complex and requires deserializing tags
  // For now, we skip tag post-filtering (it was done in primary query)

  return true;
}

// ============================================================================
// nostr_db_query_filter_result
// ============================================================================
NostrDBError nostr_db_query_filter_result(NostrDB* db, NostrDBResultSet* result, const NostrDBFilter* filter)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);

  // Remove entries that don't match filter
  uint32_t write_idx = 0;
  for (uint32_t read_idx = 0; read_idx < result->count; read_idx++) {
    if (event_matches_filter(db, result->offsets[read_idx], filter)) {
      if (write_idx != read_idx) {
        result->offsets[write_idx]    = result->offsets[read_idx];
        result->created_at[write_idx] = result->created_at[read_idx];
      }
      write_idx++;
    }
  }
  result->count = write_idx;

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_query_execute
// ============================================================================
NostrDBError nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(filter, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(result, NOSTR_DB_ERROR_NULL_PARAM);

  // Validate filter
  if (!nostr_db_filter_validate(filter)) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  // Select strategy
  NostrDBQueryStrategy strategy = nostr_db_query_select_strategy(filter);
  NostrDBError         err      = NOSTR_DB_OK;

  // Execute primary query
  switch (strategy) {
    case NOSTR_DB_QUERY_STRATEGY_BY_ID:
      err = nostr_db_query_by_ids(db, filter, result);
      break;

    case NOSTR_DB_QUERY_STRATEGY_BY_TAG:
      err = nostr_db_query_by_tag(db, filter, result);
      break;

    case NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND:
      err = nostr_db_query_by_pubkey_kind(db, filter, result);
      break;

    case NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY:
      err = nostr_db_query_by_pubkey(db, filter, result);
      break;

    case NOSTR_DB_QUERY_STRATEGY_BY_KIND:
      err = nostr_db_query_by_kind(db, filter, result);
      break;

    case NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN:
    default:
      err = nostr_db_query_timeline_scan(db, filter, result);
      break;
  }

  if (err != NOSTR_DB_OK) {
    return err;
  }

  // Post-filter results (apply remaining filter criteria)
  err = nostr_db_query_filter_result(db, result, filter);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // Sort by created_at (newest first)
  nostr_db_result_sort(result);

  // Apply limit
  uint32_t limit = filter->limit > 0 ? filter->limit : NOSTR_DB_QUERY_DEFAULT_LIMIT;
  nostr_db_result_apply_limit(result, limit);

  return NOSTR_DB_OK;
}
