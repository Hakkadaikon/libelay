#ifndef NOSTR_DB_QUERY_TYPES_H_
#define NOSTR_DB_QUERY_TYPES_H_

#include "../../../util/types.h"

// ============================================================================
// Constants
// ============================================================================
#define NOSTR_DB_FILTER_MAX_IDS 256
#define NOSTR_DB_FILTER_MAX_AUTHORS 256
#define NOSTR_DB_FILTER_MAX_KINDS 64
#define NOSTR_DB_FILTER_MAX_TAGS 26
#define NOSTR_DB_FILTER_MAX_TAG_VALUES 256
#define NOSTR_DB_RESULT_DEFAULT_CAPACITY 100
#define NOSTR_DB_QUERY_DEFAULT_LIMIT 500

// ============================================================================
// Filter ID/Pubkey (32 bytes binary)
// ============================================================================
typedef struct {
  uint8_t value[32];
  size_t  prefix_len;  // 0 = exact match, >0 = prefix match length in bytes
} NostrDBFilterId;

typedef struct {
  uint8_t value[32];
  size_t  prefix_len;
} NostrDBFilterPubkey;

// ============================================================================
// Filter Tag
// ============================================================================
typedef struct {
  char    name;                                        // Tag name ('e', 'p', etc.)
  uint8_t values[NOSTR_DB_FILTER_MAX_TAG_VALUES][32];  // Tag values (binary)
  size_t  values_count;
} NostrDBFilterTag;

// ============================================================================
// NostrDBFilter - Query filter structure
// ============================================================================
typedef struct {
  // IDs filter
  NostrDBFilterId ids[NOSTR_DB_FILTER_MAX_IDS];
  size_t          ids_count;

  // Authors filter
  NostrDBFilterPubkey authors[NOSTR_DB_FILTER_MAX_AUTHORS];
  size_t              authors_count;

  // Kinds filter
  uint32_t kinds[NOSTR_DB_FILTER_MAX_KINDS];
  size_t   kinds_count;

  // Tag filters (#e, #p, #t, etc.)
  NostrDBFilterTag tags[NOSTR_DB_FILTER_MAX_TAGS];
  size_t           tags_count;

  // Time range
  int64_t since;  // 0 = no limit
  int64_t until;  // 0 = no limit

  // Result limit
  uint32_t limit;  // 0 = use default
} NostrDBFilter;

// ============================================================================
// Result Set
// ============================================================================
typedef struct {
  uint64_t* offsets;
  int64_t*  created_at;  // Parallel array for sorting
  uint32_t  count;
  uint32_t  capacity;
} NostrDBResultSet;

// ============================================================================
// Query Strategy
// ============================================================================
typedef enum {
  NOSTR_DB_QUERY_STRATEGY_BY_ID = 0,       // ids specified
  NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY_KIND,  // authors + kinds specified
  NOSTR_DB_QUERY_STRATEGY_BY_PUBKEY,       // authors only
  NOSTR_DB_QUERY_STRATEGY_BY_KIND,         // kinds only
  NOSTR_DB_QUERY_STRATEGY_BY_TAG,          // tag search
  NOSTR_DB_QUERY_STRATEGY_TIMELINE_SCAN,   // fallback
} NostrDBQueryStrategy;

#endif
