#ifndef NOSTR_DB_QUERY_H_
#define NOSTR_DB_QUERY_H_

#include "../../../util/types.h"
#include "../db_types.h"
#include "db_query_result.h"
#include "db_query_types.h"

// Forward declaration
struct NostrDB;
typedef struct NostrDB NostrDB;

// ============================================================================
// Filter Functions
// ============================================================================

/**
 * @brief Initialize filter to default values
 * @param filter Filter to initialize
 */
void nostr_db_filter_init(NostrDBFilter* filter);

/**
 * @brief Validate filter for query execution
 * @param filter Filter to validate
 * @return true if valid, false otherwise
 */
bool nostr_db_filter_validate(const NostrDBFilter* filter);

/**
 * @brief Check if filter is empty (no constraints)
 * @param filter Filter to check
 * @return true if empty, false if has constraints
 */
bool nostr_db_filter_is_empty(const NostrDBFilter* filter);

// ============================================================================
// Query Strategy Functions
// ============================================================================

/**
 * @brief Select optimal query strategy based on filter
 * @param filter Query filter
 * @return Optimal query strategy
 */
NostrDBQueryStrategy nostr_db_query_select_strategy(const NostrDBFilter* filter);

// ============================================================================
// Query Execution Functions
// ============================================================================

/**
 * @brief Execute query with given filter
 * @param db Database instance
 * @param filter Query filter
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute ID-based query
 * @param db Database instance
 * @param filter Query filter (uses ids field)
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_by_ids(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute pubkey-based query
 * @param db Database instance
 * @param filter Query filter (uses authors field)
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_by_pubkey(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute kind-based query
 * @param db Database instance
 * @param filter Query filter (uses kinds field)
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_by_kind(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute pubkey+kind combined query
 * @param db Database instance
 * @param filter Query filter (uses authors and kinds fields)
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_by_pubkey_kind(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute tag-based query
 * @param db Database instance
 * @param filter Query filter (uses tags field)
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_by_tag(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Execute timeline scan (fallback strategy)
 * @param db Database instance
 * @param filter Query filter
 * @param result Result set (output)
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_timeline_scan(NostrDB* db, const NostrDBFilter* filter, NostrDBResultSet* result);

/**
 * @brief Post-filter results (apply remaining filter conditions)
 * @param db Database instance
 * @param result Result set to filter
 * @param filter Query filter
 * @return NOSTR_DB_OK on success, error code otherwise
 */
NostrDBError nostr_db_query_filter_result(NostrDB* db, NostrDBResultSet* result, const NostrDBFilter* filter);

#endif
