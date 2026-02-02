#ifndef NOSTR_DB_QUERY_RESULT_H_
#define NOSTR_DB_QUERY_RESULT_H_

#include "../../../util/types.h"
#include "db_query_types.h"

// Forward declaration
struct NostrDB;
typedef struct NostrDB NostrDB;

/**
 * @brief Create a new result set
 * @param capacity Initial capacity
 * @return Pointer to result set, or NULL on error
 */
NostrDBResultSet* nostr_db_result_create(uint32_t capacity);

/**
 * @brief Add an offset to result set (with duplicate check)
 * @param result Result set
 * @param offset Event offset
 * @param created_at Event timestamp
 * @return 0 on success, -1 on error, 1 if duplicate
 */
int32_t nostr_db_result_add(NostrDBResultSet* result, uint64_t offset, int64_t created_at);

/**
 * @brief Sort result set by created_at (descending - newest first)
 * @param result Result set
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_result_sort(NostrDBResultSet* result);

/**
 * @brief Free result set
 * @param result Result set to free
 */
void nostr_db_result_free(NostrDBResultSet* result);

/**
 * @brief Apply limit to result set (truncate if necessary)
 * @param result Result set
 * @param limit Maximum number of results (0 = no limit)
 */
void nostr_db_result_apply_limit(NostrDBResultSet* result, uint32_t limit);

#endif
