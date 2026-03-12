#ifndef NOSTR_DB_QUERY_H_
#define NOSTR_DB_QUERY_H_

#include "../../../util/types.h"
#include "../db_types.h"
#include "db_query_types.h"

// Forward declaration
struct NostrDB;
typedef struct NostrDB NostrDB;

// Filter functions
void                 nostr_db_filter_init(NostrDBFilter* filter);
bool                 nostr_db_filter_validate(const NostrDBFilter* filter);
bool                 nostr_db_filter_is_empty(const NostrDBFilter* filter);
NostrDBQueryStrategy nostr_db_query_select_strategy(
    const NostrDBFilter* filter);

// Result set functions
NostrDBResultSet* nostr_db_result_create(uint32_t capacity);
int32_t nostr_db_result_add(NostrDBResultSet* result, uint64_t offset,
                            int64_t created_at);
int32_t nostr_db_result_sort(NostrDBResultSet* result);
void    nostr_db_result_apply_limit(NostrDBResultSet* result, uint32_t limit);
void    nostr_db_result_free(NostrDBResultSet* result);

// Query execution
NostrDBError nostr_db_query_execute(NostrDB* db, const NostrDBFilter* filter,
                                    NostrDBResultSet* result);

#endif
