#ifndef NOSTR_DB_H_
#define NOSTR_DB_H_

#include "../../util/types.h"
#include "../nostr_types.h"
#include "db_types.h"

// Forward declarations
typedef struct NostrDB NostrDB;
struct NostrFilter;

// ============================================================================
// Result set for query results
// ============================================================================
typedef struct {
  nostr_db_offset_t* offsets;    // Array of event offsets
  uint32_t           count;      // Number of results
  uint32_t           capacity;   // Array capacity
} NostrDBResultSet;

// ============================================================================
// Initialization and shutdown
// ============================================================================

/**
 * @brief Initialize the database
 * @param db Pointer to receive NostrDB handle
 * @param data_dir Directory path for data files
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError nostr_db_init(NostrDB** db, const char* data_dir);

/**
 * @brief Shutdown the database
 * @param db NostrDB handle
 */
void nostr_db_shutdown(NostrDB* db);

// ============================================================================
// Event operations
// ============================================================================

/**
 * @brief Write an event to the database
 * @param db NostrDB handle
 * @param event Event to write
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event);

/**
 * @brief Get an event by its ID
 * @param db NostrDB handle
 * @param id Event ID (32 bytes raw)
 * @param out Output event structure
 * @return NOSTR_DB_OK on success, NOSTR_DB_ERROR_NOT_FOUND if not found
 */
NostrDBError nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id, NostrEventEntity* out);

/**
 * @brief Delete an event by its ID (logical deletion)
 * @param db NostrDB handle
 * @param id Event ID (32 bytes raw)
 * @return NOSTR_DB_OK on success, NOSTR_DB_ERROR_NOT_FOUND if not found
 */
NostrDBError nostr_db_delete_event(NostrDB* db, const uint8_t* id);

// ============================================================================
// Query operations
// ============================================================================

/**
 * @brief Query events matching a filter
 * @param db NostrDB handle
 * @param filter Query filter
 * @param result Output result set
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError nostr_db_query(NostrDB* db, const struct NostrFilter* filter, NostrDBResultSet* result);

/**
 * @brief Free a result set
 * @param result Result set to free
 */
void nostr_db_result_free(NostrDBResultSet* result);

// ============================================================================
// Statistics
// ============================================================================

/**
 * @brief Get database statistics
 * @param db NostrDB handle
 * @param stats Output statistics structure
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError nostr_db_get_stats(NostrDB* db, NostrDBStats* stats);

#endif
