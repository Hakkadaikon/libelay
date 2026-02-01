#ifndef NOSTR_DB_FILE_H_
#define NOSTR_DB_FILE_H_

#include "../../util/types.h"
#include "db_types.h"

/**
 * @brief Check if a file exists
 * @param path File path
 * @return true if file exists, false otherwise
 */
bool nostr_db_file_exists(const char* path);

/**
 * @brief Create a new file with initial size
 * @param path File path
 * @param initial_size Initial file size in bytes
 * @return File descriptor on success, -1 on error
 */
int32_t nostr_db_file_create(const char* path, size_t initial_size);

/**
 * @brief Open an existing file
 * @param path File path
 * @param writable true for read-write, false for read-only
 * @return File descriptor on success, -1 on error
 */
int32_t nostr_db_file_open(const char* path, bool writable);

/**
 * @brief Close a file
 * @param fd File descriptor
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_file_close(int32_t fd);

/**
 * @brief Get file size
 * @param fd File descriptor
 * @return File size in bytes, or -1 on error
 */
int64_t nostr_db_file_get_size(int32_t fd);

/**
 * @brief Extend file to new size
 * @param fd File descriptor
 * @param new_size New file size in bytes
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_file_extend(int32_t fd, size_t new_size);

/**
 * @brief Sync file to disk
 * @param fd File descriptor
 * @return 0 on success, -1 on error
 */
int32_t nostr_db_file_sync(int32_t fd);

#endif
