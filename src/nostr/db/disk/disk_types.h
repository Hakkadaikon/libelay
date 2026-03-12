#ifndef NOSTR_DB_DISK_TYPES_H_
#define NOSTR_DB_DISK_TYPES_H_

#include "../../../util/types.h"

// ============================================================================
// Page ID type (0 = invalid/null)
// ============================================================================
typedef uint32_t page_id_t;
#define PAGE_ID_NULL ((page_id_t)0)

// ============================================================================
// Page size constants
// ============================================================================
#define DB_PAGE_SIZE 4096
#define DB_INITIAL_PAGES 4096             // 16MB initial file size
#define DB_EXTEND_PAGES 128               // 512KB per extension batch
#define DB_MAX_PAGES ((uint32_t)1 << 24)  // ~64GB max

// ============================================================================
// Page data (4096 bytes, aligned)
// ============================================================================
typedef struct {
  uint8_t data[DB_PAGE_SIZE];
} __attribute__((aligned(DB_PAGE_SIZE))) PageData;

_Static_assert(sizeof(PageData) == DB_PAGE_SIZE, "PageData must be one page");

// ============================================================================
// File header (occupies page 0)
// ============================================================================
typedef struct {
  char      magic[8];                     // "NOSTRDB2"
  uint32_t  version;                      // Schema version (2)
  uint32_t  page_size;                    // DB_PAGE_SIZE (4096)
  uint32_t  total_pages;                  // Total pages in file
  page_id_t free_list_head;               // Head of free page list (0 = empty)
  uint32_t  free_page_count;              // Number of free pages
  uint32_t  reserved0;                    // Padding for alignment
  uint8_t   reserved[DB_PAGE_SIZE - 32];  // Fill to one page
} FileHeader;

_Static_assert(sizeof(FileHeader) == DB_PAGE_SIZE, "FileHeader must be one page");

// ============================================================================
// Free page entry (written at the beginning of a free page)
// ============================================================================
typedef struct {
  page_id_t next_free;  // Next free page ID (0 = end of list)
} FreePageEntry;

// ============================================================================
// File header magic and version
// ============================================================================
#define DB_FILE_MAGIC "NOSTRDB2"
#define DB_FILE_MAGIC_SIZE 8
#define DB_FILE_VERSION 2

// ============================================================================
// Disk manager
// ============================================================================
typedef struct {
  int32_t   fd;               // File descriptor (-1 = closed)
  uint32_t  total_pages;      // Cached total page count
  page_id_t free_list_head;   // Cached free list head
  uint32_t  free_page_count;  // Cached free page count
  char      path[256];        // File path
} DiskManager;

#endif
