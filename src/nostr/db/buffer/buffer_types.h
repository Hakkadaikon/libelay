#ifndef NOSTR_DB_BUFFER_TYPES_H_
#define NOSTR_DB_BUFFER_TYPES_H_

#include "../../../util/types.h"
#include "../disk/disk_types.h"

// ============================================================================
// Constants
// ============================================================================
#define BUFFER_POOL_DEFAULT_SIZE 4096  // 4096 frames = 16MB of page cache

// Sentinel value for empty hash table slots and invalid frame indices
#define BUFFER_FRAME_INVALID ((uint32_t)0xFFFFFFFF)

// ============================================================================
// Buffer pool (Structure of Arrays for cache-friendly access)
// ============================================================================
typedef struct {
  // --- Hot data (accessed during every lookup/eviction) ---
  page_id_t* page_ids;     // [pool_size] Page ID assigned to each frame
  uint8_t*   pin_counts;   // [pool_size] Pin count per frame
  uint8_t*   dirty_flags;  // [pool_size] Dirty flag per frame
  uint8_t*   ref_bits;     // [pool_size] Clock reference bit per frame

  // --- Page data ---
  PageData* pages;  // [pool_size] Actual page data (aligned)

  // --- Cold data ---
  uint64_t* lsn;  // [pool_size] Last modification LSN per frame

  // --- Pool metadata ---
  uint32_t pool_size;   // Total number of frames
  uint32_t clock_hand;  // Current position for Clock eviction

  // --- Page ID -> Frame index hash map (open addressing) ---
  uint32_t* hash_table;  // [hash_size] Maps page_id to frame index
  uint32_t  hash_size;   // Hash table capacity (>= pool_size * 2)

  // --- Disk manager reference ---
  DiskManager* disk;
} BufferPool;

#endif
