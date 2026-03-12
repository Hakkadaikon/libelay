#include "buffer_pool.h"

#include "../../../arch/memory.h"
#include "../../../arch/mmap.h"
#include "../../../util/string.h"

// ============================================================================
// Internal: Find the smallest prime >= n (for hash table sizing)
// ============================================================================
static uint32_t next_prime(uint32_t n)
{
  if (n <= 2) {
    return 2;
  }
  // Ensure odd
  if ((n & 1) == 0) {
    n++;
  }
  for (;;) {
    bool is_prime = true;
    for (uint32_t d = 3; d * d <= n; d += 2) {
      if (n % d == 0) {
        is_prime = false;
        break;
      }
    }
    if (is_prime) {
      return n;
    }
    n += 2;
  }
}

// ============================================================================
// Internal: Allocate memory via anonymous mmap
// ============================================================================
static void* pool_alloc(size_t size)
{
  if (size == 0) {
    return NULL;
  }
  void* ptr = internal_mmap(NULL, size, PROT_READ | PROT_WRITE,
                            MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (ptr == MAP_FAILED) {
    return NULL;
  }
  return ptr;
}

// ============================================================================
// Internal: Free memory allocated by pool_alloc
// ============================================================================
static void pool_free(void* ptr, size_t size)
{
  if (ptr != NULL && size > 0) {
    internal_munmap(ptr, size);
  }
}

// ============================================================================
// Internal: Hash function for page_id -> hash table index
// ============================================================================
static inline uint32_t hash_page_id(page_id_t page_id, uint32_t hash_size)
{
  // Knuth multiplicative hash
  uint32_t h = page_id * 2654435761u;
  return h % hash_size;
}

// ============================================================================
// Internal: Look up a page_id in the hash table
// Returns frame index, or BUFFER_FRAME_INVALID if not found
// ============================================================================
static uint32_t hash_lookup(const BufferPool* pool, page_id_t page_id)
{
  uint32_t idx   = hash_page_id(page_id, pool->hash_size);
  uint32_t start = idx;

  do {
    uint32_t frame = pool->hash_table[idx];
    if (frame == BUFFER_FRAME_INVALID) {
      return BUFFER_FRAME_INVALID;  // Empty slot -> not found
    }
    if (pool->page_ids[frame] == page_id) {
      return frame;
    }
    idx = (idx + 1) % pool->hash_size;
  } while (idx != start);

  return BUFFER_FRAME_INVALID;
}

// ============================================================================
// Internal: Insert a (page_id -> frame_index) mapping into the hash table
// ============================================================================
static void hash_insert(BufferPool* pool, page_id_t page_id, uint32_t frame_index)
{
  uint32_t idx = hash_page_id(page_id, pool->hash_size);

  for (;;) {
    uint32_t frame = pool->hash_table[idx];
    if (frame == BUFFER_FRAME_INVALID) {
      pool->hash_table[idx] = frame_index;
      return;
    }
    // Overwrite if same page_id (shouldn't happen in normal flow)
    if (pool->page_ids[frame] == page_id) {
      pool->hash_table[idx] = frame_index;
      return;
    }
    idx = (idx + 1) % pool->hash_size;
  }
}

// ============================================================================
// Internal: Remove a page_id from the hash table
//
// Uses backward-shift deletion to maintain probe chain integrity.
// ============================================================================
static void hash_remove(BufferPool* pool, page_id_t page_id)
{
  uint32_t idx   = hash_page_id(page_id, pool->hash_size);
  uint32_t start = idx;

  // Find the entry
  for (;;) {
    uint32_t frame = pool->hash_table[idx];
    if (frame == BUFFER_FRAME_INVALID) {
      return;  // Not found
    }
    if (pool->page_ids[frame] == page_id) {
      break;  // Found at idx
    }
    idx = (idx + 1) % pool->hash_size;
    if (idx == start) {
      return;  // Full loop, not found
    }
  }

  // Backward-shift deletion: slide subsequent entries back to fill the gap.
  // Standard algorithm for open-addressing with linear probing.
  uint32_t gap = idx;
  uint32_t j   = idx;
  for (;;) {
    j              = (j + 1) % pool->hash_size;
    uint32_t frame = pool->hash_table[j];

    if (frame == BUFFER_FRAME_INVALID) {
      break;  // End of probe chain
    }

    // Natural (home) slot for the entry currently at slot j
    uint32_t k = hash_page_id(pool->page_ids[frame], pool->hash_size);

    // Determine if entry at j needs to be moved to gap.
    // Move is needed if k is NOT in the cyclic range (gap, j].
    // Equivalently, move if gap is in the cyclic range [k, j).
    bool should_move;
    if (j >= gap) {
      // No wrap between gap and j
      should_move = (k <= gap) || (k > j);
    } else {
      // Wrapped: gap > j
      should_move = (k <= gap) && (k > j);
    }

    if (should_move) {
      pool->hash_table[gap] = pool->hash_table[j];
      gap                   = j;
    }
  }

  pool->hash_table[gap] = BUFFER_FRAME_INVALID;
}

// ============================================================================
// Internal: Evict a frame using the Clock algorithm
// Returns frame index, or BUFFER_FRAME_INVALID if all frames are pinned
// ============================================================================
static uint32_t clock_evict(BufferPool* pool)
{
  // Scan at most 2 * pool_size slots (two full rotations)
  uint32_t max_scan = pool->pool_size * 2;

  for (uint32_t scan = 0; scan < max_scan; scan++) {
    uint32_t frame   = pool->clock_hand;
    pool->clock_hand = (pool->clock_hand + 1) % pool->pool_size;

    // Skip pinned frames
    if (pool->pin_counts[frame] > 0) {
      continue;
    }

    // Second chance: if ref bit is set, clear it and move on
    if (pool->ref_bits[frame]) {
      pool->ref_bits[frame] = 0;
      continue;
    }

    // This frame is the victim
    return frame;
  }

  // All frames are pinned
  return BUFFER_FRAME_INVALID;
}

// ============================================================================
// Internal: Flush a single frame to disk if dirty
// ============================================================================
static NostrDBError flush_frame(BufferPool* pool, uint32_t frame)
{
  if (!pool->dirty_flags[frame]) {
    return NOSTR_DB_OK;
  }

  page_id_t pid = pool->page_ids[frame];
  if (pid == PAGE_ID_NULL) {
    return NOSTR_DB_OK;
  }

  NostrDBError err = disk_write_page(pool->disk, pid, &pool->pages[frame]);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  pool->dirty_flags[frame] = 0;
  return NOSTR_DB_OK;
}

// ============================================================================
// buffer_pool_init
// ============================================================================
NostrDBError buffer_pool_init(BufferPool* pool, DiskManager* disk, uint32_t pool_size)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(disk, NOSTR_DB_ERROR_NULL_PARAM);
  require(pool_size > 0, NOSTR_DB_ERROR_NULL_PARAM);

  internal_memset(pool, 0, sizeof(BufferPool));
  pool->disk      = disk;
  pool->pool_size = pool_size;

  // Hash table size: at least 2x pool_size, rounded up to prime
  pool->hash_size = next_prime(pool_size * 2);

  // Allocate SoA arrays
  pool->page_ids = (page_id_t*)pool_alloc(pool_size * sizeof(page_id_t));
  if (is_null(pool->page_ids)) {
    goto fail;
  }

  pool->pin_counts = (uint8_t*)pool_alloc(pool_size * sizeof(uint8_t));
  if (is_null(pool->pin_counts)) {
    goto fail;
  }

  pool->dirty_flags = (uint8_t*)pool_alloc(pool_size * sizeof(uint8_t));
  if (is_null(pool->dirty_flags)) {
    goto fail;
  }

  pool->ref_bits = (uint8_t*)pool_alloc(pool_size * sizeof(uint8_t));
  if (is_null(pool->ref_bits)) {
    goto fail;
  }

  pool->pages = (PageData*)pool_alloc(pool_size * sizeof(PageData));
  if (is_null(pool->pages)) {
    goto fail;
  }

  pool->lsn = (uint64_t*)pool_alloc(pool_size * sizeof(uint64_t));
  if (is_null(pool->lsn)) {
    goto fail;
  }

  pool->hash_table = (uint32_t*)pool_alloc(pool->hash_size * sizeof(uint32_t));
  if (is_null(pool->hash_table)) {
    goto fail;
  }

  // Initialize all page_ids to PAGE_ID_NULL (empty frames)
  for (uint32_t i = 0; i < pool_size; i++) {
    pool->page_ids[i] = PAGE_ID_NULL;
  }

  // Initialize hash table to BUFFER_FRAME_INVALID
  for (uint32_t i = 0; i < pool->hash_size; i++) {
    pool->hash_table[i] = BUFFER_FRAME_INVALID;
  }

  pool->clock_hand = 0;

  return NOSTR_DB_OK;

fail:
  buffer_pool_shutdown(pool);
  return NOSTR_DB_ERROR_MMAP_FAILED;
}

// ============================================================================
// buffer_pool_shutdown
// ============================================================================
void buffer_pool_shutdown(BufferPool* pool)
{
  if (is_null(pool)) {
    return;
  }

  // Flush all dirty pages
  if (!is_null(pool->pages) && !is_null(pool->dirty_flags) && !is_null(pool->page_ids)) {
    for (uint32_t i = 0; i < pool->pool_size; i++) {
      if (pool->dirty_flags[i] && pool->page_ids[i] != PAGE_ID_NULL) {
        flush_frame(pool, i);
      }
    }
    if (!is_null(pool->disk)) {
      disk_sync(pool->disk);
    }
  }

  // Free all SoA arrays
  uint32_t ps = pool->pool_size;
  pool_free(pool->page_ids, ps * sizeof(page_id_t));
  pool_free(pool->pin_counts, ps * sizeof(uint8_t));
  pool_free(pool->dirty_flags, ps * sizeof(uint8_t));
  pool_free(pool->ref_bits, ps * sizeof(uint8_t));
  pool_free(pool->pages, ps * sizeof(PageData));
  pool_free(pool->lsn, ps * sizeof(uint64_t));
  pool_free(pool->hash_table, pool->hash_size * sizeof(uint32_t));

  internal_memset(pool, 0, sizeof(BufferPool));
}

// ============================================================================
// buffer_pool_pin
// ============================================================================
PageData* buffer_pool_pin(BufferPool* pool, page_id_t page_id)
{
  require_not_null(pool, NULL);
  require(page_id != PAGE_ID_NULL, NULL);

  // 1. Check hash table for cache hit
  uint32_t frame = hash_lookup(pool, page_id);
  if (frame != BUFFER_FRAME_INVALID) {
    pool->pin_counts[frame]++;
    pool->ref_bits[frame] = 1;
    return &pool->pages[frame];
  }

  // 2. Cache miss: find a free frame or evict one

  // First, try to find an empty frame (page_id == PAGE_ID_NULL)
  uint32_t victim = BUFFER_FRAME_INVALID;
  for (uint32_t i = 0; i < pool->pool_size; i++) {
    if (pool->page_ids[i] == PAGE_ID_NULL) {
      victim = i;
      break;
    }
  }

  // If no empty frame, use Clock eviction
  if (victim == BUFFER_FRAME_INVALID) {
    victim = clock_evict(pool);
    if (victim == BUFFER_FRAME_INVALID) {
      return NULL;  // All frames pinned, cannot evict
    }

    // Flush victim if dirty
    if (flush_frame(pool, victim) != NOSTR_DB_OK) {
      return NULL;
    }

    // Remove old mapping from hash table
    hash_remove(pool, pool->page_ids[victim]);
  }

  // 3. Load page from disk into the victim frame
  NostrDBError err = disk_read_page(pool->disk, page_id, &pool->pages[victim]);
  if (err != NOSTR_DB_OK) {
    return NULL;
  }

  // 4. Update frame metadata
  pool->page_ids[victim]    = page_id;
  pool->pin_counts[victim]  = 1;
  pool->dirty_flags[victim] = 0;
  pool->ref_bits[victim]    = 1;
  pool->lsn[victim]         = 0;

  // 5. Insert into hash table
  hash_insert(pool, page_id, victim);

  return &pool->pages[victim];
}

// ============================================================================
// buffer_pool_alloc_page
// ============================================================================
page_id_t buffer_pool_alloc_page(BufferPool* pool, PageData** out_page)
{
  require_not_null(pool, PAGE_ID_NULL);

  // Allocate a page on disk
  page_id_t new_pid = disk_alloc_page(pool->disk);
  if (new_pid == PAGE_ID_NULL) {
    return PAGE_ID_NULL;
  }

  // Find a frame for the new page (same logic as pin, but skip disk read)

  // Try empty frame first
  uint32_t frame = BUFFER_FRAME_INVALID;
  for (uint32_t i = 0; i < pool->pool_size; i++) {
    if (pool->page_ids[i] == PAGE_ID_NULL) {
      frame = i;
      break;
    }
  }

  // Evict if needed
  if (frame == BUFFER_FRAME_INVALID) {
    frame = clock_evict(pool);
    if (frame == BUFFER_FRAME_INVALID) {
      // Cannot evict, free the disk page and fail
      disk_free_page(pool->disk, new_pid);
      return PAGE_ID_NULL;
    }

    if (flush_frame(pool, frame) != NOSTR_DB_OK) {
      disk_free_page(pool->disk, new_pid);
      return PAGE_ID_NULL;
    }

    hash_remove(pool, pool->page_ids[frame]);
  }

  // Zero out the page
  internal_memset(&pool->pages[frame], 0, sizeof(PageData));

  // Set up frame metadata
  pool->page_ids[frame]    = new_pid;
  pool->pin_counts[frame]  = 1;
  pool->dirty_flags[frame] = 1;  // New page is dirty (needs to be written)
  pool->ref_bits[frame]    = 1;
  pool->lsn[frame]         = 0;

  // Insert into hash table
  hash_insert(pool, new_pid, frame);

  if (out_page != NULL) {
    *out_page = &pool->pages[frame];
  }

  return new_pid;
}

// ============================================================================
// buffer_pool_unpin
// ============================================================================
void buffer_pool_unpin(BufferPool* pool, page_id_t page_id)
{
  if (is_null(pool) || page_id == PAGE_ID_NULL) {
    return;
  }

  uint32_t frame = hash_lookup(pool, page_id);
  if (frame == BUFFER_FRAME_INVALID) {
    return;
  }

  if (pool->pin_counts[frame] > 0) {
    pool->pin_counts[frame]--;
  }
}

// ============================================================================
// buffer_pool_mark_dirty
// ============================================================================
void buffer_pool_mark_dirty(BufferPool* pool, page_id_t page_id, uint64_t lsn)
{
  if (is_null(pool) || page_id == PAGE_ID_NULL) {
    return;
  }

  uint32_t frame = hash_lookup(pool, page_id);
  if (frame == BUFFER_FRAME_INVALID) {
    return;
  }

  pool->dirty_flags[frame] = 1;
  if (lsn > pool->lsn[frame]) {
    pool->lsn[frame] = lsn;
  }
}

// ============================================================================
// buffer_pool_flush
// ============================================================================
NostrDBError buffer_pool_flush(BufferPool* pool, page_id_t page_id)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);
  require(page_id != PAGE_ID_NULL, NOSTR_DB_ERROR_NULL_PARAM);

  uint32_t frame = hash_lookup(pool, page_id);
  if (frame == BUFFER_FRAME_INVALID) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  return flush_frame(pool, frame);
}

// ============================================================================
// buffer_pool_flush_all
// ============================================================================
NostrDBError buffer_pool_flush_all(BufferPool* pool)
{
  require_not_null(pool, NOSTR_DB_ERROR_NULL_PARAM);

  for (uint32_t i = 0; i < pool->pool_size; i++) {
    if (pool->dirty_flags[i] && pool->page_ids[i] != PAGE_ID_NULL) {
      NostrDBError err = flush_frame(pool, i);
      if (err != NOSTR_DB_OK) {
        return err;
      }
    }
  }

  return disk_sync(pool->disk);
}
