#ifndef NOSTR_DB_INTERNAL_H_
#define NOSTR_DB_INTERNAL_H_

#include "../../util/types.h"
#include "../nostr_types.h"
#include "db_types.h"

// ============================================================================
// Events file header (64 bytes, 8-byte aligned)
// ============================================================================
typedef struct {
  char     magic[8];           // "NOSTRDB\0"
  uint32_t version;            // NOSTR_DB_VERSION (1)
  uint32_t flags;              // Reserved for future use
  uint64_t event_count;        // Number of stored events
  uint64_t next_write_offset;  // Next write position
  uint64_t deleted_count;      // Number of logically deleted events
  uint64_t file_size;          // Current file size
  uint8_t  reserved[16];       // Reserved for future extension
} NostrDBEventsHeader;

// Static assertion for header size
_Static_assert(sizeof(NostrDBEventsHeader) == 64, "NostrDBEventsHeader must be 64 bytes");

// ============================================================================
// Index file common header (64 bytes)
// ============================================================================
typedef struct {
  char     magic[8];           // Index type identifier
  uint32_t version;            // NOSTR_DB_VERSION (1)
  uint32_t flags;              // Reserved for future use
  uint64_t bucket_count;       // Number of hash buckets
  uint64_t entry_count;        // Number of registered entries
  uint64_t pool_next_offset;   // Next write position in entry pool
  uint64_t pool_size;          // Entry pool size
  uint8_t  reserved[16];       // Reserved for future extension
} NostrDBIndexHeader;

// Static assertion for header size
_Static_assert(sizeof(NostrDBIndexHeader) == 64, "NostrDBIndexHeader must be 64 bytes");

// ============================================================================
// Event header in events.dat (48 bytes)
// ============================================================================
typedef struct {
  uint32_t total_length;       // Total size of this event record
  uint32_t flags;              // bit0 = deleted
  uint8_t  id[32];             // Event ID (raw bytes)
  int64_t  created_at;         // Unix timestamp
} NostrDBEventHeader;

// Static assertion for header size
_Static_assert(sizeof(NostrDBEventHeader) == 48, "NostrDBEventHeader must be 48 bytes");

// ============================================================================
// Event body (variable length, follows NostrDBEventHeader)
// ============================================================================
typedef struct {
  uint8_t  pubkey[32];         // Public key (raw bytes)
  uint8_t  sig[64];            // Signature (raw bytes)
  uint32_t kind;               // Event type
  uint32_t content_length;     // Content length
  // Followed by:
  // char content[content_length];  // Variable length content
  // uint32_t tags_length;          // Serialized tags length
  // uint8_t tags[tags_length];     // Serialized tags
  // padding to 8-byte boundary
} NostrDBEventBody;

// Static assertion for minimum body size
_Static_assert(sizeof(NostrDBEventBody) == 104, "NostrDBEventBody base must be 104 bytes");

// ============================================================================
// Tag serialization format:
// [tag_count: uint16_t]
// For each tag:
//   [value_count: uint8_t][name_len: uint8_t][name: bytes]
//   For each value:
//     [value_len: uint16_t][value: bytes]
// ============================================================================

// ============================================================================
// NostrDB internal structure
// ============================================================================
struct NostrDB {
  // File descriptors
  int32_t events_fd;
  int32_t id_idx_fd;
  int32_t pubkey_idx_fd;
  int32_t kind_idx_fd;
  int32_t pubkey_kind_idx_fd;
  int32_t tag_idx_fd;
  int32_t timeline_idx_fd;

  // mmap addresses
  void* events_map;
  void* id_idx_map;
  void* pubkey_idx_map;
  void* kind_idx_map;
  void* pubkey_kind_idx_map;
  void* tag_idx_map;
  void* timeline_idx_map;

  // Map sizes
  size_t events_map_size;
  size_t id_idx_map_size;
  size_t pubkey_idx_map_size;
  size_t kind_idx_map_size;
  size_t pubkey_kind_idx_map_size;
  size_t tag_idx_map_size;
  size_t timeline_idx_map_size;

  // Header pointers (cast from mmap)
  NostrDBEventsHeader* events_header;
  NostrDBIndexHeader*  id_idx_header;
  NostrDBIndexHeader*  pubkey_idx_header;
  NostrDBIndexHeader*  kind_idx_header;
  NostrDBIndexHeader*  pubkey_kind_idx_header;
  NostrDBIndexHeader*  tag_idx_header;
  NostrDBIndexHeader*  timeline_idx_header;

  // Data directory path
  char data_dir[256];
};

// ============================================================================
// Tag serialization functions (declarations)
// ============================================================================

/**
 * @brief Serialize tags to binary format
 * @param tags Array of tags
 * @param tag_count Number of tags
 * @param buffer Output buffer
 * @param capacity Buffer capacity
 * @return Number of bytes written, or -1 on error
 */
int64_t nostr_db_serialize_tags(
    const NostrTagEntity* tags,
    uint32_t tag_count,
    uint8_t* buffer,
    size_t capacity);

/**
 * @brief Deserialize tags from binary format
 * @param buffer Input buffer
 * @param length Buffer length
 * @param tags Output tags array
 * @param max_tags Maximum number of tags
 * @return Number of tags deserialized, or -1 on error
 */
int32_t nostr_db_deserialize_tags(
    const uint8_t* buffer,
    size_t length,
    NostrTagEntity* tags,
    uint32_t max_tags);

#endif
