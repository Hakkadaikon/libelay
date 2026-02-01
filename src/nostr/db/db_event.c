#include "../../arch/memory.h"
#include "../../util/string.h"
#include "db.h"
#include "db_file.h"
#include "db_internal.h"
#include "db_mmap.h"

// ============================================================================
// Helper: Convert hex string to binary (32 bytes = 64 hex chars)
// ============================================================================
static bool hex_to_bytes(const char* hex, size_t hex_len, uint8_t* out, size_t out_len)
{
  if (hex_len != out_len * 2) {
    return false;
  }

  for (size_t i = 0; i < out_len; i++) {
    char high = hex[i * 2];
    char low  = hex[i * 2 + 1];

    uint8_t h = 0, l = 0;

    if (high >= '0' && high <= '9') {
      h = (uint8_t)(high - '0');
    } else if (high >= 'a' && high <= 'f') {
      h = (uint8_t)(high - 'a' + 10);
    } else if (high >= 'A' && high <= 'F') {
      h = (uint8_t)(high - 'A' + 10);
    } else {
      return false;
    }

    if (low >= '0' && low <= '9') {
      l = (uint8_t)(low - '0');
    } else if (low >= 'a' && low <= 'f') {
      l = (uint8_t)(low - 'a' + 10);
    } else if (low >= 'A' && low <= 'F') {
      l = (uint8_t)(low - 'A' + 10);
    } else {
      return false;
    }

    out[i] = (uint8_t)((h << 4) | l);
  }

  return true;
}

// ============================================================================
// Helper: Convert binary to hex string
// ============================================================================
static void bytes_to_hex(const uint8_t* bytes, size_t len, char* hex)
{
  static const char hex_chars[] = "0123456789abcdef";

  for (size_t i = 0; i < len; i++) {
    hex[i * 2]     = hex_chars[(bytes[i] >> 4) & 0x0F];
    hex[i * 2 + 1] = hex_chars[bytes[i] & 0x0F];
  }
  hex[len * 2] = '\0';
}

// ============================================================================
// Helper: Compare two 32-byte IDs
// ============================================================================
static bool id_equals(const uint8_t* a, const uint8_t* b)
{
  for (int i = 0; i < 32; i++) {
    if (a[i] != b[i]) {
      return false;
    }
  }
  return true;
}

// ============================================================================
// Helper: Calculate aligned size (8-byte alignment)
// ============================================================================
static size_t align8(size_t size)
{
  return (size + 7) & ~((size_t)7);
}

// ============================================================================
// Helper: Calculate event record size
// ============================================================================
static size_t calculate_event_size(const NostrEventEntity* event, size_t tags_size)
{
  // Header + Body base + content + tags_length + tags + padding
  size_t size = sizeof(NostrDBEventHeader) +
                sizeof(NostrDBEventBody) +
                strlen(event->content) +
                sizeof(uint32_t) +  // tags_length
                tags_size;
  return align8(size);
}

// ============================================================================
// Helper: Simple hash for ID (first 8 bytes as uint64)
// ============================================================================
static uint64_t id_hash(const uint8_t* id)
{
  uint64_t hash = 0;
  for (int i = 0; i < 8; i++) {
    hash |= ((uint64_t)id[i]) << (i * 8);
  }
  return hash;
}

// ============================================================================
// nostr_db_write_event
// ============================================================================
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(event, NOSTR_DB_ERROR_NULL_PARAM);

  // Convert event ID from hex to binary
  uint8_t id_bytes[32];
  if (!hex_to_bytes(event->id, 64, id_bytes, 32)) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  // Convert pubkey from hex to binary
  uint8_t pubkey_bytes[32];
  if (!hex_to_bytes(event->pubkey, 64, pubkey_bytes, 32)) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  // Convert sig from hex to binary
  uint8_t sig_bytes[64];
  if (!hex_to_bytes(event->sig, 128, sig_bytes, 64)) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  // Serialize tags
  uint8_t tags_buffer[8192];
  int64_t tags_size = nostr_db_serialize_tags(
    event->tags, (uint32_t)event->tag_count, tags_buffer, sizeof(tags_buffer));
  if (tags_size < 0) {
    tags_size      = 2;  // Minimum: just tag count (0)
    tags_buffer[0] = 0;
    tags_buffer[1] = 0;
  }

  // Calculate total record size
  size_t content_len = strlen(event->content);
  size_t record_size = calculate_event_size(event, (size_t)tags_size);

  // Check if we need to extend the file
  uint64_t write_offset = db->events_header->next_write_offset;
  if (write_offset + record_size > db->events_map_size) {
    // Need to extend file - for now, return error
    // TODO: Implement file extension
    return NOSTR_DB_ERROR_FULL;
  }

  // Write event header
  uint8_t*            write_ptr  = (uint8_t*)db->events_map + write_offset;
  NostrDBEventHeader* evt_header = (NostrDBEventHeader*)write_ptr;
  evt_header->total_length       = (uint32_t)record_size;
  evt_header->flags              = 0;
  internal_memcpy(evt_header->id, id_bytes, 32);
  evt_header->created_at = event->created_at;
  write_ptr += sizeof(NostrDBEventHeader);

  // Write event body
  NostrDBEventBody* evt_body = (NostrDBEventBody*)write_ptr;
  internal_memcpy(evt_body->pubkey, pubkey_bytes, 32);
  internal_memcpy(evt_body->sig, sig_bytes, 64);
  evt_body->kind           = (uint32_t)event->kind;
  evt_body->content_length = (uint32_t)content_len;
  write_ptr += sizeof(NostrDBEventBody);

  // Write content
  internal_memcpy(write_ptr, event->content, content_len);
  write_ptr += content_len;

  // Write tags length and tags
  uint32_t tags_len = (uint32_t)tags_size;
  internal_memcpy(write_ptr, &tags_len, sizeof(uint32_t));
  write_ptr += sizeof(uint32_t);
  internal_memcpy(write_ptr, tags_buffer, (size_t)tags_size);

  // Update header
  db->events_header->next_write_offset = write_offset + record_size;
  db->events_header->event_count++;

  // TODO: Update all indexes
  // For now, we skip index updates - they will be implemented in Phase 4

  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_get_event_by_id
// ============================================================================
NostrDBError nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id, NostrEventEntity* out)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(id, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out, NOSTR_DB_ERROR_NULL_PARAM);

  // Linear scan through events (TODO: use ID index)
  uint64_t offset     = sizeof(NostrDBEventsHeader);
  uint64_t end_offset = db->events_header->next_write_offset;

  while (offset < end_offset) {
    NostrDBEventHeader* evt_header = (NostrDBEventHeader*)((uint8_t*)db->events_map + offset);

    // Skip deleted events
    if ((evt_header->flags & NOSTR_DB_EVENT_FLAG_DELETED) == 0) {
      // Check ID match
      if (id_equals(evt_header->id, id)) {
        // Found it! Deserialize
        internal_memset(out, 0, sizeof(NostrEventEntity));

        // Convert ID to hex
        bytes_to_hex(evt_header->id, 32, out->id);

        // Set created_at
        out->created_at = evt_header->created_at;

        // Read body
        NostrDBEventBody* evt_body = (NostrDBEventBody*)((uint8_t*)evt_header + sizeof(NostrDBEventHeader));

        // Convert pubkey to hex
        bytes_to_hex(evt_body->pubkey, 32, out->pubkey);

        // Convert sig to hex
        bytes_to_hex(evt_body->sig, 64, out->sig);

        // Set kind
        out->kind = evt_body->kind;

        // Copy content
        uint8_t* content_ptr = (uint8_t*)evt_body + sizeof(NostrDBEventBody);
        size_t   copy_len    = evt_body->content_length;
        if (copy_len >= sizeof(out->content)) {
          copy_len = sizeof(out->content) - 1;
        }
        internal_memcpy(out->content, content_ptr, copy_len);
        out->content[copy_len] = '\0';

        // Read tags
        uint8_t* tags_len_ptr = content_ptr + evt_body->content_length;
        uint32_t tags_len;
        internal_memcpy(&tags_len, tags_len_ptr, sizeof(uint32_t));

        uint8_t* tags_ptr  = tags_len_ptr + sizeof(uint32_t);
        int32_t  tag_count = nostr_db_deserialize_tags(
          tags_ptr, tags_len, out->tags, NOSTR_EVENT_TAG_LENGTH);
        if (tag_count > 0) {
          out->tag_count = (uint32_t)tag_count;
        }

        return NOSTR_DB_OK;
      }
    }

    // Move to next event
    offset += evt_header->total_length;
  }

  return NOSTR_DB_ERROR_NOT_FOUND;
}

// ============================================================================
// nostr_db_delete_event
// ============================================================================
NostrDBError nostr_db_delete_event(NostrDB* db, const uint8_t* id)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(id, NOSTR_DB_ERROR_NULL_PARAM);

  // Linear scan through events (TODO: use ID index)
  uint64_t offset     = sizeof(NostrDBEventsHeader);
  uint64_t end_offset = db->events_header->next_write_offset;

  while (offset < end_offset) {
    NostrDBEventHeader* evt_header = (NostrDBEventHeader*)((uint8_t*)db->events_map + offset);

    // Skip already deleted events
    if ((evt_header->flags & NOSTR_DB_EVENT_FLAG_DELETED) == 0) {
      // Check ID match
      if (id_equals(evt_header->id, id)) {
        // Mark as deleted (logical delete)
        evt_header->flags |= NOSTR_DB_EVENT_FLAG_DELETED;
        db->events_header->deleted_count++;

        // TODO: Update indexes

        return NOSTR_DB_OK;
      }
    }

    // Move to next event
    offset += evt_header->total_length;
  }

  return NOSTR_DB_ERROR_NOT_FOUND;
}
