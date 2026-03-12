#include "event_serializer.h"

#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "../db_types.h"

// ============================================================================
// Internal: Convert single hex character to numeric value
// ============================================================================
static int32_t hex_char_to_val(char c)
{
  if (is_digit(c)) {
    return c - '0';
  }
  if (is_lower(c) && c <= 'f') {
    return c - 'a' + 10;
  }
  if (is_upper(c) && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

// ============================================================================
// Internal: Convert hex string to raw bytes
// ============================================================================
static bool hex_to_raw(const char* hex, size_t hex_len, uint8_t* out,
                       size_t out_len)
{
  if (hex_len != out_len * 2) {
    return false;
  }

  for (size_t i = 0; i < out_len; i++) {
    int32_t h = hex_char_to_val(hex[i * 2]);
    int32_t l = hex_char_to_val(hex[i * 2 + 1]);
    if (h < 0 || l < 0) {
      return false;
    }
    out[i] = (uint8_t)((h << 4) | l);
  }

  return true;
}

// ============================================================================
// Internal: Convert raw bytes to hex string
// ============================================================================
static void raw_to_hex(const uint8_t* bytes, size_t len, char* hex)
{
  static const char hex_chars[] = "0123456789abcdef";
  for (size_t i = 0; i < len; i++) {
    hex[i * 2]     = hex_chars[(bytes[i] >> 4) & 0x0F];
    hex[i * 2 + 1] = hex_chars[bytes[i] & 0x0F];
  }
  hex[len * 2] = '\0';
}

// Reuse existing tag serialization functions (defined in db_tags.c)
extern int64_t nostr_db_serialize_tags(const NostrTagEntity* tags,
                                       uint32_t tag_count, uint8_t* buffer,
                                       size_t capacity);
extern int32_t nostr_db_deserialize_tags(const uint8_t* buffer, size_t length,
                                         NostrTagEntity* tags,
                                         uint32_t        max_tags);

// ============================================================================
// event_serialize
// ============================================================================
int32_t event_serialize(const NostrEventEntity* event, uint8_t* buffer,
                        uint16_t capacity)
{
  require_not_null(event, -1);
  require_not_null(buffer, -1);

  // Serialize tags first to get length
  uint8_t tags_buf[8192];
  int64_t tags_size = nostr_db_serialize_tags(event->tags, event->tag_count,
                                              tags_buf, sizeof(tags_buf));
  if (tags_size < 0) {
    tags_size   = 2;
    tags_buf[0] = 0;
    tags_buf[1] = 0;
  }

  uint16_t content_len = (uint16_t)strlen(event->content);
  uint16_t total_size =
    (uint16_t)(sizeof(EventRecord) + content_len + (uint16_t)tags_size);

  if (total_size > capacity) {
    return -1;
  }

  // Build EventRecord header
  EventRecord rec;
  internal_memset(&rec, 0, sizeof(rec));

  // Convert hex ID to binary
  if (!hex_to_raw(event->id, 64, rec.id, 32)) {
    return -1;
  }

  // Convert hex pubkey to binary
  if (!hex_to_raw(event->pubkey, 64, rec.pubkey, 32)) {
    return -1;
  }

  // Convert hex signature to binary
  if (!hex_to_raw(event->sig, 128, rec.sig, 64)) {
    return -1;
  }

  rec.created_at     = event->created_at;
  rec.kind           = event->kind;
  rec.flags          = 0;
  rec.content_length = content_len;
  rec.tags_length    = (uint16_t)tags_size;

  // Write header
  uint8_t* dst = buffer;
  internal_memcpy(dst, &rec, sizeof(EventRecord));
  dst += sizeof(EventRecord);

  // Write content
  internal_memcpy(dst, event->content, content_len);
  dst += content_len;

  // Write tags
  internal_memcpy(dst, tags_buf, (size_t)tags_size);

  return (int32_t)total_size;
}

// ============================================================================
// event_deserialize
// ============================================================================
NostrDBError event_deserialize(const uint8_t* buffer, uint16_t length,
                               NostrEventEntity* event)
{
  require_not_null(buffer, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(event, NOSTR_DB_ERROR_NULL_PARAM);
  require(length >= sizeof(EventRecord), NOSTR_DB_ERROR_INVALID_EVENT);

  const EventRecord* rec = (const EventRecord*)buffer;

  // NOTE: Do NOT memset the entire NostrEventEntity here (it's >1MB).
  // The caller is responsible for zeroing the struct before calling.
  // We only write the fields we need.

  // Convert binary ID to hex
  raw_to_hex(rec->id, 32, event->id);

  // Convert binary pubkey to hex
  raw_to_hex(rec->pubkey, 32, event->pubkey);

  // Convert binary signature to hex
  raw_to_hex(rec->sig, 64, event->sig);

  event->created_at = rec->created_at;
  event->kind       = rec->kind;

  // Read content
  uint16_t content_len = rec->content_length;
  if (sizeof(EventRecord) + content_len > length) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  const uint8_t* content_ptr = buffer + sizeof(EventRecord);
  size_t         copy_len    = content_len;
  if (copy_len >= NOSTR_EVENT_CONTENT_LENGTH) {
    copy_len = NOSTR_EVENT_CONTENT_LENGTH - 1;
  }
  internal_memcpy(event->content, content_ptr, copy_len);
  event->content[copy_len] = '\0';

  // Read tags
  uint16_t tags_len = rec->tags_length;
  if (sizeof(EventRecord) + content_len + tags_len > length) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  const uint8_t* tags_ptr  = content_ptr + content_len;
  int32_t        tag_count = nostr_db_deserialize_tags(
    tags_ptr, tags_len, event->tags, NOSTR_EVENT_TAG_LENGTH);
  if (tag_count > 0) {
    event->tag_count = (uint32_t)tag_count;
  }

  return NOSTR_DB_OK;
}
