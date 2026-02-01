#include "../nostr_types.h"
#include "../../arch/memory.h"
#include "../../util/string.h"
#include "db_internal.h"

/**
 * Tag serialization format:
 * [tag_count: uint16_t]
 * For each tag:
 *   [value_count: uint8_t][name_len: uint8_t][name: bytes]
 *   For each value:
 *     [value_len: uint16_t][value: bytes]
 */

int64_t nostr_db_serialize_tags(
    const NostrTagEntity* tags,
    uint32_t tag_count,
    uint8_t* buffer,
    size_t capacity)
{
  require_not_null(buffer, -1);
  require(capacity >= 2, -1);

  uint8_t* ptr = buffer;
  uint8_t* end = buffer + capacity;

  // Write tag count (uint16_t)
  require(tag_count <= 0xFFFF, -1);
  require(ptr + 2 <= end, -1);

  uint16_t tag_count_u16 = (uint16_t)tag_count;
  ptr[0] = (uint8_t)(tag_count_u16 & 0xFF);
  ptr[1] = (uint8_t)((tag_count_u16 >> 8) & 0xFF);
  ptr += 2;

  for (uint32_t i = 0; i < tag_count; i++) {
    const NostrTagEntity* tag = &tags[i];

    // Count values (item_count is already the value count)
    uint8_t value_count = (uint8_t)tag->item_count;
    if (tag->item_count > 255) {
      value_count = 255;
    }

    // Get tag name length (key field is the tag name)
    size_t name_len = strlen(tag->key);
    if (name_len > 255) {
      name_len = 255;
    }

    // Write value_count (uint8_t) and name_len (uint8_t)
    require(ptr + 2 + name_len <= end, -1);

    *ptr++ = value_count;
    *ptr++ = (uint8_t)name_len;

    // Write tag name
    internal_memcpy(ptr, tag->key, name_len);
    ptr += name_len;

    // Write each value
    for (uint8_t j = 0; j < value_count; j++) {
      size_t value_len = strlen(tag->values[j]);
      if (value_len > 0xFFFF) {
        value_len = 0xFFFF;
      }

      // Write value_len (uint16_t)
      require(ptr + 2 + value_len <= end, -1);

      ptr[0] = (uint8_t)(value_len & 0xFF);
      ptr[1] = (uint8_t)((value_len >> 8) & 0xFF);
      ptr += 2;

      // Write value
      internal_memcpy(ptr, tag->values[j], value_len);
      ptr += value_len;
    }
  }

  return (int64_t)(ptr - buffer);
}

int32_t nostr_db_deserialize_tags(
    const uint8_t* buffer,
    size_t length,
    NostrTagEntity* tags,
    uint32_t max_tags)
{
  require_not_null(buffer, -1);
  require_not_null(tags, -1);
  require(length >= 2, -1);

  const uint8_t* ptr = buffer;
  const uint8_t* end = buffer + length;

  // Read tag count (uint16_t)
  uint16_t tag_count = (uint16_t)(ptr[0] | (ptr[1] << 8));
  ptr += 2;

  if (tag_count > max_tags) {
    tag_count = (uint16_t)max_tags;
  }

  for (uint16_t i = 0; i < tag_count; i++) {
    if (ptr + 2 > end) {
      return (int32_t)i;  // Return number of tags successfully read
    }

    // Read value_count and name_len
    uint8_t value_count = *ptr++;
    uint8_t name_len = *ptr++;

    if (ptr + name_len > end) {
      return (int32_t)i;
    }

    // Read tag name
    size_t copy_len = name_len;
    if (copy_len > sizeof(tags[i].key) - 1) {
      copy_len = sizeof(tags[i].key) - 1;
    }
    internal_memcpy(tags[i].key, ptr, copy_len);
    tags[i].key[copy_len] = '\0';
    ptr += name_len;

    // Limit value count
    if (value_count > NOSTR_EVENT_TAG_VALUE_COUNT) {
      value_count = NOSTR_EVENT_TAG_VALUE_COUNT;
    }
    tags[i].item_count = value_count;

    // Read each value
    for (uint8_t j = 0; j < value_count; j++) {
      if (ptr + 2 > end) {
        tags[i].item_count = j;
        return (int32_t)(i + 1);
      }

      // Read value_len (uint16_t)
      uint16_t value_len = (uint16_t)(ptr[0] | (ptr[1] << 8));
      ptr += 2;

      if (ptr + value_len > end) {
        tags[i].item_count = j;
        return (int32_t)(i + 1);
      }

      // Read value
      size_t vcopy_len = value_len;
      if (vcopy_len > NOSTR_EVENT_TAG_VALUE_LENGTH - 1) {
        vcopy_len = NOSTR_EVENT_TAG_VALUE_LENGTH - 1;
      }
      internal_memcpy(tags[i].values[j], ptr, vcopy_len);
      tags[i].values[j][vcopy_len] = '\0';
      ptr += value_len;
    }
  }

  return (int32_t)tag_count;
}
