#ifndef _NOSTR_TYPES__
#define _NOSTR_TYPES__

#include "../util/types.h"

constexpr size_t NOSTR_KEY_LENGTH       = 33;
constexpr size_t NOSTR_TAG_LENGTH       = 2048;
constexpr size_t NOSTR_TAG_KEY_LENGTH   = 32;
constexpr size_t NOSTR_TAG_VALUE_COUNT  = 16;
constexpr size_t NOSTR_TAG_VALUE_LENGTH = 512;
constexpr size_t NOSTR_CONTENT_LENGTH   = 1 * 1024 * 1024;

typedef struct {
  char   key[ALIGN_UP_8(NOSTR_TAG_KEY_LENGTH)];
  char   values[NOSTR_TAG_VALUE_COUNT][ALIGN_UP_8(NOSTR_TAG_VALUE_LENGTH)];
  size_t item_count;
} NostrTag, *PNostrTag;

typedef struct {
  char     id[ALIGN_UP_8(NOSTR_KEY_LENGTH)];
  char     pubkey[ALIGN_UP_8(NOSTR_KEY_LENGTH)];
  uint32_t kind;
  uint32_t tag_count;
  uint64_t created_at;
  NostrTag tags[NOSTR_TAG_LENGTH];
  char     content[ALIGN_UP_8(NOSTR_CONTENT_LENGTH)];
} NostrEvent, *PNostrEvent;

#endif
