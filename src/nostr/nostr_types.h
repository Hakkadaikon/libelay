#ifndef NOSTR_TYPES_H_
#define NOSTR_TYPES_H_

#include "../util/types.h"

#define NOSTR_KEY_LENGTH 33
#define NOSTR_TAG_LENGTH 2048
#define NOSTR_TAG_KEY_LENGTH 32
#define NOSTR_TAG_VALUE_COUNT 16
#define NOSTR_TAG_VALUE_LENGTH 512
#define NOSTR_CONTENT_LENGTH (1 * 1024 * 1024)

typedef struct {
  char   key[NOSTR_TAG_KEY_LENGTH];
  char   values[NOSTR_TAG_VALUE_COUNT][NOSTR_TAG_VALUE_LENGTH];
  size_t item_count;
} NostrTag, *PNostrTag;

typedef struct {
  char     id[NOSTR_KEY_LENGTH];
  char     pubkey[NOSTR_KEY_LENGTH];
  uint32_t kind;
  uint32_t tag_count;
  uint64_t created_at;
  NostrTag tags[NOSTR_TAG_LENGTH];
  char     content[NOSTR_CONTENT_LENGTH];
} NostrEvent, *PNostrEvent;

#endif
