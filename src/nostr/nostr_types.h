#ifndef NOSTR_TYPES_H_
#define NOSTR_TYPES_H_

#include "../util/types.h"

#define NOSTR_EVENT_TAG_LENGTH (2 * 1024)
#define NOSTR_EVENT_TAG_VALUE_COUNT 16
#define NOSTR_EVENT_TAG_VALUE_LENGTH 512
#define NOSTR_EVENT_CONTENT_LENGTH (1 * 1024 * 1024)

#define NOSTR_REQ_IDS_LENGTH 512
#define NOSTR_REQ_AUTHORS_LENGTH NOSTR_EVENT_TAG_LENGTH
#define NOSTR_REQ_KINDS_LENGTH 512
#define NOSTR_REQ_TAGS_LENGTH 512

typedef struct {
  char   key[64];
  char   values[NOSTR_EVENT_TAG_VALUE_COUNT][NOSTR_EVENT_TAG_VALUE_LENGTH];
  size_t item_count;
} NostrTagEntity, *PNostrTagEntity;

typedef struct {
  char           id[65];
  char           dummy1[7];
  char           pubkey[65];
  char           dummy2[7];
  uint32_t       kind;
  uint32_t       tag_count;
  time_t         created_at;
  NostrTagEntity tags[NOSTR_EVENT_TAG_LENGTH];
  char           content[NOSTR_EVENT_CONTENT_LENGTH];
  char           sig[129];
  char           dummy3[7];
} NostrEventEntity, *PNostrEventEntity;

typedef struct {
  uint32_t ids[NOSTR_REQ_IDS_LENGTH];
  uint32_t authors[NOSTR_REQ_AUTHORS_LENGTH];
  uint32_t kinds[NOSTR_REQ_KINDS_LENGTH];
  int32_t  tags[NOSTR_REQ_TAGS_LENGTH];
  time_t   since;
  time_t   until;
  size_t   limit;
} NostrReqEntity, *PNostrReqEntity;

/**
 * @brief NIP-11 relay information document structure
 */
typedef struct {
  const char* name;            ///< Relay name (recommended < 30 chars)
  const char* description;     ///< Relay description
  const char* pubkey;          ///< Admin pubkey (32-byte hex)
  const char* contact;         ///< Contact URI (e.g., mailto:, nostr:)
  const char* software;        ///< URL to relay software project
  const char* version;         ///< Software version
  const int*  supported_nips;  ///< Array of supported NIP numbers (NULL-terminated with -1)
} NostrRelayInfo, *PNostrRelayInfo;

#endif
