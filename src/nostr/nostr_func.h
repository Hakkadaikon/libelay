#ifndef NOSTR_FUNCS_H_
#define NOSTR_FUNCS_H_

#define JSMN_HEADER
#include "../json/json_wrapper.h"
#include "nostr_types.h"

typedef bool (*PNostrEventCallback)(const NostrEventEntity* event);
typedef bool (*PNostrRequestCallback)(const NostrReqEntity* req);

typedef struct {
  PNostrEventCallback   event;
  PNostrRequestCallback req;
} NostrFuncs, *PNostrFuncs;

bool extract_nostr_event(const PJsonFuncs funcs, const char* json, const jsontok_t* token, const size_t token_count, NostrEventEntity* event);
bool nostr_event_handler(const char* json, PNostrFuncs nostr_funcs);

/**
 * @brief Generate NIP-11 relay information JSON
 *
 * @param[in]  info            Relay information structure
 * @param[in]  buffer_capacity Buffer capacity
 * @param[out] buffer          Output buffer for JSON
 *
 * @return true on success, false on failure
 */
bool nostr_nip11_response(const PNostrRelayInfo info, const size_t buffer_capacity, char* buffer);

#endif
