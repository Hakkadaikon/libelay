#ifndef NOSTR_FUNCS_H_
#define NOSTR_FUNCS_H_

#define JSMN_HEADER
#include "../json/json_wrapper.h"
#include "nostr_types.h"

bool is_valid_nostr_event(const PJsonFuncs funcs, const char* json, const jsontok_t* token, const size_t token_count);
bool is_valid_nostr_event_id(const PJsonFuncs funcs, const char* json, const jsontok_t* token);
bool is_valid_nostr_event_pubkey(const PJsonFuncs funcs, const char* json, const jsontok_t* token);
bool is_valid_nostr_event_kind(const PJsonFuncs funcs, const char* json, const jsontok_t* token);
bool is_valid_nostr_event_created_at(const PJsonFuncs funcs, const char* json, const jsontok_t* token);
bool is_valid_nostr_event_sig(const PJsonFuncs funcs, const char* json, const jsontok_t* token);
bool is_valid_nostr_event_tags(const PJsonFuncs funcs, const char* json, const jsontok_t* token);

bool json_to_nostr_event(const char* json, PNostrEvent event);

#endif
