#include "../../util/log.h"
#include "../../util/string.h"
#include "../nostr_func.h"

bool is_valid_nostr_event(const PJsonFuncs funcs, const char* json, const jsontok_t* token, const size_t token_count)
{
  for (int i = 0; i < token_count; i += 2) {
    int key_index   = i;
    int value_index = i + 1;

    if (!funcs->is_string(&token[key_index])) {
      log_debug("JSON error: key is not string\n");
      return false;
    }

    if (funcs->strncmp(json, &token[key_index], "id", 2)) {
      log_debug("id found\n");

      if (!is_valid_nostr_event_id(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }

    if (funcs->strncmp(json, &token[key_index], "pubkey", 6)) {
      log_debug("pubkey found\n");

      if (!is_valid_nostr_event_pubkey(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }

    if (funcs->strncmp(json, &token[key_index], "kind", 4)) {
      log_debug("kind found\n");

      if (!is_valid_nostr_event_kind(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }

    if (funcs->strncmp(json, &token[key_index], "created_at", 10)) {
      log_debug("created_at found\n");

      if (!is_valid_nostr_event_created_at(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }

    if (funcs->strncmp(json, &token[key_index], "sig", 3)) {
      log_debug("sig found\n");

      if (!is_valid_nostr_event_sig(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }

    if (funcs->strncmp(json, &token[key_index], "tags", 4)) {
      log_debug("tags found\n");

      if (!is_valid_nostr_event_tags(funcs, json, &token[value_index])) {
        return false;
      }

      continue;
    }
  }

  return true;
}
