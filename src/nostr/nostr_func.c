#include "nostr_func.h"

#include "../util/log.h"
#include "../util/string.h"
#include "nostr_types.h"

bool json_to_nostr_event(const char* json, PNostrEvent event)
{
  JsonFuncs  funcs;
  JsonParser parser;
  jsontok_t  token[JSON_TOKEN_CAPACITY];

  json_funcs_init(&funcs);

  funcs.init(&parser);

  size_t json_len = strlen(json);

  int32_t token_count = funcs.parse(
    &parser,
    json,
    json_len,
    token,
    JSON_TOKEN_CAPACITY);

  if (token_count < 5) {
    log_debug("JSON error: Parse error\n");
    var_debug("token_count:", token_count);
    return false;
  }

  if (!funcs.is_array(&token[0])) {
    log_debug("JSON error: json is not array\n");
    return false;
  }

  if (!funcs.is_string(&token[1])) {
    log_debug("JSON error: array[0] is not a string\n");
    return false;
  }

  if (!funcs.strncmp(json, &token[1], "EVENT", 5)) {
    log_debug("JSON error: array[0] is not a \"EVENT\"\n");
    return false;
  }

  if (!funcs.is_object(&token[2])) {
    log_debug("JSON error: Invalid EVENT format\n");
    return false;
  }

  if (!is_valid_nostr_event(&funcs, json, &token[3], token_count - 3)) {
    return false;
  }

  return true;
}
