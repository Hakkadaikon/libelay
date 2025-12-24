#include "nostr_func.h"

#include "../json/json_wrapper.h"
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

  if (token_count < 2) {
    log_debug("JSON error: Parse error\n");
    var_debug("token_count:", token_count);
    return false;
  }

  if (!funcs.is_array(&token[0])) {
    log_debug("JSON error: json is not array\n");
    return false;
  }

  if (funcs.is_array(&token[0]) || funcs.is_object(&token[2])) {
    log_debug("JSON error: Invalid format\n");
    return false;
  }

  return true;
}

// int main(void)
//{
//   const char* json = "[\"EVENT\",{\"id\":\"dc097cd6bd76f2d8816f8a2d294e8442173228e5b24fb946aa05dd89339c9168\",\"pubkey\":\"79be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798\",\"created_at\":1723212754,\"kind\":1,\"tags\":[[\"e\",\"event_id_ex\"],[\"p\",\"pubkey_ex\",\"wss://relay\"]],\"content\":\"Hello No-Libc!\",\"sig\":\"5d2f49649a4f448d13757ee563fd1b8fa04e4dc1931dd34763fb7df40a082cbdc4e136c733177d3b96a0321f8783fd6b218fea046e039a23d99b1ab9e2d8b45f\"}]";
//   NostrEvent  event;
//
//   json_to_nostr_event(json, &event);
// }
