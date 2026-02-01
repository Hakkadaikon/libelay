#include "nostr_func.h"

#include "../util/allocator.h"
#include "../util/log.h"
#include "../util/string.h"
#include "nostr_types.h"

static size_t append_string(char* buffer, size_t pos, size_t capacity, const char* str);
static size_t append_json_string_field(char* buffer, size_t pos, size_t capacity, const char* key, const char* value, bool add_comma);
static size_t append_json_nips_array(char* buffer, size_t pos, size_t capacity, const int* nips, bool add_comma);

bool nostr_event_handler(const char* json, PNostrFuncs nostr_funcs)
{
  JsonFuncs  json_funcs;
  JsonParser parser;
  jsontok_t  token[JSON_TOKEN_CAPACITY];

  json_funcs_init(&json_funcs);

  json_funcs.init(&parser);

  size_t json_len = strlen(json);

  int32_t token_count = json_funcs.parse(
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

  if (!json_funcs.is_array(&token[0])) {
    log_debug("JSON error: json is not array\n");
    return false;
  }

  if (!json_funcs.is_string(&token[1])) {
    log_debug("JSON error: array[0] is not a string\n");
    return false;
  }

  if (!json_funcs.strncmp(json, &token[1], "EVENT", 5)) {
    if (!json_funcs.is_object(&token[2])) {
      log_debug("JSON error: Invalid EVENT format\n");
      return false;
    }

    NostrEventEntity event;

    if (!extract_nostr_event(
          &json_funcs,
          json,
          &token[3],
          token_count - 3,
          &event)) {
      log_debug("Nostr Event Error: Invalid Nostr JSON format\n");
      return false;
    }

    log_debug("Nostr funcs\n");
    return nostr_funcs->event(&event);
  }

  if (!json_funcs.strncmp(json, &token[1], "REQ", 3)) {
  }

  return false;
}

static size_t append_string(char* buffer, size_t pos, size_t capacity, const char* str)
{
  size_t len = strlen(str);
  if (pos + len >= capacity) {
    return pos;
  }
  websocket_memcpy(buffer + pos, str, len);
  return pos + len;
}

static size_t append_json_string_field(char* buffer, size_t pos, size_t capacity, const char* key, const char* value, bool add_comma)
{
  if (is_null(value)) {
    return pos;
  }

  if (add_comma) {
    pos = append_string(buffer, pos, capacity, ",");
  }
  pos = append_string(buffer, pos, capacity, "\"");
  pos = append_string(buffer, pos, capacity, key);
  pos = append_string(buffer, pos, capacity, "\":\"");
  pos = append_string(buffer, pos, capacity, value);
  pos = append_string(buffer, pos, capacity, "\"");

  return pos;
}

static size_t append_json_nips_array(char* buffer, size_t pos, size_t capacity, const int* nips, bool add_comma)
{
  if (is_null(nips)) {
    return pos;
  }

  if (add_comma) {
    pos = append_string(buffer, pos, capacity, ",");
  }
  pos = append_string(buffer, pos, capacity, "\"supported_nips\":[");

  bool first = true;
  for (size_t i = 0; nips[i] >= 0; i++) {
    if (!first) {
      pos = append_string(buffer, pos, capacity, ",");
    }
    first = false;

    char num_buf[16];
    itoa(nips[i], num_buf, sizeof(num_buf));
    pos = append_string(buffer, pos, capacity, num_buf);
  }

  pos = append_string(buffer, pos, capacity, "]");

  return pos;
}

bool nostr_nip11_response(const PNostrRelayInfo info, const size_t buffer_capacity, char* buffer)
{
  if (is_null(info) || is_null(buffer) || buffer_capacity == 0) {
    return false;
  }

  websocket_memset(buffer, 0x00, buffer_capacity);

  size_t pos        = 0;
  bool   has_fields = false;

  pos = append_string(buffer, pos, buffer_capacity, "{");

  // Add name field
  if (!is_null(info->name)) {
    pos        = append_json_string_field(buffer, pos, buffer_capacity, "name", info->name, has_fields);
    has_fields = true;
  }

  // Add description field
  if (!is_null(info->description)) {
    pos        = append_json_string_field(buffer, pos, buffer_capacity, "description", info->description, has_fields);
    has_fields = true;
  }

  // Add pubkey field
  if (!is_null(info->pubkey)) {
    pos        = append_json_string_field(buffer, pos, buffer_capacity, "pubkey", info->pubkey, has_fields);
    has_fields = true;
  }

  // Add contact field
  if (!is_null(info->contact)) {
    pos        = append_json_string_field(buffer, pos, buffer_capacity, "contact", info->contact, has_fields);
    has_fields = true;
  }

  // Add supported_nips array
  if (!is_null(info->supported_nips)) {
    pos        = append_json_nips_array(buffer, pos, buffer_capacity, info->supported_nips, has_fields);
    has_fields = true;
  }

  // Add software field
  if (!is_null(info->software)) {
    pos        = append_json_string_field(buffer, pos, buffer_capacity, "software", info->software, has_fields);
    has_fields = true;
  }

  // Add version field
  if (!is_null(info->version)) {
    pos = append_json_string_field(buffer, pos, buffer_capacity, "version", info->version, has_fields);
  }

  pos = append_string(buffer, pos, buffer_capacity, "}");

  buffer[pos] = '\0';

  return pos > 2;  // At least "{}" with some content
}
