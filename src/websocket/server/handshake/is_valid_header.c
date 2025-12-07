#include "../../../crypto/base64.h"
#include "../../../util/string.h"
#include "../../websocket_local.h"

#define IS_VALID_KEY(value, expected) \
  strncmp_sensitive(value, expected, HTTP_HEADER_KEY_CAPACITY, sizeof(expected), false)

static bool is_valid_request_header_line(PHTTPRequestHeaderLine restrict line);
static bool is_valid_host(const char* restrict host);
static bool is_valid_upgrade(const char* restrict value);
static bool is_valid_connection(const char* restrict value);
static bool is_valid_websocket_key(const char* restrict value);
static bool is_valid_version(const char* restrict value);

bool is_valid_request_header(PHTTPRequestHeaderLine restrict headers, size_t header_size)
{
  for (size_t i = 0; i < header_size; i++) {
    if (!is_valid_request_header_line(&headers[i])) {
      return false;
    }
  }

  return true;
}

static bool is_valid_request_header_line(PHTTPRequestHeaderLine restrict line)
{
  if (IS_VALID_KEY(line->key, "host")) {
    if (is_valid_host(line->value)) {
      return true;
    }
    return false;
  } else if (IS_VALID_KEY(line->key, "upgrade")) {
    if (is_valid_upgrade(line->value)) {
      return true;
    }
    return false;
  } else if (IS_VALID_KEY(line->key, "connection")) {
    if (is_valid_connection(line->value)) {
      return true;
    }
    return false;
  } else if (IS_VALID_KEY(line->key, "sec-webSocket-key")) {
    if (is_valid_websocket_key(line->value)) {
      return true;
    }
    return false;
  } else if (IS_VALID_KEY(line->key, "sec-websocket-version")) {
    if (is_valid_version(line->value)) {
      return true;
    }
    return false;
  }

  return true;
}

static bool is_valid_host(const char* restrict host)
{
  return true;
}

static bool is_valid_upgrade(const char* restrict value)
{
  if (!IS_VALID_KEY(value, "websocket")) {
    str_error("Invalid websocket request header [Key: Upgrade] : ", value);
    return false;
  }

  return true;
}

static bool is_valid_connection(const char* restrict value)
{
  if (!IS_VALID_KEY(value, "upgrade")) {
    str_error("Invalid websocket request header [Key: Connection] : ", value);
    return false;
  }

  return true;
}

static bool is_valid_version(const char* restrict value)
{
  if (!IS_VALID_KEY(value, "13")) {
    str_error("Invalid websocket request header [Key: Sec-WebSocket-Version] : ", value);
    return false;
  }

  return true;
}

static bool is_valid_websocket_key(const char* restrict value)
{
  if (strnlen(value, HTTP_HEADER_VALUE_CAPACITY) < 16) {
    log_error("Invalid websocket request header [Key: Sec-WebSocket-Key] Length is less than 16.\n");
    return false;
  }

  if (!is_base64(value)) {
    log_error("Invalid websocket request header [Key: Sec-WebSocket-Key] Client key is not base64.\n");
    return false;
  }

  return true;
}
