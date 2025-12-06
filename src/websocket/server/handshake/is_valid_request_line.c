#include "../../../util/string.h"
#include "../../websocket_local.h"

#define IS_VALID_HTTP_VERSION(value, expected) \
  is_compare_str(value, expected, HTTP_VERSION_CAPACITY, sizeof(expected), false)
#define IS_VALID_HTTP_METHOD(value, expected) \
  is_compare_str(value, expected, HTTP_METHOD_CAPACITY, sizeof(expected), false)

static bool is_valid_method(char* value);
static bool is_valid_target(char* value);
static bool is_valid_http_version(char* value);

bool is_valid_request_line(PHTTPRequestLine restrict line)
{
  if (!is_valid_method(line->method)) {
    return false;
  }

  if (!is_valid_target(line->target)) {
    return false;
  }

  if (!is_valid_http_version(line->http_version)) {
    return false;
  }

  return true;
}

static bool is_valid_method(char* value)
{
  if (!IS_VALID_HTTP_METHOD(value, "get")) {
    log_error("Invalid websocket request line: method is not GET\n");
    return false;
  }

  return true;
}

static bool is_valid_target(char* value)
{
  if (get_str_nlen(value, HTTP_TARGET_CAPACITY) <= 0) {
    log_error("Invalid websocket request line: target size is 0\n");
    return false;
  }

  return true;
}

static bool is_valid_http_version(char* value)
{
  bool has_error = false;

  if (!IS_VALID_HTTP_VERSION(value, "http/1.1") &&
      !IS_VALID_HTTP_VERSION(value, "http/2.0") &&
      !IS_VALID_HTTP_VERSION(value, "http/3.0")) {
    log_error("Invalid websocket request line: Invalid HTTP version(Not 1.1/2.0/3.0)\n");
    return false;
  }

  return true;
}
