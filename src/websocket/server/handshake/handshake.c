#include "../../../util/allocator.h"
#include "../../../util/string.h"
#include "../../websocket_local.h"

static char* select_websocket_client_key(PHTTPRequest restrict request);
static bool  build_handshake_packet(
   const char* restrict accept_key,
   const int32_t accept_key_capacity,
   char* restrict buffer,
   const size_t capacity);

bool client_handshake(
  const int32_t       client_sock,
  PWebSocketRawBuffer buffer,
  PHTTPRequest restrict request)
{
  bool has_error = false;

  if (!is_valid_request(request)) {
    has_error = true;
    goto FINALIZE;
  }

  char* client_key = select_websocket_client_key(request);
  if (is_null(client_key)) {
    has_error = true;
    goto FINALIZE;
  }

  char accept_key[HTTP_HEADER_VALUE_CAPACITY];
  if (!generate_websocket_acceptkey(client_key, sizeof(accept_key), accept_key)) {
    has_error = true;
    goto FINALIZE;
  }

  if (has_error) {
    str_info("Invalid handshake request : ", buffer->request);
  } else {
    if (!build_handshake_packet(accept_key, sizeof(accept_key), buffer->response, buffer->capacity)) {
      has_error = true;
      goto FINALIZE;
    }

    size_t response_len = strnlen(buffer->response, buffer->capacity);
    if (response_len == 0) {
      has_error = true;
      goto FINALIZE;
    }

    if (websocket_send(client_sock, response_len, buffer->response) != WEBSOCKET_ERRORCODE_NONE) {
      log_error("Failed to send OK frame.");
      has_error = true;
      goto FINALIZE;
    }

    log_debug("handshake success !");
    str_debug("request : ", buffer->request);
    str_debug("response : ", buffer->response);
  }

FINALIZE:
  // Wipe variables
  websocket_memset_s(accept_key, sizeof(accept_key), 0x00, sizeof(accept_key));

  return !has_error;
}

static char* select_websocket_client_key(PHTTPRequest restrict request)
{
  for (size_t i = 0; i < request->header_size; i++) {
    PHTTPRequestHeaderLine line = &request->headers[i];
    if (strncmp_sensitive(line->key, "Sec-WebSocket-Key", sizeof(line->key), 17, false)) {
      return line->value;
    }
  }

  log_error("WebSocket client key is not found.\n");
  return NULL;
}

static bool build_handshake_packet(
  const char* restrict accept_key,
  const int32_t accept_key_capacity,
  char* restrict buffer,
  const size_t capacity)
{
  const char OK_MESSAGE[] =
    "HTTP/1.1 101 Switching Protocols\r\n"
    "Upgrade: websocket\r\n"
    "Connection: Upgrade\r\n"
    "Sec-WebSocket-Accept: ";
  const size_t OK_MESSAGE_LEN    = sizeof(OK_MESSAGE) - 1;
  const size_t ACCEPT_KEY_LEN    = strnlen(accept_key, accept_key_capacity);
  const size_t REQUIRED_CAPACITY = OK_MESSAGE_LEN + ACCEPT_KEY_LEN + 5;

  if (capacity <= REQUIRED_CAPACITY) {
    return false;
  }

  char* ptr = buffer;
  websocket_memcpy(ptr, OK_MESSAGE, OK_MESSAGE_LEN);
  ptr += OK_MESSAGE_LEN;

  websocket_memcpy(ptr, accept_key, ACCEPT_KEY_LEN);
  ptr += ACCEPT_KEY_LEN;

  websocket_memcpy(ptr, "\r\n\r\n", 4);
  ptr += 4;

  return true;
}
