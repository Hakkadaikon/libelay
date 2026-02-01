#include "../../../util/allocator.h"
#include "../../../util/string.h"
#include "../../websocket_local.h"

static char* select_websocket_client_key(PHTTPRequest restrict request);
static bool  build_handshake_packet(
   const char* restrict accept_key,
   const int32_t accept_key_capacity,
   char* restrict buffer,
   const size_t capacity);
static bool build_nip11_response(
  const char* restrict body,
  const size_t body_len,
  char* restrict buffer,
  const size_t capacity);

/**
 * @brief Handle NIP-11 relay information request
 *
 * @param[in]  client_sock Client socket
 * @param[in]  buffer      Raw buffer for request/response
 * @param[in]  request     Parsed HTTP request
 * @param[in]  callbacks   User callbacks (handshake_callback for NIP-11)
 *
 * @return true if NIP-11 request was handled, false otherwise
 */
static bool handle_nip11_request(
  const int32_t             client_sock,
  const WebSocketRawBuffer* buffer,
  PHTTPRequest restrict request,
  const WebSocketCallbacks* callbacks)
{
  if (is_null(callbacks) || is_null(callbacks->handshake_callback)) {
    return false;
  }

  if (!is_nip11_request(request->headers, request->header_size)) {
    return false;
  }

  log_debug("NIP-11 request detected\n");

  // Use half of buffer for body, half for full response
  size_t body_capacity     = buffer->capacity / 2;
  char*  body_buffer       = buffer->response;
  char*  response          = buffer->response + body_capacity;
  size_t response_capacity = buffer->capacity - body_capacity;

  websocket_memset_s(body_buffer, body_capacity, 0x00, body_capacity);
  websocket_memset_s(response, response_capacity, 0x00, response_capacity);

  if (!callbacks->handshake_callback(request, body_capacity, body_buffer)) {
    log_debug("Handshake callback returned false, not NIP-11\n");
    return false;
  }

  size_t body_len = strnlen(body_buffer, body_capacity);
  if (body_len == 0) {
    log_error("NIP-11 callback returned empty body\n");
    return false;
  }

  if (!build_nip11_response(body_buffer, body_len, response, response_capacity)) {
    log_error("Failed to build NIP-11 response\n");
    return false;
  }

  size_t response_len = strnlen(response, response_capacity);
  if (websocket_send(client_sock, response_len, response) != WEBSOCKET_ERRORCODE_NONE) {
    log_error("Failed to send NIP-11 response\n");
    return false;
  }

  log_debug("NIP-11 response sent successfully\n");
  str_debug("NIP-11 response: ", response);

  return true;
}

HandshakeResult client_handshake(
  const int32_t             client_sock,
  const WebSocketRawBuffer* buffer,
  PHTTPRequest restrict request,
  const WebSocketCallbacks* callbacks)
{
  HandshakeResult result = HANDSHAKE_RESULT_ERROR;

  // Check for NIP-11 request first
  if (handle_nip11_request(client_sock, buffer, request, callbacks)) {
    // NIP-11 request handled, close connection
    return HANDSHAKE_RESULT_NIP11;
  }

  if (!is_valid_request(request)) {
    goto FINALIZE;
  }

  char* client_key = select_websocket_client_key(request);
  if (is_null(client_key)) {
    goto FINALIZE;
  }

  char accept_key[HTTP_HEADER_VALUE_CAPACITY];
  if (!generate_websocket_acceptkey(client_key, sizeof(accept_key), accept_key)) {
    log_error("Invalid WebSocket handshake request. Failed generate accept key\n");
    goto FINALIZE;
  }

  if (!build_handshake_packet(accept_key, sizeof(accept_key), buffer->response, buffer->capacity)) {
    goto FINALIZE;
  }

  size_t response_len = strnlen(buffer->response, buffer->capacity);
  if (response_len == 0) {
    goto FINALIZE;
  }

  if (websocket_send(client_sock, response_len, buffer->response) != WEBSOCKET_ERRORCODE_NONE) {
    log_error("Failed to send OK frame.");
    goto FINALIZE;
  }

  log_debug("handshake success !");
  str_debug("request : ", buffer->request);
  str_debug("response : ", buffer->response);

  result = HANDSHAKE_RESULT_WEBSOCKET;

FINALIZE:
  // Wipe variables
  websocket_memset_s(accept_key, sizeof(accept_key), 0x00, sizeof(accept_key));

  if (result == HANDSHAKE_RESULT_ERROR) {
    str_info("Invalid handshake request : ", buffer->request);
  }

  return result;
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

/**
 * @brief Build NIP-11 HTTP response with proper headers
 *
 * @param[in]  body      JSON body content
 * @param[in]  body_len  Length of JSON body
 * @param[out] buffer    Output buffer for full HTTP response
 * @param[in]  capacity  Buffer capacity
 *
 * @return true on success, false if buffer too small
 */
static bool build_nip11_response(
  const char* restrict body,
  const size_t body_len,
  char* restrict buffer,
  const size_t capacity)
{
  const char NIP11_HEADER[] =
    "HTTP/1.1 200 OK\r\n"
    "Content-Type: application/nostr+json\r\n"
    "Access-Control-Allow-Origin: *\r\n"
    "Access-Control-Allow-Headers: *\r\n"
    "Access-Control-Allow-Methods: GET, OPTIONS\r\n"
    "\r\n";
  const size_t NIP11_HEADER_LEN  = sizeof(NIP11_HEADER) - 1;
  const size_t REQUIRED_CAPACITY = NIP11_HEADER_LEN + body_len + 1;

  if (capacity <= REQUIRED_CAPACITY) {
    log_error("NIP-11 response buffer too small\n");
    return false;
  }

  char* ptr = buffer;
  websocket_memcpy(ptr, NIP11_HEADER, NIP11_HEADER_LEN);
  ptr += NIP11_HEADER_LEN;

  websocket_memcpy(ptr, body, body_len);
  ptr += body_len;

  *ptr = '\0';

  return true;
}
