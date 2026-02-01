#ifndef NOSTR_SERVER_LOOP_ACCEPT_H_
#define NOSTR_SERVER_LOOP_ACCEPT_H_

#include "../../../util/allocator.h"
#include "../../websocket_local.h"

static inline bool accept_handle(
  const int32_t             epoll_fd,
  const int32_t             server_sock,
  const WebSocketRawBuffer* buffer,
  WebSocketEpollEvent*      event,
  const WebSocketCallbacks* callbacks)
{
  require_valid_length(epoll_fd, false);
  require_valid_length(server_sock, false);
  require_not_null(buffer, false);
  require_not_null(buffer->request, false);
  require_not_null(buffer->response, false);
  require_valid_length(buffer->capacity, false);

  HTTPRequest request;
  ssize_t     bytes_read;

  bool err = false;

  log_debug("accept...\n");
  int32_t client_sock = websocket_accept(server_sock);
  if (client_sock < 0) {
    if (client_sock == WEBSOCKET_ERRORCODE_FATAL_ERROR) {
      err = true;
    }

    goto FINALIZE;
  }

  log_debug("epoll add(client sock)...");
  if (!websocket_epoll_add(epoll_fd, client_sock, event)) {
    err = true;
    goto FINALIZE;
  }

  log_debug("Read to message...");
  while (1) {
    bytes_read = websocket_recv(client_sock, buffer->capacity, buffer->request);
    if (bytes_read < 0) {
      if (bytes_read == WEBSOCKET_ERRORCODE_CONTINUABLE_ERROR) {
        continue;
      }

      err = true;
      var_info("Failed to message read.\n", client_sock);
      websocket_epoll_del(epoll_fd, client_sock);
      goto FINALIZE;
    }

    break;
  }

  log_debug("Analyze to message...\n");
  if (!extract_http_request(buffer->request, bytes_read, &request)) {
    err = true;
    goto FINALIZE;
  }

  HandshakeResult handshake_result = client_handshake(client_sock, buffer, &request, callbacks);
  if (handshake_result == HANDSHAKE_RESULT_ERROR) {
    websocket_epoll_del(epoll_fd, client_sock);
    err = true;
    goto FINALIZE;
  }

  if (handshake_result == HANDSHAKE_RESULT_NIP11) {
    // NIP-11 request was handled, close connection (not an error)
    websocket_epoll_del(epoll_fd, client_sock);
    websocket_close(client_sock);
    log_debug("NIP-11 request completed, connection closed\n");
    return true;
  }

FINALIZE:
  if (err) {
    log_debug("websocket_accept error. finalize...\n");
    if (client_sock >= 0) {
      websocket_close(client_sock);
    }

    return false;
  }

  if (!is_null(callbacks->connect_callback)) {
    callbacks->connect_callback(client_sock);
  }

  var_debug("accept done. client_sock : ", client_sock);
  return true;
}

#endif
