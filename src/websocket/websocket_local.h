#ifndef _NOSTR_WEBSOCKET_LOCAL_H_
#define _NOSTR_WEBSOCKET_LOCAL_H_

/**
 * @file  websocket_local.h
 *
 * @brief Parses each parameter of a websocket packet stored in network byte order.
 * @see RFC6455 (https://datatracker.ietf.org/doc/html/rfc6455)
 */

#include "../arch/linux/epoll.h"
#include "../http/http.h"
#include "../util/log.h"
#include "../util/signal.h"
#include "../util/string.h"
#include "websocket.h"

typedef struct {
  int32_t              epoll_fd;
  uint8_t              dummy[4];
  WebSocketEpollEvent* event;
} WebSocketEpollLoopArgs;

typedef struct {
  size_t  capacity;
  char*   request;
  char*   response;
  uint8_t dummy[6];
} WebSocketRawBuffer;

typedef enum {
  WEBSOCKET_SYSCALL_ERROR = -1
} WebSocketSysCallErrorCode;

/*----------------------------------------------------------------------------*/
/* websocket/server/handshake                                                 */
/*----------------------------------------------------------------------------*/

/**
 * @brief Handshake result codes
 */
typedef enum {
  HANDSHAKE_RESULT_WEBSOCKET = 1,  ///< WebSocket upgrade successful
  HANDSHAKE_RESULT_NIP11     = 0,  ///< NIP-11 request handled (close connection)
  HANDSHAKE_RESULT_ERROR     = -1  ///< Error occurred
} HandshakeResult;

bool            is_valid_request(PHTTPRequest request);
bool            is_valid_request_header(PHTTPRequestHeaderLine headers, size_t header_size);
bool            is_valid_request_line(PHTTPRequestLine line);
bool            is_nip11_request(PHTTPRequestHeaderLine headers, size_t header_size);
HandshakeResult client_handshake(
  const int32_t             client_sock,
  const WebSocketRawBuffer* buffer,
  PHTTPRequest              request,
  const WebSocketCallbacks* callbacks);

/*----------------------------------------------------------------------------*/
/* websocket/crypto.c                                                         */
/*----------------------------------------------------------------------------*/

bool generate_websocket_acceptkey(const char* client_key, const size_t accept_key_size, char* accept_key);

/*----------------------------------------------------------------------------*/
/* websocket/socket/accept.c                                                  */
/*----------------------------------------------------------------------------*/

int32_t websocket_accept(const int32_t sock_fd);

/*----------------------------------------------------------------------------*/
/* websocket/socket/listen.c                                                  */
/*----------------------------------------------------------------------------*/

int32_t websocket_listen(const int32_t port_num, const int32_t backlog);

/*----------------------------------------------------------------------------*/
/* websocket/socket/epoll.c or websocket/socket/kqueue.c                      */
/*----------------------------------------------------------------------------*/

bool    websocket_epoll_add(const int32_t epoll_fd, const int32_t sock_fd, WebSocketEpollEvent* event);
bool    websocket_epoll_del(const int32_t epoll_fd, const int32_t sock_fd);
int32_t websocket_epoll_create();
int32_t websocket_epoll_wait(const int32_t epoll_fd, WebSocketEpollEvent* events, const int32_t max_events);
int32_t websocket_epoll_getfd(const WebSocketEpollEvent* event);
int32_t websocket_epoll_rise_error(const WebSocketEpollEvent* event);
int32_t websocket_epoll_rise_input(const WebSocketEpollEvent* event);

/*----------------------------------------------------------------------------*/
/* websocket/internal_log.c                                                   */
/*----------------------------------------------------------------------------*/

bool websocket_packet_dump(const WebSocketEntity* entity);
void websocket_epoll_event_dump(const int32_t events);

#endif