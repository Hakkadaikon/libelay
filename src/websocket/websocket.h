#ifndef _NOSTR_WEBSOCKET_H_
#define _NOSTR_WEBSOCKET_H_

/** @file  websocket.h
 *
 * @brief Parses each parameter of a websocket packet stored in network byte order.
 * @see RFC6455 (https://datatracker.ietf.org/doc/html/rfc6455)
 */
#include "../util/types.h"

/**
 * @enum WebSocketOpCode
 * @brief websocket packet type
 */
typedef enum {
  WEBSOCKET_OP_CODE_TEXT   = 0x1,  ///< When this frame is received, Invokes the user callback.
  WEBSOCKET_OP_CODE_BINARY = 0x2,  ///< When this frame is received, Invokes the user callback.
  WEBSOCKET_OP_CODE_CLOSE  = 0x8,  ///< When this frame is received, closes the client socket.
  WEBSOCKET_OP_CODE_PING   = 0x9,  ///< When this frame is received, this server replies with a Pong.
  WEBSOCKET_OP_CODE_PONG   = 0xA,  ///< Pong frame.
} WebSocketOpCode;

/**
 * @brief Error code when calling a WebSocket function
 */
typedef enum {
  WEBSOCKET_ERRORCODE_FATAL_ERROR        = -3,  ///< When this code is received, This server will terminate processing.
  WEBSOCKET_ERRORCODE_SOCKET_CLOSE_ERROR = -2,  ///< When this code is received, This server closes the client socket.
  WEBSOCKET_ERRORCODE_CONTINUABLE_ERROR  = -1,  ///< When this code is received, This server will not proceed to the next epoll loop and will continue.
  WEBSOCKET_ERRORCODE_NONE               = 0,   ///< Normal code. No action.
} WebSocketErrorCode;

/**
 * @brief Result of parsing the websocket packet
 */
typedef struct _WebSocketEntity {
  uint8_t         fin;
  uint8_t         rsv1;
  uint8_t         rsv2;
  uint8_t         rsv3;
  WebSocketOpCode opcode;
  uint8_t         mask;
  uint8_t         payload_len;
  uint8_t         dummy1[6];
  uint64_t        ext_payload_len;
  uint8_t         masking_key[4];
  char*           payload;
  uint8_t         dummy2[3];
} WebSocketEntity;

/**
 * @brief User callback that is called when a websocket packet is received.
 */
typedef bool (*PWebSocketReceiveCallback)(
  const int32_t          client_sock,      ///< @param[in]     client_sock     Client socket that sent the data
  const WebSocketEntity* entity,           ///< @param[in]     enttity         Parsed websocket packet
  const size_t           buffer_capacity,  ///< @param[in]     buffer_capacity Response_buffer capacity.
  char*                  response_buffer   ///< @param[in/out] response_buffer This buffer must be used to create the return packet.
);

/**
 * @brief User callback to be called when connection is established
 */
typedef void (*PWebSocketConnectCallback)(
  const int32_t client_sock  ///< @param[in] client_sock Client socket to connect
);

/**
 * @ brief User callback that is called when a client is disconnected.
 *
 * When this callback is called, the connection has already been disconnected,
 * so BSD socket functions cannot be called using the client_sock argument.
 * It is passed to identify the client and release resources if they have been allocated.
 */
typedef void (*PWebSocketDisconnectCallback)(
  const int32_t client_sock  ///< @param[in] client_sock Client socket to disconnect
);

/**
 * @brief User callback list to pass to the WebSocket library.
 */
typedef struct {
  PWebSocketReceiveCallback    receive_callback;     ///< @see PWebSocketReceiveCallback
  PWebSocketConnectCallback    connect_callback;     ///< @see PWebSocketConnectCallback
  PWebSocketDisconnectCallback disconnect_callback;  ///< @see PWebSocketDisconnectCallback
} WebSocketCallbacks, *PWebSocketCallbacks;

/**
 * @brief Arguments to pass to websocket_init()
 */
typedef struct {
  int32_t port_num;  ///< WebSocket port number
  int32_t backlog;   ///< Listen queue size
} WebSocketInitArgs, *PWebSocketInitArgs;

/**
 * @brief Arguments to pass to websocket_loop()
 */
typedef struct {
  int32_t            server_sock;      ///< Socket descriptor obtained by websocket_server_init() function
  int32_t            dummy;            ///< dummy
  size_t             buffer_capacity;  ///< Capacity of the send and receive buffer for one client.
  WebSocketCallbacks callbacks;        ///< @see WebSocketCallBacks
} WebSocketLoopArgs, *PWebSocketLoopArgs;

/**
 * @brief Parse raw data in network byte order into a websocket flame structure
 *
 * @param[in]  raw         raw data (network byte order)
 * @param[in]  packet_size Size of webSocket packet
 * @param[out] entity      Output destination of parsed packet
 *
 * @return true: Parse was successful / false: Failed parse
 */
bool to_websocket_entity(const char* raw, const size_t packet_size, WebSocketEntity* entity);

/**
 * @brief Creates raw data to send back to the client
 *
 * @param[in]  entity   Transmitted frame before it becomes raw data
 * @param[in]  capacity Raw data capacity
 * @param[out] raw      Raw data packet to be returned
 *
 * @return Size of raw data. If parsing fails, 0 is returned.
 */
size_t to_websocket_packet(const WebSocketEntity* entity, const size_t capacity, char* raw);

/**
 * @brief Initialize a WebSocket server. socket listen and register signal handler.
 *
 * @param[in] port_num Listening port number
 * @param[in] backlog  Listen queue size
 *
 * @return Positive value: Server socket descriptor / Negative value: WebSocket error code
 * @see WebSocketErrorCode
 */
int32_t websocket_server_init(const PWebSocketInitArgs args);

/**
 * @brief Wrapper for the BSD socket send() API.
 *
 * @param[in] sock_fd     Destination socket descriptor
 * @param[in] buffer_size Buffer size
 * @param[in] buffer      Buffer that stores the transmission data
 *
 * @return WebSocket error code
 * @see WebSocketErrorCode
 */
int32_t websocket_send(const int32_t sock_fd, const size_t buffer_size, const char* buffer);

/**
 * @brief Wrapper for the BSD socket recv() API.
 *
 * @param[in]  sock_fd  Destination socket descriptor
 * @param[in]  capacity Receive buffer capacity
 * @param[out] buffer   Receive buffer. User must allocate the amount specified by the capacity variable.
 *
 * @return Positive value: Receive data size / Negative value: WebSocket error code
 * @see WebSocketErrorCode
 */
ssize_t websocket_recv(const int32_t sock_fd, const size_t capacity, char* buffer);

/**
 * @brief Wrapper for the BSD socket close() API
 *
 * @param[in] sock_fd Socket descriptor
 *
 * @return WebSocket error code
 * @see WebSocketErrorCode
 */
int32_t websocket_close(const int32_t sock_fd);

/**
 * @brief Enter the receive loop from the client.
 *        This function will block until it is sent a SIGHUP/SIGINT/SIGTERM signal or detects a FATAL ERROR internally.
 *
 * @param[in] args @see WebSocketLoopArgs
 *
 * @return true: success / false: failure
 */
bool websocket_server_loop(PWebSocketLoopArgs args);

#endif
