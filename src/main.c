#include "util/allocator.h"
#include "util/log.h"
#include "websocket/websocket.h"

bool websocket_callback_echoback(
  const int              client_sock,
  const WebSocketEntity* entity,
  const size_t           buffer_capacity,
  char*                  response_buffer)
{
  WebSocketEntity response_entity;

  websocket_memcpy(&response_entity, entity, sizeof(WebSocketEntity));

  switch (entity->opcode) {
    case WEBSOCKET_OP_CODE_TEXT: {
      response_entity.mask = 0;
      size_t packet_size   = to_websocket_packet(&response_entity, buffer_capacity, response_buffer);
      if (packet_size == 0) {
        log_error("Failed to create websocket packet.\n");
        return false;
      }

      websocket_send(client_sock, packet_size, response_buffer);
    } break;
    default:
      break;
  }

  return true;
}

bool websocket_receive_callback(
  const int              client_sock,
  const WebSocketEntity* entity,
  const size_t           buffer_capacity,
  char*                  response_buffer)
{
  return websocket_callback_echoback(client_sock, entity, buffer_capacity, response_buffer);
}

void websocket_connect_callback(int client_sock)
{
  log_info("[user] hello connect\n");
}

void websocket_disconnect_callback(int client_sock)
{
  log_info("[user] bye\n");
}

int main()
{
  WebSocketInitArgs init_args;
  init_args.port_num = 8080;
  init_args.backlog  = 5;

  int server_sock = websocket_server_init(&init_args);
  if (server_sock < WEBSOCKET_ERRORCODE_NONE) {
    log_error("websocket server init error.\n");
    var_error("server_sock: ", server_sock);
    return 1;
  }

  WebSocketLoopArgs loop_args;
  loop_args.server_sock                   = server_sock;
  loop_args.callbacks.receive_callback    = websocket_receive_callback;
  loop_args.callbacks.connect_callback    = websocket_connect_callback;
  loop_args.callbacks.disconnect_callback = websocket_disconnect_callback;
  loop_args.buffer_capacity               = 1024;

  websocket_server_loop(&loop_args);
  websocket_close(server_sock);

  log_error("websocket server end.\n");
  return 0;
}
