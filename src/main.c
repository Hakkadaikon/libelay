#include "nostr/nostr_func.h"
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

/**
 * @brief NIP-11 handshake callback
 *
 * Called during HTTP handshake phase to handle NIP-11 relay information requests.
 * When a client sends Accept: application/nostr+json, this returns relay metadata.
 */
bool websocket_handshake_callback(
  const PHTTPRequest request,
  const size_t       buffer_capacity,
  char*              response_buffer)
{
  // Relay information for NIP-11
  static const int supported_nips[] = {1, 11, -1};  // -1 terminates the array

  NostrRelayInfo info;
  info.name           = "libelay";
  info.description    = "A high-performance Nostr relay without libc";
  info.pubkey         = NULL;  // Optional: admin pubkey
  info.contact        = NULL;  // Optional: contact URI
  info.software       = "https://github.com/hakkadaikon/libelay";
  info.version        = "0.1.0";
  info.supported_nips = supported_nips;

  if (!nostr_nip11_response(&info, buffer_capacity, response_buffer)) {
    log_error("Failed to generate NIP-11 response\n");
    return false;
  }

  log_info("[NIP-11] Relay information requested\n");
  return true;
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
  loop_args.callbacks.handshake_callback  = websocket_handshake_callback;
  loop_args.buffer_capacity               = 1024;

  websocket_server_loop(&loop_args);
  websocket_close(server_sock);

  log_error("websocket server end.\n");
  return 0;
}
