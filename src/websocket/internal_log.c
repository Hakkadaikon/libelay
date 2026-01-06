#include "websocket_local.h"

bool websocket_packet_dump(const WebSocketEntity* restrict entity)
{
  require_not_null(entity, false);

  var_debug("fin             : ", entity->fin);
  var_debug("rsv1            : ", entity->rsv1);
  var_debug("rsv2            : ", entity->rsv2);
  var_debug("rsv3            : ", entity->rsv3);
  var_debug("opcode          : ", entity->opcode);
  var_debug("mask            : ", entity->mask);
  var_debug("payload_len     : ", entity->payload_len);
  var_debug("ext_payload_len : ", entity->ext_payload_len);
  str_debug("payload         : ", entity->payload);

  return true;
}

void websocket_epoll_event_dump(const int32_t events)
{
  log_debug("epoll events: ");

  if (events & EPOLLIN) {
    log_debug("EPOLLIN ");
  }

  if (events & EPOLLERR) {
    log_debug("EPOLLIERR ");
  }

  if (events & EPOLLHUP) {
    log_debug("EPOLLHUP ");
  }

  if (events & EPOLLOUT) {
    log_debug("EPOLLOUT ");
  }

  if (events & EPOLLRDHUP) {
    log_debug("EPOLLRDHUP ");
  }

  log_debug("\n");
}
