#ifndef NOSTR_SERVER_LOOP_RECEIVE_HANDLE_H_
#define NOSTR_SERVER_LOOP_RECEIVE_HANDLE_H_

#include "../../../util/allocator.h"
#include "../../websocket_local.h"
#include "opcode_handle.h"

static inline int32_t receive_handle(
  const int32_t             client_sock,
  const size_t              read_size,
  WebSocketRawBuffer*       buffer,
  const WebSocketCallbacks* callbacks)
{
  require_valid_length(client_sock, WEBSOCKET_ERRORCODE_FATAL_ERROR);
  require_valid_length(read_size, WEBSOCKET_ERRORCODE_FATAL_ERROR);
  require_not_null(buffer, WEBSOCKET_ERRORCODE_FATAL_ERROR);
  require_valid_length(buffer->capacity, WEBSOCKET_ERRORCODE_FATAL_ERROR);
  require_not_null(buffer->request, WEBSOCKET_ERRORCODE_FATAL_ERROR);
  require_not_null(buffer->response, WEBSOCKET_ERRORCODE_FATAL_ERROR);

  WebSocketEntity entity;
  char*           payload_buf = (char*)websocket_alloc(read_size);
  int32_t         rtn         = 0;
  size_t          offset      = 0;

  while (offset < read_size) {
    websocket_memset(&entity, 0x00, sizeof(entity));
    entity.payload = payload_buf;

    size_t consumed = to_websocket_entity_consumed(
      buffer->request + offset, read_size - offset, &entity);

    if (consumed == 0) {
      break;
    }

    websocket_packet_dump(&entity);

    rtn = opcode_handle(client_sock, buffer, callbacks, &entity);
    if (rtn != WEBSOCKET_ERRORCODE_NONE) {
      goto FINALIZE;
    }

    if (is_rise_signal()) {
      var_info("rise signal. sock : ", client_sock);
      rtn = WEBSOCKET_ERRORCODE_FATAL_ERROR;
      goto FINALIZE;
    }

    offset += consumed;
  }

FINALIZE:
  websocket_free(payload_buf);
  return rtn;
}

#endif