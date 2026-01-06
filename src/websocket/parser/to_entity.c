/**
 * @file  to_entity.c
 *
 * @brief Parses each parameter of a websocket packet stored in network byte order.
 * @see RFC6455 (https://datatracker.ietf.org/doc/html/rfc6455)
 */

#include "../../util/allocator.h"
#include "../websocket_local.h"

/**
 * @brief Parse raw data in network byte order into a websocket packet structure
 *
 * @param[in]  raw          Raw data (network byte order)
 * @param[in]  packet_size  Capacity of raw data
 * @param[out] entity       Output destination of parsed packet
 *
 * @return true: Parse was successful / false: Failed parse
 */
bool to_websocket_entity(const char* restrict raw, const size_t capacity, WebSocketEntity* restrict entity)
{
  require_not_null(raw, false);
  require_not_null(entity, false);
  require_not_null(entity->payload, false);
  require(capacity >= 2, false);

  //  0                   1                   2                   3
  //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  // +-+-+-+-+-------+-+-------------+-------------------------------+
  // |F|R|R|R| opcode|M| Payload len |    Extended payload length    |
  // |I|S|S|S|  (4)  |A|     (7)     |             (16/64)           |
  // |N|V|V|V|       |S|             |   (if payload len==126/127)   |
  // | |1|2|3|       |K|             |                               |
  // +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
  // |     Extended payload length continued, if payload len == 127  |
  // + - - - - - - - - - - - - - - - +-------------------------------+
  // |                               |Masking-key, if MASK set to 1  |
  // +-------------------------------+-------------------------------+
  // | Masking-key (continued)       |          Payload Data         |
  // +-------------------------------- - - - - - - - - - - - - - - - +
  // :                     Payload Data continued ...                :
  // + - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - +
  // |                     Payload Data continued ...                |
  // +---------------------------------------------------------------+
  // 0-7bit
  // +-+-+-+-+-------+
  // |F|R|R|R| opcode|
  // |I|S|S|S|  (4)  |
  // |N|V|V|V|       |
  // | |1|2|3|       |
  // +-+-+-+-+-------+
  entity->fin    = (raw[0] & 0x80) >> 7;
  entity->rsv1   = (raw[0] & 0x40) >> 6;
  entity->rsv2   = (raw[0] & 0x20) >> 5;
  entity->rsv3   = (raw[0] & 0x10) >> 4;
  entity->opcode = (raw[0] & 0x0F);

  // 8-15bit
  // +-+-------------+
  // |M| Payload len |
  // |A|     (7)     |
  // |S|             |
  // |K|             |
  // +-+-------------+
  entity->mask        = (raw[1] & 0x80) >> 7;
  entity->payload_len = (raw[1] & 0x7F);

  size_t packet_offset = 2;

  // Expanded payload length
  // - nothing  (if payload_len <= 125)
  // - 16-31bit (if payload_len = 126)
  // - 16-79bit (if payload_len = 127)
  //
  //  0                   1                   2                   3
  //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  //                                 +-------------------------------+
  //                                 |    Extended payload length    |
  //                                 |             (16/64)           |
  //                                 |   (if payload len==126/127)   |
  //                                 |                               |
  // +-+-+-+-+-------+-+-------------+ - - - - - - - - - - - - - - - +
  // |     Extended payload length continued, if payload len == 127  |
  // + - - - - - - - - - - - - - - - +-------------------------------+
  // |                               |
  // +-------------------------------+
  if (entity->payload_len == 126) {
    require_valid_length(capacity - 4, false);
    entity->ext_payload_len = (raw[2] << 8) | raw[3];
    require(entity->ext_payload_len <= capacity, false);
    packet_offset += 2;
  } else if (entity->payload_len == 127) {
    require(capacity >= 10, false);

    for (int32_t i = 0; i < 8; i++) {
      entity->ext_payload_len = (entity->ext_payload_len << 8) | raw[2 + i];
    }
    packet_offset += 8;
  } else {
    entity->ext_payload_len = entity->payload_len;
  }

  // masking key
  // - 16-47bit  (if payload_len <= 125)
  // - 32-63bit  (if payload_len = 126)
  // - 80-111bit (if payload_len = 127)
  //
  //  0                   1                   2                   3
  //  0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
  // + - - - - - - - - - - - - - - - +-------------------------------+
  // |                               |Masking-key, if MASK set to 1  |
  // +-------------------------------+-------------------------------+
  // | Masking-key (continued)       |
  // +--------------------------------
  if (entity->mask) {
    require(capacity >= packet_offset + 4, false);
    websocket_memcpy(entity->masking_key, &raw[packet_offset], 4);
    packet_offset += sizeof(entity->masking_key);
  }

  require(entity->ext_payload_len <= (capacity - packet_offset), false);

  const char* payload_raw = &raw[packet_offset];
  for (size_t i = 0; i < entity->ext_payload_len; i++) {
    entity->payload[i] =
      payload_raw[i] ^ (entity->mask ? entity->masking_key[i % 4] : 0);
  }
  entity->payload[entity->ext_payload_len] = '\0';
  return true;
}
