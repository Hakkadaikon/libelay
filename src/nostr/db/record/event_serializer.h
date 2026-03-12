#ifndef NOSTR_DB_EVENT_SERIALIZER_H_
#define NOSTR_DB_EVENT_SERIALIZER_H_

#include "../../../util/types.h"
#include "../../nostr_types.h"
#include "../db_types.h"
#include "record_types.h"

/**
 * @brief Serialize a NostrEventEntity into an EventRecord byte buffer
 *
 * Output format: [EventRecord header (152 bytes)][content][tags]
 *
 * @param event Source event entity (hex-encoded fields)
 * @param buffer Output buffer
 * @param capacity Buffer capacity
 * @return Bytes written on success, -1 on failure
 */
int32_t event_serialize(const NostrEventEntity* event, uint8_t* buffer,
                        uint16_t capacity);

/**
 * @brief Deserialize an EventRecord byte buffer into a NostrEventEntity
 * @param buffer Input buffer (EventRecord format)
 * @param length Buffer length
 * @param event Output event entity (hex-encoded fields)
 * @return NOSTR_DB_OK on success, error code on failure
 */
NostrDBError event_deserialize(const uint8_t* buffer, uint16_t length,
                               NostrEventEntity* event);

#endif
