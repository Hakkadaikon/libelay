#include "../../arch/memory.h"
#include "../../util/string.h"
#include "db.h"
#include "db_internal.h"
#include "record/event_serializer.h"
#include "record/record_manager.h"

// ============================================================================
// nostr_db_write_event
// ============================================================================
NostrDBError nostr_db_write_event(NostrDB* db, const NostrEventEntity* event)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(event, NOSTR_DB_ERROR_NULL_PARAM);

  // Serialize event to binary format
  uint8_t buf[8192];
  int32_t size = event_serialize(event, buf, sizeof(buf));
  if (size < 0) {
    return NOSTR_DB_ERROR_INVALID_EVENT;
  }

  // Insert record into storage
  RecordId     rid;
  NostrDBError err = record_insert(&db->buffer_pool, buf, (uint16_t)size, &rid);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // Get EventRecord header and tag data for index insertion
  EventRecord*   rec         = (EventRecord*)buf;
  const uint8_t* tags_data   = buf + sizeof(EventRecord) + rec->content_length;
  uint16_t       tags_length = rec->tags_length;

  // Insert into all indexes
  err = index_manager_insert_event(&db->indexes, rid, rec, tags_data,
                                   tags_length);
  if (err != NOSTR_DB_OK) {
    // Rollback record insertion on index failure
    record_delete(&db->buffer_pool, rid);
    return err;
  }

  db->event_count++;
  return NOSTR_DB_OK;
}

// ============================================================================
// nostr_db_get_event_by_id
// ============================================================================
NostrDBError nostr_db_get_event_by_id(NostrDB* db, const uint8_t* id,
                                      NostrEventEntity* out)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(id, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out, NOSTR_DB_ERROR_NULL_PARAM);

  // Lookup RecordId via ID index
  RecordId     rid;
  NostrDBError err = index_id_lookup(&db->indexes.id_index, id, &rid);
  if (err != NOSTR_DB_OK) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Read record data
  uint8_t  buf[8192];
  uint16_t length = sizeof(buf);
  err             = record_read(&db->buffer_pool, rid, buf, &length);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  // Check deleted flag
  EventRecord* rec = (EventRecord*)buf;
  if (rec->flags & NOSTR_DB_EVENT_FLAG_DELETED) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Deserialize to NostrEventEntity
  return event_deserialize(buf, length, out);
}

// ============================================================================
// nostr_db_get_event_at_offset
// ============================================================================
NostrDBError nostr_db_get_event_at_offset(NostrDB*          db,
                                          nostr_db_offset_t offset,
                                          NostrEventEntity* out)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(out, NOSTR_DB_ERROR_NULL_PARAM);

  // Interpret offset as packed RecordId (page_id in upper 32, slot in lower 16)
  RecordId rid;
  rid.page_id    = (page_id_t)(offset >> 16);
  rid.slot_index = (uint16_t)(offset & 0xFFFF);

  if (rid.page_id == PAGE_ID_NULL) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Read record data
  uint8_t      buf[8192];
  uint16_t     length = sizeof(buf);
  NostrDBError err    = record_read(&db->buffer_pool, rid, buf, &length);
  if (err != NOSTR_DB_OK) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Check deleted flag
  EventRecord* rec = (EventRecord*)buf;
  if (rec->flags & NOSTR_DB_EVENT_FLAG_DELETED) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  return event_deserialize(buf, length, out);
}

// ============================================================================
// nostr_db_delete_event
// ============================================================================
NostrDBError nostr_db_delete_event(NostrDB* db, const uint8_t* id)
{
  require_not_null(db, NOSTR_DB_ERROR_NULL_PARAM);
  require_not_null(id, NOSTR_DB_ERROR_NULL_PARAM);

  // Lookup RecordId via ID index
  RecordId     rid;
  NostrDBError err = index_id_lookup(&db->indexes.id_index, id, &rid);
  if (err != NOSTR_DB_OK) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Read the record to get data needed for index deletion
  uint8_t  buf[8192];
  uint16_t length = sizeof(buf);
  err             = record_read(&db->buffer_pool, rid, buf, &length);
  if (err != NOSTR_DB_OK) {
    return err;
  }

  EventRecord* rec = (EventRecord*)buf;

  // Check if already deleted
  if (rec->flags & NOSTR_DB_EVENT_FLAG_DELETED) {
    return NOSTR_DB_ERROR_NOT_FOUND;
  }

  // Remove from all indexes
  const uint8_t* tags_data   = buf + sizeof(EventRecord) + rec->content_length;
  uint16_t       tags_length = rec->tags_length;
  index_manager_delete_event(&db->indexes, rid, rec, tags_data, tags_length);

  // Mark record as deleted by setting flag
  rec->flags |= NOSTR_DB_EVENT_FLAG_DELETED;
  record_update(&db->buffer_pool, &rid, buf, length);

  db->deleted_count++;
  return NOSTR_DB_OK;
}
