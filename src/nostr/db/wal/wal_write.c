#include "../../../arch/memory.h"
#include "../../../util/string.h"
#include "wal_manager.h"

// ============================================================================
// Internal: Find active transaction index, returns -1 if not found
// ============================================================================
static int32_t find_active_tx(const WalManager* wal, uint32_t tx_id)
{
  for (uint8_t i = 0; i < wal->active_tx_count; i++) {
    if (wal->active_tx[i] == tx_id) {
      return (int32_t)i;
    }
  }
  return -1;
}

// ============================================================================
// Internal: Remove active transaction by index
// ============================================================================
static void remove_active_tx(WalManager* wal, int32_t idx)
{
  if (idx < 0 || idx >= wal->active_tx_count) {
    return;
  }

  // Shift remaining entries left
  for (int32_t i = idx; i < wal->active_tx_count - 1; i++) {
    wal->active_tx[i]     = wal->active_tx[i + 1];
    wal->active_tx_lsn[i] = wal->active_tx_lsn[i + 1];
  }
  wal->active_tx_count--;
}

// ============================================================================
// Internal: Ensure buffer has enough space, flush if needed
// ============================================================================
static NostrDBError ensure_buffer_space(WalManager* wal, uint32_t needed)
{
  if (wal->buffer_used + needed > wal->buffer_size) {
    NostrDBError err = wal_flush(wal);
    if (err != NOSTR_DB_OK) {
      return err;
    }
  }
  // After flush, check if single record fits in buffer
  if (needed > wal->buffer_size) {
    return NOSTR_DB_ERROR_FULL;
  }
  return NOSTR_DB_OK;
}

// ============================================================================
// Internal: Write a simple record (no payload) into the WAL buffer
// ============================================================================
static lsn_t write_simple_record(WalManager* wal, uint32_t tx_id,
                                 WalRecordType type)
{
  uint32_t total = sizeof(WalRecordHeader);
  if (ensure_buffer_space(wal, total) != NOSTR_DB_OK) {
    return LSN_NULL;
  }

  lsn_t lsn = wal->next_lsn++;

  // Find prev_lsn for this transaction
  lsn_t   prev_lsn = LSN_NULL;
  int32_t tx_idx   = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    prev_lsn = wal->active_tx_lsn[tx_idx];
  }

  WalRecordHeader hdr;
  internal_memset(&hdr, 0, sizeof(hdr));
  hdr.lsn         = lsn;
  hdr.prev_lsn    = prev_lsn;
  hdr.tx_id       = tx_id;
  hdr.type        = type;
  hdr.data_length = 0;

  internal_memcpy(wal->buffer + wal->buffer_used, &hdr, sizeof(hdr));
  wal->buffer_used += sizeof(hdr);

  // Update active tx LSN
  if (tx_idx >= 0) {
    wal->active_tx_lsn[tx_idx] = lsn;
  }

  return lsn;
}

// ============================================================================
// wal_log_begin
// ============================================================================
uint32_t wal_log_begin(WalManager* wal)
{
  require_not_null(wal, 0);
  require(wal->active_tx_count < 64, 0);

  uint32_t tx_id = wal->next_tx_id++;

  // Add to active transaction list before writing
  uint8_t idx             = wal->active_tx_count;
  wal->active_tx[idx]     = tx_id;
  wal->active_tx_lsn[idx] = LSN_NULL;
  wal->active_tx_count++;

  lsn_t lsn = write_simple_record(wal, tx_id, WAL_RECORD_BEGIN);
  if (lsn == LSN_NULL) {
    // Rollback
    wal->active_tx_count--;
    return 0;
  }

  return tx_id;
}

// ============================================================================
// wal_log_update
// ============================================================================
lsn_t wal_log_update(WalManager* wal, uint32_t tx_id, page_id_t page_id,
                     uint16_t offset, uint16_t length, const void* old_data,
                     const void* new_data)
{
  require_not_null(wal, LSN_NULL);
  require_not_null(old_data, LSN_NULL);
  require_not_null(new_data, LSN_NULL);
  require(length > 0, LSN_NULL);

  uint16_t data_length =
    (uint16_t)(sizeof(WalUpdatePayload) + length * 2);
  uint32_t total = sizeof(WalRecordHeader) + data_length;

  if (ensure_buffer_space(wal, total) != NOSTR_DB_OK) {
    return LSN_NULL;
  }

  lsn_t lsn = wal->next_lsn++;

  // Find prev_lsn for this transaction
  lsn_t   prev_lsn = LSN_NULL;
  int32_t tx_idx   = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    prev_lsn = wal->active_tx_lsn[tx_idx];
  }

  // Write header
  WalRecordHeader hdr;
  internal_memset(&hdr, 0, sizeof(hdr));
  hdr.lsn         = lsn;
  hdr.prev_lsn    = prev_lsn;
  hdr.tx_id       = tx_id;
  hdr.type        = WAL_RECORD_UPDATE;
  hdr.data_length = data_length;

  uint8_t* dst = wal->buffer + wal->buffer_used;
  internal_memcpy(dst, &hdr, sizeof(hdr));
  dst += sizeof(hdr);

  // Write update payload
  WalUpdatePayload payload;
  payload.page_id = page_id;
  payload.offset  = offset;
  payload.length  = length;
  internal_memcpy(dst, &payload, sizeof(payload));
  dst += sizeof(payload);

  // Write old_data (before-image)
  internal_memcpy(dst, old_data, length);
  dst += length;

  // Write new_data (after-image)
  internal_memcpy(dst, new_data, length);

  wal->buffer_used += total;

  // Update active tx LSN
  if (tx_idx >= 0) {
    wal->active_tx_lsn[tx_idx] = lsn;
  }

  return lsn;
}

// ============================================================================
// wal_log_alloc_page
// ============================================================================
lsn_t wal_log_alloc_page(WalManager* wal, uint32_t tx_id, page_id_t page_id)
{
  require_not_null(wal, LSN_NULL);

  uint16_t data_length = (uint16_t)sizeof(WalPagePayload);
  uint32_t total       = sizeof(WalRecordHeader) + data_length;

  if (ensure_buffer_space(wal, total) != NOSTR_DB_OK) {
    return LSN_NULL;
  }

  lsn_t lsn = wal->next_lsn++;

  lsn_t   prev_lsn = LSN_NULL;
  int32_t tx_idx   = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    prev_lsn = wal->active_tx_lsn[tx_idx];
  }

  WalRecordHeader hdr;
  internal_memset(&hdr, 0, sizeof(hdr));
  hdr.lsn         = lsn;
  hdr.prev_lsn    = prev_lsn;
  hdr.tx_id       = tx_id;
  hdr.type        = WAL_RECORD_ALLOC_PAGE;
  hdr.data_length = data_length;

  uint8_t* dst = wal->buffer + wal->buffer_used;
  internal_memcpy(dst, &hdr, sizeof(hdr));
  dst += sizeof(hdr);

  WalPagePayload payload;
  payload.page_id = page_id;
  internal_memcpy(dst, &payload, sizeof(payload));

  wal->buffer_used += total;

  if (tx_idx >= 0) {
    wal->active_tx_lsn[tx_idx] = lsn;
  }

  return lsn;
}

// ============================================================================
// wal_log_free_page
// ============================================================================
lsn_t wal_log_free_page(WalManager* wal, uint32_t tx_id, page_id_t page_id)
{
  require_not_null(wal, LSN_NULL);

  uint16_t data_length = (uint16_t)sizeof(WalPagePayload);
  uint32_t total       = sizeof(WalRecordHeader) + data_length;

  if (ensure_buffer_space(wal, total) != NOSTR_DB_OK) {
    return LSN_NULL;
  }

  lsn_t lsn = wal->next_lsn++;

  lsn_t   prev_lsn = LSN_NULL;
  int32_t tx_idx   = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    prev_lsn = wal->active_tx_lsn[tx_idx];
  }

  WalRecordHeader hdr;
  internal_memset(&hdr, 0, sizeof(hdr));
  hdr.lsn         = lsn;
  hdr.prev_lsn    = prev_lsn;
  hdr.tx_id       = tx_id;
  hdr.type        = WAL_RECORD_FREE_PAGE;
  hdr.data_length = data_length;

  uint8_t* dst = wal->buffer + wal->buffer_used;
  internal_memcpy(dst, &hdr, sizeof(hdr));
  dst += sizeof(hdr);

  WalPagePayload payload;
  payload.page_id = page_id;
  internal_memcpy(dst, &payload, sizeof(payload));

  wal->buffer_used += total;

  if (tx_idx >= 0) {
    wal->active_tx_lsn[tx_idx] = lsn;
  }

  return lsn;
}

// ============================================================================
// wal_log_commit
// ============================================================================
NostrDBError wal_log_commit(WalManager* wal, uint32_t tx_id)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);

  lsn_t lsn = write_simple_record(wal, tx_id, WAL_RECORD_COMMIT);
  if (lsn == LSN_NULL) {
    return NOSTR_DB_ERROR_FULL;
  }

  // Remove from active transaction list
  int32_t tx_idx = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    remove_active_tx(wal, tx_idx);
  }

  // Flush WAL buffer on commit to guarantee durability
  return wal_flush(wal);
}

// ============================================================================
// wal_log_abort
// ============================================================================
NostrDBError wal_log_abort(WalManager* wal, uint32_t tx_id)
{
  require_not_null(wal, NOSTR_DB_ERROR_NULL_PARAM);

  lsn_t lsn = write_simple_record(wal, tx_id, WAL_RECORD_ABORT);
  if (lsn == LSN_NULL) {
    return NOSTR_DB_ERROR_FULL;
  }

  // Remove from active transaction list
  int32_t tx_idx = find_active_tx(wal, tx_id);
  if (tx_idx >= 0) {
    remove_active_tx(wal, tx_idx);
  }

  return NOSTR_DB_OK;
}
