#include "arch/mmap.h"
#include "nostr/db/db.h"
#include "nostr/db/query/db_query.h"
#include "nostr/db/query/db_query_types.h"
#include "nostr/nostr_func.h"
#include "nostr/response/nostr_response.h"
#include "nostr/subscription/nostr_close.h"
#include "nostr/subscription/nostr_filter.h"
#include "nostr/subscription/nostr_req.h"
#include "nostr/subscription/nostr_subscription.h"
#include "util/allocator.h"
#include "util/log.h"
#include "websocket/websocket.h"

// ============================================================================
// Global state
// ============================================================================
static NostrDB*                 g_db                   = NULL;
static NostrSubscriptionManager g_subscription_manager = {NULL, 0};
static bool                     g_db_initialized       = false;

// ============================================================================
// Response buffer for sending messages
// ============================================================================
#define RESPONSE_BUFFER_SIZE 65536
static char g_response_buffer[RESPONSE_BUFFER_SIZE];

// ============================================================================
// Helper: Send WebSocket text message
// ============================================================================
static bool send_websocket_message(int32_t client_sock, const char* message, size_t message_len)
{
  WebSocketEntity response_entity;
  char            packet_buffer[RESPONSE_BUFFER_SIZE];

  internal_memset(&response_entity, 0, sizeof(WebSocketEntity));
  response_entity.fin     = 1;
  response_entity.opcode  = WEBSOCKET_OP_CODE_TEXT;
  response_entity.mask    = 0;
  response_entity.payload = (char*)message;

  if (message_len <= 125) {
    response_entity.payload_len     = (uint8_t)message_len;
    response_entity.ext_payload_len = message_len;
  } else if (message_len <= 65535) {
    response_entity.payload_len     = 126;
    response_entity.ext_payload_len = message_len;
  } else {
    response_entity.payload_len     = 127;
    response_entity.ext_payload_len = message_len;
  }

  size_t packet_size = to_websocket_packet(&response_entity, sizeof(packet_buffer), packet_buffer);
  if (packet_size == 0) {
    log_error("Failed to create websocket packet.\n");
    return false;
  }

  websocket_send(client_sock, packet_size, packet_buffer);
  return true;
}

// ============================================================================
// Helper: Send OK response
// ============================================================================
static void send_ok_response(int32_t client_sock, const char* event_id, bool ok, const char* message)
{
  if (nostr_response_ok(event_id, ok, message, g_response_buffer, RESPONSE_BUFFER_SIZE)) {
    size_t len = strlen(g_response_buffer);
    send_websocket_message(client_sock, g_response_buffer, len);
  }
}

// ============================================================================
// Helper: Convert hex character to value
// ============================================================================
static int32_t hex_char_val(char c)
{
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

// ============================================================================
// Helper: Convert hex string to binary
// ============================================================================
static bool hex_str_to_bin(const char* hex, uint8_t* out, size_t out_len)
{
  for (size_t i = 0; i < out_len; i++) {
    int32_t h = hex_char_val(hex[i * 2]);
    int32_t l = hex_char_val(hex[i * 2 + 1]);
    if (h < 0 || l < 0) return false;
    out[i] = (uint8_t)((h << 4) | l);
  }
  return true;
}

// ============================================================================
// Helper: Check if event kind is replaceable (NIP-01)
// ============================================================================
static bool is_replaceable_kind(uint32_t kind)
{
  return kind == 0 || kind == 3 || (kind >= 10000 && kind <= 19999);
}

// ============================================================================
// Helper: Check if event kind is addressable/parameterized replaceable (NIP-01)
// ============================================================================
static bool is_addressable_kind(uint32_t kind)
{
  return kind >= 30000 && kind <= 39999;
}

// ============================================================================
// Helper: Get d-tag value from event (returns "" if not found)
// ============================================================================
static const char* get_d_tag_value(const NostrEventEntity* event)
{
  for (uint32_t i = 0; i < event->tag_count; i++) {
    if (event->tags[i].key[0] == 'd' && event->tags[i].key[1] == '\0') {
      if (event->tags[i].item_count >= 1) {
        return event->tags[i].values[0];
      }
      return "";
    }
  }
  return "";
}

// ============================================================================
// Helper: Case-sensitive string equality check
// ============================================================================
static bool str_equal(const char* a, const char* b)
{
  while (*a && *b) {
    if (*a != *b) return false;
    a++;
    b++;
  }
  return *a == *b;
}

// ============================================================================
// Helper: Convert NostrFilter to NostrDBFilter
// ============================================================================
static void convert_filter_to_db_filter(const NostrFilter* src, NostrDBFilter* dst)
{
  nostr_db_filter_init(dst);

  // Copy IDs
  dst->ids_count = src->ids_count;
  for (size_t i = 0; i < src->ids_count && i < NOSTR_DB_FILTER_MAX_IDS; i++) {
    internal_memcpy(dst->ids[i].value, src->ids[i].value, 32);
    dst->ids[i].prefix_len = src->ids[i].prefix_len;
  }

  // Copy authors
  dst->authors_count = src->authors_count;
  for (size_t i = 0; i < src->authors_count && i < NOSTR_DB_FILTER_MAX_AUTHORS; i++) {
    internal_memcpy(dst->authors[i].value, src->authors[i].value, 32);
    dst->authors[i].prefix_len = src->authors[i].prefix_len;
  }

  // Copy kinds
  dst->kinds_count = src->kinds_count;
  for (size_t i = 0; i < src->kinds_count && i < NOSTR_DB_FILTER_MAX_KINDS; i++) {
    dst->kinds[i] = src->kinds[i];
  }

  // Copy tag filters
  dst->tags_count = src->tags_count;
  for (size_t i = 0; i < src->tags_count && i < NOSTR_DB_FILTER_MAX_TAGS; i++) {
    dst->tags[i].name         = src->tags[i].name;
    dst->tags[i].values_count = src->tags[i].values_count;
    for (size_t j = 0; j < src->tags[i].values_count && j < NOSTR_DB_FILTER_MAX_TAG_VALUES; j++) {
      internal_memcpy(dst->tags[i].values[j], src->tags[i].values[j], 32);
    }
  }

  // Copy time range and limit
  dst->since = src->since;
  dst->until = src->until;
  if (src->has_limit) {
    dst->limit = src->limit;
  } else {
    dst->limit = NOSTR_DB_QUERY_DEFAULT_LIMIT;
  }
}

// ============================================================================
// Callback context for broadcast
// ============================================================================
typedef struct {
  const NostrEventEntity* event;
  int32_t                 source_client;  // Client that sent the event (don't echo back)
} BroadcastContext;

// ============================================================================
// Helper: Broadcast callback for subscription matching
// ============================================================================
static void broadcast_to_subscription(const NostrSubscription* subscription, void* user_data)
{
  BroadcastContext* ctx = (BroadcastContext*)user_data;

  // Don't echo back to the source client
  if (subscription->client_fd == ctx->source_client) {
    return;
  }

  // Generate EVENT response
  if (nostr_response_event(subscription->subscription_id, ctx->event, g_response_buffer, RESPONSE_BUFFER_SIZE)) {
    size_t len = strlen(g_response_buffer);
    send_websocket_message(subscription->client_fd, g_response_buffer, len);
  }
}

// ============================================================================
// Helper: Store event and broadcast to matching subscriptions
// ============================================================================
static bool store_and_broadcast(int32_t client_sock, const NostrEventEntity* event)
{
  NostrDBError err = nostr_db_write_event(g_db, event);

  if (err == NOSTR_DB_OK) {
    send_ok_response(client_sock, event->id, true, "");

    BroadcastContext ctx;
    ctx.event         = event;
    ctx.source_client = client_sock;
    nostr_subscription_find_matching(&g_subscription_manager, event, broadcast_to_subscription, &ctx);
    return true;
  } else if (err == NOSTR_DB_ERROR_DUPLICATE) {
    send_ok_response(client_sock, event->id, true, "duplicate:");
    return true;
  } else if (err == NOSTR_DB_ERROR_DELETED) {
    send_ok_response(client_sock, event->id, false, "deleted: event was previously deleted");
    return true;
  } else {
    const char* msg = "error: failed to save event";
    if (err == NOSTR_DB_ERROR_FULL) {
      msg = "error: database full";
    } else if (err == NOSTR_DB_ERROR_INVALID_EVENT) {
      msg = "error: invalid event";
    }
    send_ok_response(client_sock, event->id, false, msg);
    return false;
  }
}

// ============================================================================
// Handle replaceable events: check for existing and delete old ones
// Returns true if the new event should be stored, false if rejected
// ============================================================================
static bool handle_replaceable_check(int32_t client_sock, const NostrEventEntity* event)
{
  uint8_t pubkey_bin[32];
  if (!hex_str_to_bin(event->pubkey, pubkey_bin, 32)) return true;

  NostrDBFilter db_filter;
  nostr_db_filter_init(&db_filter);
  db_filter.authors_count         = 1;
  db_filter.authors[0].prefix_len = 32;
  internal_memcpy(db_filter.authors[0].value, pubkey_bin, 32);
  db_filter.kinds_count = 1;
  db_filter.kinds[0]    = event->kind;
  db_filter.limit       = 100;

  NostrDBResultSet* result = nostr_db_result_create(0);
  if (is_null(result)) return true;

  NostrDBError err = nostr_db_query_execute(g_db, &db_filter, result);
  if (err != NOSTR_DB_OK || result->count == 0) {
    nostr_db_result_free(result);
    return true;
  }

  NostrEventEntity* existing = (NostrEventEntity*)internal_mmap(
    NULL, sizeof(NostrEventEntity), PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (existing == MAP_FAILED) {
    nostr_db_result_free(result);
    return true;
  }

  bool should_store = true;

  // First pass: check if any existing event is newer
  for (uint32_t i = 0; i < result->count; i++) {
    if (nostr_db_get_event_at_offset(g_db, result->offsets[i], existing) != NOSTR_DB_OK) {
      continue;
    }

    if (existing->created_at > event->created_at) {
      should_store = false;
      break;
    }
    if (existing->created_at == event->created_at) {
      // Same timestamp: keep the one with the lexicographically lower ID
      size_t id_len = strlen(event->id);
      if (strncmp_sensitive(existing->id, event->id, id_len, id_len, true) &&
          id_len == strlen(existing->id)) {
        // IDs are equal - same event, treat as duplicate (will be caught by write)
      } else {
        // Compare character by character
        bool existing_lower = false;
        for (size_t c = 0; c < 64; c++) {
          if (existing->id[c] < event->id[c]) {
            existing_lower = true;
            break;
          } else if (existing->id[c] > event->id[c]) {
            break;
          }
        }
        if (existing_lower) {
          should_store = false;
          break;
        }
      }
    }
  }

  if (!should_store) {
    send_ok_response(client_sock, event->id, false, "duplicate: have a newer event");
    internal_munmap(existing, sizeof(NostrEventEntity));
    nostr_db_result_free(result);
    return false;
  }

  // Second pass: delete all old events
  for (uint32_t i = 0; i < result->count; i++) {
    if (nostr_db_get_event_at_offset(g_db, result->offsets[i], existing) == NOSTR_DB_OK) {
      uint8_t old_id_bin[32];
      if (hex_str_to_bin(existing->id, old_id_bin, 32)) {
        nostr_db_delete_event(g_db, old_id_bin);
      }
    }
  }

  internal_munmap(existing, sizeof(NostrEventEntity));
  nostr_db_result_free(result);
  return true;
}

// ============================================================================
// Handle addressable events: check for existing with same d-tag
// Returns true if the new event should be stored, false if rejected
// ============================================================================
static bool handle_addressable_check(int32_t client_sock, const NostrEventEntity* event)
{
  uint8_t pubkey_bin[32];
  if (!hex_str_to_bin(event->pubkey, pubkey_bin, 32)) return true;

  const char* d_value = get_d_tag_value(event);

  NostrDBFilter db_filter;
  nostr_db_filter_init(&db_filter);
  db_filter.authors_count         = 1;
  db_filter.authors[0].prefix_len = 32;
  internal_memcpy(db_filter.authors[0].value, pubkey_bin, 32);
  db_filter.kinds_count = 1;
  db_filter.kinds[0]    = event->kind;
  db_filter.limit       = 100;

  NostrDBResultSet* result = nostr_db_result_create(0);
  if (is_null(result)) return true;

  NostrDBError err = nostr_db_query_execute(g_db, &db_filter, result);
  if (err != NOSTR_DB_OK || result->count == 0) {
    nostr_db_result_free(result);
    return true;
  }

  NostrEventEntity* existing = (NostrEventEntity*)internal_mmap(
    NULL, sizeof(NostrEventEntity), PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (existing == MAP_FAILED) {
    nostr_db_result_free(result);
    return true;
  }

  bool should_store = true;

  for (uint32_t i = 0; i < result->count; i++) {
    if (nostr_db_get_event_at_offset(g_db, result->offsets[i], existing) != NOSTR_DB_OK) {
      continue;
    }

    // Check d-tag match
    const char* existing_d = get_d_tag_value(existing);
    if (!str_equal(existing_d, d_value)) {
      continue;
    }

    if (existing->created_at > event->created_at) {
      should_store = false;
      break;
    }
    if (existing->created_at == event->created_at) {
      bool existing_lower = false;
      for (size_t c = 0; c < 64; c++) {
        if (existing->id[c] < event->id[c]) {
          existing_lower = true;
          break;
        } else if (existing->id[c] > event->id[c]) {
          break;
        }
      }
      if (existing_lower) {
        should_store = false;
        break;
      }
    }

    // Delete old addressable event
    uint8_t old_id_bin[32];
    if (hex_str_to_bin(existing->id, old_id_bin, 32)) {
      nostr_db_delete_event(g_db, old_id_bin);
    }
  }

  if (!should_store) {
    send_ok_response(client_sock, event->id, false, "duplicate: have a newer event");
  }

  internal_munmap(existing, sizeof(NostrEventEntity));
  nostr_db_result_free(result);
  return should_store;
}

// ============================================================================
// Process deletion event (kind 5): delete referenced events
// Returns true if at least one valid deletion was performed or all refs are own
// ============================================================================
static bool process_deletion_event(int32_t client_sock, const NostrEventEntity* deletion_event)
{
  uint8_t deletion_pubkey_bin[32];
  if (!hex_str_to_bin(deletion_event->pubkey, deletion_pubkey_bin, 32)) return false;

  NostrEventEntity* target = (NostrEventEntity*)internal_mmap(
    NULL, sizeof(NostrEventEntity), PROT_READ | PROT_WRITE,
    MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  if (target == MAP_FAILED) return false;

  bool has_invalid_ref = false;

  for (uint32_t i = 0; i < deletion_event->tag_count; i++) {
    const NostrTagEntity* tag = &deletion_event->tags[i];

    // Process e-tags (delete by event ID)
    if (tag->key[0] == 'e' && tag->key[1] == '\0' && tag->item_count >= 1) {
      size_t id_len = strlen(tag->values[0]);
      if (id_len != 64) continue;

      uint8_t target_id_bin[32];
      if (!hex_str_to_bin(tag->values[0], target_id_bin, 32)) continue;

      if (nostr_db_get_event_by_id(g_db, target_id_bin, target) == NOSTR_DB_OK) {
        uint8_t target_pubkey_bin[32];
        if (hex_str_to_bin(target->pubkey, target_pubkey_bin, 32) &&
            internal_memcmp(deletion_pubkey_bin, target_pubkey_bin, 32) == 0) {
          nostr_db_delete_event(g_db, target_id_bin);
        } else {
          has_invalid_ref = true;
        }
      }
      // If event not found, just skip (not an error)
    }

    // Process a-tags (delete by kind:pubkey:d-tag)
    if (tag->key[0] == 'a' && tag->key[1] == '\0' && tag->item_count >= 1) {
      const char* a_val = tag->values[0];

      // Parse kind
      uint32_t a_kind = 0;
      size_t   pos    = 0;
      while (a_val[pos] && a_val[pos] != ':') {
        if (a_val[pos] < '0' || a_val[pos] > '9') break;
        a_kind = a_kind * 10 + (uint32_t)(a_val[pos] - '0');
        pos++;
      }
      if (a_val[pos] != ':') continue;
      pos++;

      // Parse pubkey (64 hex chars)
      const char* a_pubkey_start = &a_val[pos];
      size_t      a_pubkey_len   = 0;
      while (a_val[pos] && a_val[pos] != ':') {
        a_pubkey_len++;
        pos++;
      }
      if (a_pubkey_len != 64) continue;

      uint8_t a_pubkey_bin[32];
      if (!hex_str_to_bin(a_pubkey_start, a_pubkey_bin, 32)) continue;

      // Check that the a-tag pubkey matches the deletion event's pubkey
      if (internal_memcmp(deletion_pubkey_bin, a_pubkey_bin, 32) != 0) {
        has_invalid_ref = true;
        continue;
      }

      // Parse d-tag value
      const char* a_d_value = "";
      if (a_val[pos] == ':') {
        pos++;
        a_d_value = &a_val[pos];
      }

      // Query for matching events
      NostrDBFilter db_filter;
      nostr_db_filter_init(&db_filter);
      db_filter.authors_count         = 1;
      db_filter.authors[0].prefix_len = 32;
      internal_memcpy(db_filter.authors[0].value, a_pubkey_bin, 32);
      db_filter.kinds_count = 1;
      db_filter.kinds[0]    = a_kind;
      db_filter.limit       = 100;

      NostrDBResultSet* result = nostr_db_result_create(0);
      if (is_null(result)) continue;

      if (nostr_db_query_execute(g_db, &db_filter, result) == NOSTR_DB_OK) {
        for (uint32_t j = 0; j < result->count; j++) {
          if (nostr_db_get_event_at_offset(g_db, result->offsets[j], target) == NOSTR_DB_OK) {
            const char* target_d = get_d_tag_value(target);
            if (str_equal(target_d, a_d_value)) {
              // Only delete if target is older than or same age as deletion event
              if (target->created_at <= deletion_event->created_at) {
                uint8_t target_id_bin[32];
                if (hex_str_to_bin(target->id, target_id_bin, 32)) {
                  nostr_db_delete_event(g_db, target_id_bin);
                }
              }
            }
          }
        }
      }

      nostr_db_result_free(result);
    }
  }

  internal_munmap(target, sizeof(NostrEventEntity));

  if (has_invalid_ref) {
    send_ok_response(client_sock, deletion_event->id, false,
                     "blocked: cannot delete other's events");
    return false;
  }

  return true;
}

// ============================================================================
// Handle EVENT message
// ============================================================================
static bool handle_event_message(int32_t client_sock, const NostrEventEntity* event)
{
  if (!g_db_initialized || g_db == NULL) {
    send_ok_response(client_sock, event->id, false, "error: database not initialized");
    return false;
  }

  // Handle deletion events (kind 5)
  if (event->kind == 5) {
    if (!process_deletion_event(client_sock, event)) {
      return true;  // Rejected (tried to delete other's events)
    }
    // Store the deletion event itself
    return store_and_broadcast(client_sock, event);
  }

  // Handle replaceable events (kinds 0, 3, 10000-19999)
  if (is_replaceable_kind(event->kind)) {
    if (!handle_replaceable_check(client_sock, event)) {
      return true;  // Rejected - older than existing
    }
  }

  // Handle addressable events (kinds 30000-39999)
  if (is_addressable_kind(event->kind)) {
    if (!handle_addressable_check(client_sock, event)) {
      return true;  // Rejected - older than existing
    }
  }

  // Store and broadcast
  return store_and_broadcast(client_sock, event);
}

// ============================================================================
// Handle REQ message
// ============================================================================
static bool handle_req_message(int32_t client_sock, const NostrReqMessage* req)
{
  // Add subscription
  NostrSubscription* sub = nostr_subscription_add(&g_subscription_manager, client_sock, req);
  if (sub == NULL) {
    // Send CLOSED response if subscription limit reached
    if (nostr_response_closed(req->subscription_id, "error: subscription limit reached", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
    return false;
  }

  log_info("[REQ] Subscription added: ");
  log_info(req->subscription_id);
  log_info("\n");

  // Query database for matching events
  if (g_db_initialized && g_db != NULL) {
    for (size_t filter_idx = 0; filter_idx < req->filters_count; filter_idx++) {
      NostrDBFilter db_filter;
      convert_filter_to_db_filter(&req->filters[filter_idx], &db_filter);

      NostrDBResultSet* result = nostr_db_result_create(0);
      if (is_null(result)) continue;

      NostrDBError err = nostr_db_query_execute(g_db, &db_filter, result);
      if (err == NOSTR_DB_OK && result->count > 0) {
        // Allocate event on heap to avoid stack overflow
        NostrEventEntity* event = (NostrEventEntity*)internal_mmap(
          NULL, sizeof(NostrEventEntity), PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (event != MAP_FAILED) {
          for (uint32_t i = 0; i < result->count; i++) {
            if (nostr_db_get_event_at_offset(g_db, result->offsets[i], event) == NOSTR_DB_OK) {
              if (nostr_response_event(req->subscription_id, event, g_response_buffer, RESPONSE_BUFFER_SIZE)) {
                size_t len = strlen(g_response_buffer);
                send_websocket_message(client_sock, g_response_buffer, len);
              }
            }
          }
          internal_munmap(event, sizeof(NostrEventEntity));
        }
      }

      nostr_db_result_free(result);
    }
  }

  // Send EOSE (End of Stored Events)
  if (nostr_response_eose(req->subscription_id, g_response_buffer, RESPONSE_BUFFER_SIZE)) {
    size_t len = strlen(g_response_buffer);
    send_websocket_message(client_sock, g_response_buffer, len);
  }

  return true;
}

// ============================================================================
// Handle CLOSE message
// ============================================================================
static bool handle_close_message(int32_t client_sock, const NostrCloseMessage* close_msg)
{
  log_info("[CLOSE] Subscription: ");
  log_info(close_msg->subscription_id);
  log_info("\n");

  bool removed = nostr_subscription_remove(&g_subscription_manager, client_sock, close_msg->subscription_id);

  if (removed) {
    // Optionally send CLOSED response (not required by NIP-01)
    if (nostr_response_closed(close_msg->subscription_id, "", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
  }

  return true;
}

// ============================================================================
// Nostr protocol callback - EVENT
// ============================================================================
static int32_t g_current_client_sock = -1;

static bool nostr_event_callback(const NostrEventEntity* event)
{
  return handle_event_message(g_current_client_sock, event);
}

// ============================================================================
// Nostr protocol callback - REQ
// ============================================================================
static bool nostr_req_callback(const NostrReqMessage* req)
{
  return handle_req_message(g_current_client_sock, req);
}

// ============================================================================
// Nostr protocol callback - CLOSE
// ============================================================================
static bool nostr_close_callback(const NostrCloseMessage* close_msg)
{
  return handle_close_message(g_current_client_sock, close_msg);
}

// ============================================================================
// WebSocket receive callback
// ============================================================================
bool websocket_receive_callback(
  const int              client_sock,
  const WebSocketEntity* entity,
  const size_t           buffer_capacity,
  char*                  response_buffer)
{
  if (entity->opcode != WEBSOCKET_OP_CODE_TEXT) {
    return true;  // Ignore non-text frames
  }

  // Get payload
  const char* payload     = entity->payload;
  size_t      payload_len = entity->payload_len;
  if (payload_len == 126 || payload_len == 127) {
    payload_len = entity->ext_payload_len;
  }

  // Null-terminate payload for JSON parsing
  // Note: This assumes the payload buffer has room for null terminator
  char* payload_mutable        = (char*)payload;
  payload_mutable[payload_len] = '\0';

  // Set current client for callbacks
  g_current_client_sock = client_sock;

  // Parse Nostr message
  NostrFuncs nostr_funcs;
  nostr_funcs.event = nostr_event_callback;
  nostr_funcs.req   = nostr_req_callback;
  nostr_funcs.close = nostr_close_callback;

  if (!nostr_event_handler(payload, &nostr_funcs)) {
    // Send NOTICE for parse errors
    if (nostr_response_notice("error: invalid message format", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
  }

  return true;
}

// ============================================================================
// WebSocket connect callback
// ============================================================================
void websocket_connect_callback(int client_sock)
{
  log_info("[Connect] Client connected\n");
}

// ============================================================================
// WebSocket disconnect callback
// ============================================================================
void websocket_disconnect_callback(int client_sock)
{
  log_info("[Disconnect] Client disconnected\n");

  // Remove all subscriptions for this client
  size_t removed = nostr_subscription_remove_client(&g_subscription_manager, client_sock);
  if (removed > 0) {
    log_info("[Disconnect] Removed subscriptions: ");
    char num_buf[16];
    itoa((int32_t)removed, num_buf, sizeof(num_buf));
    log_info(num_buf);
    log_info("\n");
  }
}

// ============================================================================
// NIP-11 handshake callback
// ============================================================================
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

// ============================================================================
// Main
// ============================================================================
int main()
{
  // Initialize subscription manager
  if (!nostr_subscription_manager_init(&g_subscription_manager)) {
    log_error("[Subscription] Failed to initialize subscription manager\n");
    return 1;
  }

  // Initialize database
  NostrDBError db_err = nostr_db_init(&g_db, "./data");
  if (db_err == NOSTR_DB_OK) {
    g_db_initialized = true;
    log_info("[DB] Database initialized successfully\n");
  } else {
    log_error("[DB] Failed to initialize database, running without persistence\n");
    g_db             = NULL;
    g_db_initialized = false;
  }

  // Initialize WebSocket server
  WebSocketInitArgs init_args;
  init_args.port_num = 8080;
  init_args.backlog  = 5;

  int server_sock = websocket_server_init(&init_args);
  if (server_sock < WEBSOCKET_ERRORCODE_NONE) {
    log_error("websocket server init error.\n");
    var_error("server_sock: ", server_sock);
    if (g_db != NULL) {
      nostr_db_shutdown(g_db);
    }
    return 1;
  }

  log_info("[Server] Nostr relay started on port 8080\n");

  // Set up loop arguments
  WebSocketLoopArgs loop_args;
  loop_args.server_sock                   = server_sock;
  loop_args.callbacks.receive_callback    = websocket_receive_callback;
  loop_args.callbacks.connect_callback    = websocket_connect_callback;
  loop_args.callbacks.disconnect_callback = websocket_disconnect_callback;
  loop_args.callbacks.handshake_callback  = websocket_handshake_callback;
  loop_args.buffer_capacity               = 65536;

  // Run server loop (blocks until signal)
  websocket_server_loop(&loop_args);

  // Cleanup
  websocket_close(server_sock);

  if (g_db != NULL) {
    log_info("[DB] Shutting down database\n");
    nostr_db_shutdown(g_db);
    g_db = NULL;
  }

  nostr_subscription_manager_destroy(&g_subscription_manager);

  log_info("[Server] Nostr relay stopped\n");
  return 0;
}
