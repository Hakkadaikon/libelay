#include "arch/mmap.h"
#include "nostr/db/db.h"
#include "nostr/db/query/db_query.h"
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
  dst->limit = src->limit;
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
// Handle EVENT message
// ============================================================================
static bool handle_event_message(int32_t client_sock, const NostrEventEntity* event)
{
  if (!g_db_initialized || g_db == NULL) {
    // Send error OK response
    if (nostr_response_ok(event->id, false, "error: database not initialized", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
    return false;
  }

  // Save event to database
  NostrDBError err = nostr_db_write_event(g_db, event);

  if (err == NOSTR_DB_OK) {
    // Send OK success response
    if (nostr_response_ok(event->id, true, "", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }

    // Broadcast to matching subscriptions
    BroadcastContext ctx;
    ctx.event         = event;
    ctx.source_client = client_sock;
    nostr_subscription_find_matching(&g_subscription_manager, event, broadcast_to_subscription, &ctx);

    return true;
  } else if (err == NOSTR_DB_ERROR_DUPLICATE) {
    // Duplicate event - still OK per NIP-01
    if (nostr_response_ok(event->id, true, "duplicate:", g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
    return true;
  } else {
    // Error saving event
    const char* msg = "error: failed to save event";
    if (err == NOSTR_DB_ERROR_FULL) {
      msg = "error: database full";
    } else if (err == NOSTR_DB_ERROR_INVALID_EVENT) {
      msg = "error: invalid event";
    }

    if (nostr_response_ok(event->id, false, msg, g_response_buffer, RESPONSE_BUFFER_SIZE)) {
      size_t len = strlen(g_response_buffer);
      send_websocket_message(client_sock, g_response_buffer, len);
    }
    return false;
  }
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
