#include "../../websocket_local.h"

bool is_valid_request(PHTTPRequest restrict request)
{
  if (!is_valid_request_line(&request->line)) {
    return false;
  }

  if (!is_valid_request_header(request->headers, request->header_size)) {
    return false;
  }

  return true;
}
