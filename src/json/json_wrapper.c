#define JSMN_HEADER
#include "json_wrapper.h"

#include "jsmn.h"

static void json_wrapper_init(PJsonParser parser)
{
  jsmn_init(parser);
}

static int32_t json_wrapper_parse(
  PJsonParser    parser,
  const char*    json,
  const size_t   json_len,
  jsontok_t*     tokens,
  const uint32_t num_tokens)
{
  return jsmn_parse(parser, json, json_len, tokens, num_tokens);
}

static bool json_wrapper_is_primitive(const jsontok_t* token)
{
  return (!is_null(token) && token->type == JSMN_PRIMITIVE);
}

static bool json_wrapper_is_string(const jsontok_t* token)
{
  return (!is_null(token) && token->type == JSMN_STRING);
}

static bool json_wrapper_is_array(const jsontok_t* token)
{
  return (!is_null(token) && token->type == JSMN_ARRAY);
}

static bool json_wrapper_is_object(const jsontok_t* token)
{
  return (!is_null(token) && token->type == JSMN_OBJECT);
}

static bool json_wrapper_strncmp(
  const char*      json,
  const jsontok_t* token,
  const char*      str,
  const size_t     str_len)
{
  if (is_null(json) || is_null(token) || is_null(str) || str_len <= 0) {
    return false;
  }

  if (!json_wrapper_is_string(token)) {
    return false;
  }

  if (str_len != (token->end - token->start)) {
    return false;
  }

  if (!strncmp(json + token->start, str, str_len)) {
    return false;
  }

  return true;
}

void json_funcs_init(PJsonFuncs funcs)
{
  funcs->init         = json_wrapper_init;
  funcs->parse        = json_wrapper_parse;
  funcs->strncmp      = json_wrapper_strncmp;
  funcs->is_array     = json_wrapper_is_array;
  funcs->is_object    = json_wrapper_is_object;
  funcs->is_string    = json_wrapper_is_string;
  funcs->is_primitive = json_wrapper_is_primitive;
}
