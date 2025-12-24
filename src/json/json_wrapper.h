#ifndef NOSTR_JSMN_WRAPPER_H_
#define NOSTR_JSMN_WRAPPER_H_

#include "../util/string.h"
#include "jsmn.h"

#define JSON_TOKEN_CAPACITY (4 * 1024)

typedef jsmntype_t  jsontype_t;
typedef jsmntok_t   jsontok_t;
typedef jsmn_parser JsonParser, *PJsonParser;

typedef bool (*PJsonStrCmpCallback)(
  const char*      json,
  const jsontok_t* token,
  const char*      str,
  const size_t     str_len);

typedef int32_t (*PJsonParseCallback)(
  PJsonParser    parser,
  const char*    json,
  const size_t   json_len,
  jsontok_t*     tokens,
  const uint32_t num_tokens);

typedef void (*PJsonInitCallback)(
  PJsonParser parser);

typedef bool (*PJsonTokenIsArrayCallback)(
  const jsontok_t* token);

typedef bool (*PJsonTokenIsObjectCallback)(
  const jsontok_t* token);

typedef bool (*PJsonTokenIsStringCallback)(
  const jsontok_t* token);

typedef bool (*PJsonTokenIsPrimitiveCallback)(
  const jsontok_t* token);

typedef struct {
  PJsonInitCallback             init;
  PJsonStrCmpCallback           strncmp;
  PJsonParseCallback            parse;
  PJsonTokenIsObjectCallback    is_object;
  PJsonTokenIsArrayCallback     is_array;
  PJsonTokenIsStringCallback    is_string;
  PJsonTokenIsPrimitiveCallback is_primitive;
} JsonFuncs, *PJsonFuncs;

void json_funcs_init(PJsonFuncs funcs);

#endif
