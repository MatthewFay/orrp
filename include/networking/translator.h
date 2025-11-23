#ifndef translator_h
#define translator_h

#include "engine/api.h"
#include <stdbool.h>

typedef enum {
  TRANSLATOR_RESP_FORMAT_TYPE_TEXT
} translator_response_format_type_t;

typedef struct translator_result_s {
  bool success;
  translator_response_format_type_t response_type;
  char *response;
  const char *err_msg;
} translator_result_t;

void translate(api_response_t *api_resp,
               translator_response_format_type_t resp_type,
               translator_result_t *tr);

#endif