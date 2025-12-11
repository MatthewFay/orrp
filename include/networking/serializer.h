#ifndef serializer_h
#define serializer_h

#include "engine/api.h"
#include <stdbool.h>
#include <stddef.h>

typedef struct serializer_result_s {
  bool success;
  char *response;
  size_t response_size;
  const char *err_msg;
} serializer_result_t;

enum serializer_resp_status { SER_RESP_OK, SER_RESP_ERR };

// Does NOT take ownership of `raw_data` - Caller must free.
void serializer_encode(const enum serializer_resp_status status,
                       const char *raw_data, const size_t raw_data_size,
                       serializer_result_t *sr);

void serializer_encode_err(const char *err_msg, serializer_result_t *sr);

void serializer_encode_api_resp(const api_response_t *api_resp,
                                serializer_result_t *sr);

#endif