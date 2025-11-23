#include "networking/translator.h"
#include "engine/api.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void _handle_basic_fmt(api_response_t *api_resp,
                              translator_result_t *tr) {

  if (api_resp->resp_type == API_RESP_TYPE_ACK) {
    tr->response = strdup("OK\n");
    tr->success = true;
    return;
  }
  if (api_resp->resp_type != API_RESP_TYPE_LIST_U32) {
    tr->err_msg = "Unexpected response type";
    return;
  }

  api_response_type_list_u32_t *list = &api_resp->payload.list_u32;

  if (list->count == 0) {
    // set response to empty string ""
    tr->response = calloc(1, 1);
    if (!tr->response) {
      tr->err_msg = "Out of memory";
      return;
    }
    tr->success = true;
    return;
  }

  // uint32_t (10 chars max) + comma
  size_t buf_size = ((uint32_t)list->count * 11) + 1;
  tr->response = malloc(buf_size);

  if (!tr->response) {
    tr->err_msg = "Out of memory";
    return;
  }

  char *ptr = tr->response;
  for (uint32_t i = 0; i < list->count; i++) {
    int written = sprintf(ptr, "%u", list->int32s[i]);
    ptr += written;
    if (i < list->count - 1) {
      *ptr++ = ',';
    }
  }
  *ptr = '\0';

  tr->success = true;
}

void translate(api_response_t *api_resp,
               translator_response_format_type_t resp_type,
               translator_result_t *tr) {
  memset(tr, 0, sizeof(translator_result_t));
  tr->response_type = resp_type;

  if (!api_resp) {
    tr->err_msg = "Invalid args";
    return;
  }

  if (!api_resp->is_ok) {
    tr->err_msg = "API response is_ok=false";
    return;
  }

  switch (resp_type) {
  case TRANSLATOR_RESP_FORMAT_TYPE_TEXT:
    _handle_basic_fmt(api_resp, tr);
    break;
  default:
    tr->err_msg = "Unknown format type";
    break;
  }
}
