#include "networking/serializer.h"
#include "engine/api.h"
#include "mpack.h"
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void serializer_encode(const enum serializer_resp_status status,
                       const char *raw_data, const size_t raw_data_size,
                       serializer_result_t *sr) {
  const char *status_str = NULL;

  // Zero out the result structure.
  // This ensures sr->response is NULL and sr->response_size is 0.
  // This tells mpack_writer_init_growable to allocate a NEW buffer.
  memset(sr, 0, sizeof(serializer_result_t));

  switch (status) {
  case SER_RESP_OK:
    status_str = "OK";
    break;
  case SER_RESP_ERR:
    status_str = "ERR";
    break;
  default:
    status_str = "ERR";
    break;
  }

  mpack_writer_t writer;
  mpack_writer_init_growable(&writer, &sr->response, &sr->response_size);

  mpack_start_map(&writer, raw_data == NULL ? 1 : 2);

  mpack_write_cstr(&writer, "status");
  mpack_write_cstr(&writer, status_str);

  if (raw_data) {
    mpack_write_cstr(&writer, "data");
    // raw_data is already a valid MsgPack map
    // So we write it directly as an object.
    mpack_write_object_bytes(&writer, raw_data, raw_data_size);
  }

  mpack_finish_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "serializer_encode: Serializer error\n");
    if (sr->response) {
      free(sr->response);
    }
    sr->response = NULL;
    sr->response_size = 0;
    sr->success = false;
  } else {
    sr->success = true;
  }
}

void serializer_encode_err(const char *err_msg, serializer_result_t *sr) {
  // Initialize to NULL/0 so mpack allocates memory
  char *data = NULL;
  size_t data_size = 0;

  mpack_writer_t writer;
  mpack_writer_init_growable(&writer, &data, &data_size);
  mpack_start_map(&writer, 1);
  mpack_write_cstr(&writer, "err_msg");
  mpack_write_cstr(&writer, err_msg);
  mpack_finish_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "serializer_encode_err: Serializer error\n");
    sr->response = NULL;
    sr->response_size = 0;
    sr->success = false;
  } else {
    serializer_encode(SER_RESP_ERR, data, data_size, sr);
  }

  // Free the intermediate buffer (serializer_encode made a copy)
  free(data);
}

static void _encode_list_u32(const api_response_t *api_resp,
                             serializer_result_t *sr) {
  // Initialize to NULL/0 so mpack allocates memory
  char *data = NULL;
  size_t data_size = 0;

  const api_response_type_list_u32_t *list = &api_resp->payload.list_u32;

  mpack_writer_t writer;
  mpack_writer_init_growable(&writer, &data, &data_size);
  mpack_start_map(&writer, 1);
  mpack_write_cstr(&writer, "ids");
  mpack_start_array(&writer, list->count);

  for (uint32_t i = 0; i < list->count; i++) {
    mpack_write_u32(&writer, list->int32s[i]);
  }

  mpack_finish_array(&writer);
  mpack_finish_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "_encode_list_u32: Serializer error\n");
    sr->response = NULL;
    sr->response_size = 0;
    sr->success = false;
  } else {
    serializer_encode(SER_RESP_OK, data, data_size, sr);
  }

  free(data);
}

static void _encode_list_obj(const api_response_t *api_resp,
                             serializer_result_t *sr) {
  // Initialize to NULL/0 so mpack allocates memory
  char *data = NULL;
  size_t data_size = 0;

  const api_response_type_list_obj_t *list = &api_resp->payload.list_obj;

  mpack_writer_t writer;
  mpack_writer_init_growable(&writer, &data, &data_size);
  mpack_start_map(&writer, list->next_cursor ? 2 : 1);
  if (list->next_cursor) {
    mpack_write_cstr(&writer, "next_cursor");
    mpack_write_u32(&writer, list->next_cursor);
  }
  mpack_write_cstr(&writer, "objects");
  mpack_start_array(&writer, list->count);

  for (uint32_t i = 0; i < list->count; i++) {
    if (list->objects[i].data == NULL) {
      break;
    }
    // object is already a valid MsgPack map
    // So we write it directly as an object.
    mpack_write_object_bytes(&writer, list->objects[i].data,
                             list->objects[i].data_size);
  }

  mpack_finish_array(&writer);
  mpack_finish_map(&writer);

  if (mpack_writer_destroy(&writer) != mpack_ok) {
    fprintf(stderr, "_encode_list_obj: Serializer error\n");
    sr->response = NULL;
    sr->response_size = 0;
    sr->success = false;
  } else {
    serializer_encode(SER_RESP_OK, data, data_size, sr);
  }

  free(data);
}

void serializer_encode_api_resp(const api_response_t *api_resp,
                                serializer_result_t *sr) {
  if (!sr) {
    return;
  }
  if (!api_resp) {
    sr->err_msg = "Invalid args";
    sr->success = false;
    return;
  }

  if (!api_resp->is_ok) {
    sr->err_msg =
        api_resp->err_msg != NULL ? api_resp->err_msg : "Unknown error";
    return;
  }

  switch (api_resp->resp_type) {
  case API_RESP_TYPE_ACK:
    serializer_encode(SER_RESP_OK, NULL, 0, sr);
    break;
  case API_RESP_TYPE_LIST_U32:
    _encode_list_u32(api_resp, sr);
    break;
  case API_RESP_TYPE_LIST_OBJ:
    _encode_list_obj(api_resp, sr);
    break;
  default:
    sr->err_msg = "Unknown response type";
    break;
  }
}