#include "engine/api.h"
#include "mpack.h"
#include "networking/serializer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

static serializer_result_t sr;

void setUp(void) { memset(&sr, 0, sizeof(serializer_result_t)); }

void tearDown(void) {
  if (sr.response) {
    free(sr.response);
  }
}

// --- Helpers to Validate MsgPack Output ---

// Decodes the buffer and asserts top-level { "status": "OK" }
void assert_msgpack_is_ack(char *data, size_t size) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);

  TEST_ASSERT_EQUAL(mpack_ok, mpack_tree_error(&tree));

  mpack_node_t root = mpack_tree_root(&tree);
  TEST_ASSERT_TRUE(mpack_node_map_contains_cstr(root, "status"));

  mpack_node_t status_node = mpack_node_map_cstr(root, "status");
  char *status_str = mpack_node_cstr_alloc(status_node, 10);
  TEST_ASSERT_EQUAL_STRING("OK", status_str);

  free(status_str);
  TEST_ASSERT_TRUE(mpack_tree_destroy(&tree) == mpack_ok);
}

// Decodes buffer and asserts { "status": "OK", "data": { "ids":
// [expected_ids...] } }
void assert_msgpack_has_ids(char *data, size_t size, uint32_t *expected,
                            size_t count) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  TEST_ASSERT_EQUAL(mpack_ok, mpack_tree_error(&tree));

  mpack_node_t root = mpack_tree_root(&tree);

  // Check "status" == "OK"
  mpack_node_t status = mpack_node_map_cstr(root, "status");
  char *s = mpack_node_cstr_alloc(status, 10);
  TEST_ASSERT_EQUAL_STRING("OK", s);
  free(s);

  // Check "data" exists
  TEST_ASSERT_TRUE(mpack_node_map_contains_cstr(root, "data"));
  mpack_node_t data_node = mpack_node_map_cstr(root, "data");

  // Check "ids" inside data
  TEST_ASSERT_TRUE(mpack_node_map_contains_cstr(data_node, "ids"));
  mpack_node_t ids_node = mpack_node_map_cstr(data_node, "ids");

  // Check Array Size
  TEST_ASSERT_EQUAL_UINT32(count, mpack_node_array_length(ids_node));

  // Check Array Content
  for (size_t i = 0; i < count; i++) {
    mpack_node_t item = mpack_node_array_at(ids_node, i);
    TEST_ASSERT_EQUAL_UINT32(expected[i], mpack_node_u32(item));
  }

  TEST_ASSERT_TRUE(mpack_tree_destroy(&tree) == mpack_ok);
}

// Decodes buffer and asserts { "status": "ERR", "data": { "err_msg": "..." } }
void assert_msgpack_is_error(char *data, size_t size,
                             const char *expected_msg) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  TEST_ASSERT_EQUAL(mpack_ok, mpack_tree_error(&tree));

  mpack_node_t root = mpack_tree_root(&tree);

  // Check "status" == "ERR"
  mpack_node_t status = mpack_node_map_cstr(root, "status");
  char *s = mpack_node_cstr_alloc(status, 10);
  TEST_ASSERT_EQUAL_STRING("ERR", s);
  free(s);

  // Check "data" -> "err_msg"
  mpack_node_t data_node = mpack_node_map_cstr(root, "data");
  mpack_node_t msg_node = mpack_node_map_cstr(data_node, "err_msg");

  char *msg = mpack_node_cstr_alloc(msg_node, 100);
  TEST_ASSERT_EQUAL_STRING(expected_msg, msg);
  free(msg);

  TEST_ASSERT_TRUE(mpack_tree_destroy(&tree) == mpack_ok);
}

// --- Test Suites ---

// 1. Test the Low-Level Serializer Logic
void test_SerializerEncode_StatusOnly_ShouldReturnStatusMap(void) {
  // Act
  serializer_encode(SER_RESP_OK, NULL, 0, &sr);

  // Assert
  TEST_ASSERT_TRUE(sr.success);
  TEST_ASSERT_NOT_NULL(sr.response);
  TEST_ASSERT_GREATER_THAN(0, sr.response_size);
  assert_msgpack_is_ack(sr.response, sr.response_size);
}

void test_SerializerEncodeErr_ShouldWrapMessage(void) {
  // Act
  serializer_encode_err("Database Exploded", &sr);

  // Assert
  TEST_ASSERT_TRUE(sr.success); // It successfully encoded the error
  TEST_ASSERT_NOT_NULL(sr.response);
  assert_msgpack_is_error(sr.response, sr.response_size, "Database Exploded");
}

// 2. Test API Response: ACKs
void test_ApiResp_Ack_ShouldProduceSimpleOk(void) {
  // Arrange
  api_response_t resp;
  resp.is_ok = true;
  resp.resp_type = API_RESP_TYPE_ACK;

  // Act
  serializer_encode_api_resp(&resp, &sr);

  // Assert
  TEST_ASSERT_TRUE(sr.success);
  TEST_ASSERT_NOT_NULL(sr.response);
  assert_msgpack_is_ack(sr.response, sr.response_size);
}

// 3. Test API Response: List of U32
void test_ApiResp_ListU32_ShouldStitchNestedData(void) {
  // Arrange
  uint32_t ids[] = {101, 202, 303, 9999};

  api_response_t resp;
  resp.is_ok = true;
  resp.resp_type = API_RESP_TYPE_LIST_U32;
  resp.payload.list_u32.count = 4;
  resp.payload.list_u32.int32s = ids;

  // Act
  serializer_encode_api_resp(&resp, &sr);

  // Assert
  TEST_ASSERT_TRUE(sr.success);
  TEST_ASSERT_NOT_NULL(sr.response);

  // Uses mpack node reader to verify the nesting structure
  assert_msgpack_has_ids(sr.response, sr.response_size, ids, 4);
}

void test_ApiResp_ListU32_EmptyList_ShouldReturnEmptyArray(void) {
  // Arrange
  api_response_t resp;
  resp.is_ok = true;
  resp.resp_type = API_RESP_TYPE_LIST_U32;
  resp.payload.list_u32.count = 0;
  resp.payload.list_u32.int32s = NULL;

  // Act
  serializer_encode_api_resp(&resp, &sr);

  // Assert
  TEST_ASSERT_TRUE(sr.success);

  // Verify manually here for emptiness
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, sr.response, sr.response_size);
  mpack_tree_parse(&tree);

  mpack_node_t ids = mpack_node_map_cstr(
      mpack_node_map_cstr(mpack_tree_root(&tree), "data"), "ids");
  TEST_ASSERT_EQUAL_UINT32(0, mpack_node_array_length(ids));
  mpack_tree_destroy(&tree);
}

// 4. Test API Response: Errors (Logic Check)
void test_ApiResp_Error_ShouldSetStructError_NotGenerateBytes(void) {
  // Note: based on your current implementation of serializer_encode_api_resp,
  // if !is_ok, it does NOT call serializer_encode_err. It just sets
  // sr->err_msg.

  // Arrange
  api_response_t resp;
  resp.is_ok = false;
  resp.err_msg = "Parser failed";

  // Act
  serializer_encode_api_resp(&resp, &sr);

  // Assert
  TEST_ASSERT_EQUAL_STRING("Parser failed", sr.err_msg);
  TEST_ASSERT_NULL(
      sr.response); // No bytes generated in this specific code path
  TEST_ASSERT_EQUAL(0, sr.response_size);
}

void test_ApiResp_InvalidInput_ShouldFailGracefully(void) {
  // Act
  serializer_encode_api_resp(NULL, &sr);

  // Assert
  TEST_ASSERT_FALSE(sr.success);
  TEST_ASSERT_EQUAL_STRING("Invalid args", sr.err_msg);
}

int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_SerializerEncode_StatusOnly_ShouldReturnStatusMap);
  RUN_TEST(test_SerializerEncodeErr_ShouldWrapMessage);
  RUN_TEST(test_ApiResp_Ack_ShouldProduceSimpleOk);
  RUN_TEST(test_ApiResp_ListU32_ShouldStitchNestedData);
  RUN_TEST(test_ApiResp_ListU32_EmptyList_ShouldReturnEmptyArray);
  RUN_TEST(test_ApiResp_Error_ShouldSetStructError_NotGenerateBytes);
  RUN_TEST(test_ApiResp_InvalidInput_ShouldFailGracefully);

  return UNITY_END();
}