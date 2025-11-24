#include "engine/api.h"
#include "networking/translator.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Global result struct used in tests
static translator_result_t tr;

void setUp(void) {
  // Zero out result before every test
  memset(&tr, 0, sizeof(translator_result_t));
}

void tearDown(void) {
  // If the translator allocated memory, we must free it to prevent leaks in the
  // test runner
  if (tr.response) {
    free(tr.response);
    tr.response = NULL;
  }
}

// --- Test Group: Input Validation ---

void test_translate_null_api_response_should_fail(void) {
  translate(NULL, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_FALSE(tr.success);
  TEST_ASSERT_EQUAL_STRING("Invalid args", tr.err_msg);
  TEST_ASSERT_NULL(tr.response);
}

void test_translate_api_error_flag_should_fail(void) {
  api_response_t api_resp = {0};
  api_resp.is_ok = false; // Simulating Engine failure
  api_resp.err_msg = "Engine blew up";

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_FALSE(tr.success);
  TEST_ASSERT_EQUAL_STRING("API response is_ok=false", tr.err_msg);
  TEST_ASSERT_NULL(tr.response);
}

void test_translate_unknown_format_type_should_fail(void) {
  api_response_t api_resp = {0};
  api_resp.is_ok = true;

  // Casting integer to enum to simulate invalid input
  translate(&api_resp, (translator_response_format_type_t)999, &tr);

  TEST_ASSERT_FALSE(tr.success);
  TEST_ASSERT_EQUAL_STRING("Unknown format type", tr.err_msg);
  TEST_ASSERT_NULL(tr.response);
}

// --- Test Group: Formatting Logic (Happy Paths) ---

void test_translate_ack_should_return_ok_newline(void) {
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  api_resp.resp_type = API_RESP_TYPE_ACK;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_TRUE(tr.success);
  TEST_ASSERT_NOT_NULL(tr.response);
  TEST_ASSERT_EQUAL_STRING("OK\n", tr.response);
}

void test_translate_list_empty_should_return_empty_string(void) {
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  api_resp.resp_type = API_RESP_TYPE_LIST_U32;
  api_resp.payload.list_u32.count = 0;
  api_resp.payload.list_u32.int32s = NULL;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_TRUE(tr.success);
  TEST_ASSERT_NOT_NULL(tr.response);
  // Should be an empty string (allocated 1 byte, set to 0)
  TEST_ASSERT_EQUAL_STRING("", tr.response);
}

void test_translate_list_single_item_should_format_correctly(void) {
  uint32_t data[] = {42};
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  api_resp.resp_type = API_RESP_TYPE_LIST_U32;
  api_resp.payload.list_u32.count = 1;
  api_resp.payload.list_u32.int32s = data;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_TRUE(tr.success);
  TEST_ASSERT_NOT_NULL(tr.response);
  // Implementation adds a newline at the end
  TEST_ASSERT_EQUAL_STRING("42\n", tr.response);
}

void test_translate_list_multiple_items_should_comma_separate(void) {
  uint32_t data[] = {100, 200, 300};
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  api_resp.resp_type = API_RESP_TYPE_LIST_U32;
  api_resp.payload.list_u32.count = 3;
  api_resp.payload.list_u32.int32s = data;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_TRUE(tr.success);
  TEST_ASSERT_NOT_NULL(tr.response);
  TEST_ASSERT_EQUAL_STRING("100,200,300\n", tr.response);
}

void test_translate_large_numbers_should_format_correctly(void) {
  uint32_t data[] = {4294967295}; // UINT32_MAX
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  api_resp.resp_type = API_RESP_TYPE_LIST_U32;
  api_resp.payload.list_u32.count = 1;
  api_resp.payload.list_u32.int32s = data;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_TRUE(tr.success);
  TEST_ASSERT_NOT_NULL(tr.response);
  TEST_ASSERT_EQUAL_STRING("4294967295\n", tr.response);
}

// --- Test Group: Logic Mismatches ---

void test_translate_wrong_resp_type_for_text_fmt_should_fail(void) {
  api_response_t api_resp = {0};
  api_resp.is_ok = true;
  // We request TEXT format, but provide a type that isn't handled by
  // _handle_basic_fmt explicitly
  api_resp.resp_type = 99999;

  translate(&api_resp, TRANSLATOR_RESP_FORMAT_TYPE_TEXT, &tr);

  TEST_ASSERT_FALSE(tr.success);
  TEST_ASSERT_EQUAL_STRING("Unexpected response type", tr.err_msg);
  TEST_ASSERT_NULL(tr.response);
}

int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_translate_null_api_response_should_fail);
  RUN_TEST(test_translate_api_error_flag_should_fail);
  RUN_TEST(test_translate_unknown_format_type_should_fail);

  RUN_TEST(test_translate_ack_should_return_ok_newline);
  RUN_TEST(test_translate_list_empty_should_return_empty_string);
  RUN_TEST(test_translate_list_single_item_should_format_correctly);
  RUN_TEST(test_translate_list_multiple_items_should_comma_separate);
  RUN_TEST(test_translate_large_numbers_should_format_correctly);

  RUN_TEST(test_translate_wrong_resp_type_for_text_fmt_should_fail);

  return UNITY_END();
}