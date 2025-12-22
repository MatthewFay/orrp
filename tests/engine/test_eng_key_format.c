#include "engine/eng_key_format/eng_key_format.h"
#include "query/ast.h"
#include "unity.h"
#include <stdio.h>
#include <string.h>

// --- Setup/Teardown ---
void setUp(void) {
  // Put any setup code here
}

void tearDown(void) {
  // Put any teardown code here
}

// ====================================================================
// Custom Tag Into Tests
// ====================================================================

/**
 * Test case for successful conversion of a custom tag.
 * Expected format: "key:value"
 */
void test_custom_tag_into_success(void) {
  char buffer[64];
  ast_node_t *value = ast_create_string_literal_node("my_value");
  ast_node_t custom_tag = {.tag = {.custom_key = "my_key", .value = value}};

  TEST_ASSERT_TRUE(custom_tag_into(buffer, sizeof(buffer), &custom_tag));
  TEST_ASSERT_EQUAL_STRING("my_key:my_value", buffer);
  ast_free(value);
}

/**
 * Test case for buffer overflow (insufficient size).
 */
void test_custom_tag_into_buffer_too_small(void) {
  char buffer[10]; // Too small for "long_key:long_value"
  ast_node_t *value = ast_create_string_literal_node("long_value");

  ast_node_t custom_tag = {.tag = {.custom_key = "long_key", .value = value}};

  TEST_ASSERT_FALSE(custom_tag_into(buffer, sizeof(buffer), &custom_tag));
  ast_free(value);
}

/**
 * Test case for zero-size buffer.
 */
void test_custom_tag_into_zero_size(void) {
  char buffer[1];
  ast_node_t *value = ast_create_string_literal_node("v");

  ast_node_t custom_tag = {.tag = {.custom_key = "k", .value = value}};

  TEST_ASSERT_FALSE(custom_tag_into(buffer, 0, &custom_tag));
  ast_free(value);
}

// ====================================================================
// DB Key Into Tests
// ====================================================================

/**
 * Test case for a successful USER key with an INTEGER key type.
 * Expected format: "container_name|user_db_type|integer_key"
 */
void test_db_key_into_user_integer_success(void) {
  char buffer[64];
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USR,
      .usr_db_type = 42,
      .container_name = "users",
      .db_key = {.type = DB_KEY_U32, .key = {.i = 12345}}};

  TEST_ASSERT_TRUE(db_key_into(buffer, sizeof(buffer), &db_key));
  TEST_ASSERT_EQUAL_STRING("users|42|12345", buffer);
}

/**
 * Test case for a successful SYSTEM key with a STRING key type.
 */
void test_db_key_into_system_string_success(void) {
  char buffer[64];
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_SYS,
      .sys_db_type = 1,
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "config_item_name"}}};

  TEST_ASSERT_TRUE(db_key_into(buffer, sizeof(buffer), &db_key));
  TEST_ASSERT_EQUAL_STRING("system|1|config_item_name", buffer);
}

/**
 * Test case for buffer overflow with a large string key.
 */
void test_db_key_into_buffer_too_small(void) {
  char buffer[20]; // Should be too small for the full output
  char long_string[] = "this_is_a_very_long_key_string_to_force_overflow";
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USR,
      .usr_db_type = 5,
      .container_name = "data",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = long_string}}};

  TEST_ASSERT_FALSE(db_key_into(buffer, sizeof(buffer), &db_key));
}

/**
 * Test case for an unsupported/invalid key type.
 * The original code defaults to returning false if the type is not INTEGER or
 * STRING.
 */
void test_db_key_into_invalid_type(void) {
  char buffer[64];
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USR,
      .usr_db_type = 10,
      .container_name = "temp",
      .db_key = {.type = (db_key_type_t)999, // Invalid/unhandled type
                 .key = {.i = 100}}};

  TEST_ASSERT_FALSE(db_key_into(buffer, sizeof(buffer), &db_key));
}

// --- Main Test Runner ---
int main(void) {
  UNITY_BEGIN();

  // custom_tag_into tests
  RUN_TEST(test_custom_tag_into_success);
  RUN_TEST(test_custom_tag_into_buffer_too_small);
  RUN_TEST(test_custom_tag_into_zero_size);

  // db_key_into tests
  RUN_TEST(test_db_key_into_user_integer_success);
  RUN_TEST(test_db_key_into_system_string_success);
  RUN_TEST(test_db_key_into_buffer_too_small);
  RUN_TEST(test_db_key_into_invalid_type);

  return UNITY_END();
}