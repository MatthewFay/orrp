#include "engine/container/container.h"
#include "engine/op/op.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void container_free_db_key_contents(eng_container_db_key_t *db_key) {
  (void)db_key;
}

// --- TEST SETUP & TEARDOWN ---

void setUp(void) {}

void tearDown(void) {
  // Clean up after each test
}

// --- TEST CASES ---

/**
 * @brief Tests successful creation of a string-based operation.
 */
void test_op_create_str_val_success(void) {
  // Arrange
  eng_container_db_key_t db_key = {
      .container_name = "test_container",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "test_key"}}};
  const char *val = "test_value";

  // Act
  op_t *op =
      op_create_str_val(&db_key, PUT, COND_PUT_IF_EXISTING_LESS_THAN, val);

  // Assert
  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(PUT, op->op_type);
  TEST_ASSERT_EQUAL(OP_STR_DATA, op->data_type);
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL_STRING(db_key.db_key.key.s, op->db_key.db_key.key.s);
  TEST_ASSERT_EQUAL_STRING(val, op->data.str_value);

  // Make sure the strings were duplicated, not just pointed to
  TEST_ASSERT_NOT_EQUAL(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_NOT_EQUAL(db_key.db_key.key.s, op->db_key.db_key.key.s);

  // Cleanup
  op_destroy(op);
}

/**
 * @brief Tests op_create_str_val with null arguments.
 */
void test_op_create_str_val_null_args(void) {
  eng_container_db_key_t db_key = {
      .container_name = "c", .db_key = {.type = DB_KEY_STRING, .key.s = "k"}};
  const char *val = "v";

  TEST_ASSERT_NULL(op_create_str_val(NULL, PUT, COND_PUT_NONE, val));
  TEST_ASSERT_NULL(op_create_str_val(&db_key, PUT, COND_PUT_NONE, NULL));
}

/**
 * @brief Tests successful creation of an integer-based operation.
 */
void test_op_create_int32_val_success(void) {
  // Arrange
  eng_container_db_key_t db_key = {
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 456;

  // Act
  op_t *op = op_create_int32_val(&db_key, INT_ADD_VALUE, COND_PUT_NONE, val);

  // Assert
  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(INT_ADD_VALUE, op->op_type);
  TEST_ASSERT_EQUAL(OP_INT32_DATA, op->data_type);
  TEST_ASSERT_EQUAL(COND_PUT_NONE, op->cond_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL(db_key.db_key.key.i, op->db_key.db_key.key.i);
  TEST_ASSERT_EQUAL_UINT32(val, op->data.int32_value);

  // Cleanup
  op_destroy(op);
}

void test_op_create_int32_val_with_zero_value(void) {
  // Arrange
  eng_container_db_key_t db_key = {
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 0;

  // Act
  op_t *op = op_create_int32_val(&db_key, PUT, COND_PUT_NONE, val);

  // Assert
  // This assertion will fail. The function returns NULL due to `if(!val)`.
  // It should create the op successfully.
  TEST_ASSERT_NOT_NULL(op);
  if (op) {
    TEST_ASSERT_EQUAL_UINT32(0, op->data.int32_value);
    op_destroy(op);
  }
}

void test_op_create_count_tag_increment(void) {
  // Arrange
  eng_container_db_key_t db_key = {
      .container_name = "tag_container",
      .db_key = {.type = DB_KEY_STRING, .key.s = "tag_key"}};
  const char *tag = "purchase:prod123";
  uint32_t entity_id = 999;
  uint32_t increment = 1;

  // Act
  op_t *op = op_create_count_tag_increment(&db_key, tag, entity_id, increment);

  TEST_ASSERT_NOT_NULL(op);

  if (op) {
    TEST_ASSERT_EQUAL(COUNT_TAG_INCREMENT, op->op_type);
    TEST_ASSERT_EQUAL(OP_COUNT_TAG_DATA, op->data_type);
    TEST_ASSERT_EQUAL_STRING(tag, op->data.count_tag_data->tag);
    TEST_ASSERT_EQUAL(entity_id, op->data.count_tag_data->entity_id);
    TEST_ASSERT_EQUAL(increment, op->data.count_tag_data->increment);
    op_destroy(op);
  }
}

/**
 * @brief Tests that op_destroy handles NULL input gracefully.
 */
void test_op_destroy_null(void) {
  // This test simply checks that calling destroy with NULL doesn't crash.
  op_destroy(NULL);
  TEST_PASS();
}

// --- MAIN RUNNER ---

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_op_create_str_val_success);
  RUN_TEST(test_op_create_str_val_null_args);
  RUN_TEST(test_op_create_int32_val_success);
  RUN_TEST(test_op_create_int32_val_with_zero_value);
  RUN_TEST(test_op_create_count_tag_increment);
  RUN_TEST(test_op_destroy_null);
  return UNITY_END();
}
