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

void tearDown(void) {}

// --- BASIC LIFECYCLE TESTS ---

void test_op_create_destroy(void) {
  op_t *op = op_create(OP_PUT);
  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_PUT, op->op_type);
  TEST_ASSERT_EQUAL(OP_TARGET_NONE, op->target_type);
  TEST_ASSERT_EQUAL(OP_VALUE_NONE, op->value_type);
  op_destroy(op);
}

void test_op_destroy_null(void) {
  op_destroy(NULL);
  TEST_PASS();
}

// --- SETTER TESTS ---

void test_op_set_target(void) {
  eng_container_db_key_t db_key = {
      .container_name = "test_container",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "test_key"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &db_key);

  TEST_ASSERT_EQUAL(OP_TARGET_STRING, op->target_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL_STRING(db_key.db_key.key.s, op->db_key.db_key.key.s);

  op_destroy(op);
}

void test_op_set_condition(void) {
  op_t *op = op_create(OP_COND_PUT);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);

  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);

  op_destroy(op);
}

void test_op_set_value_int32(void) {
  op_t *op = op_create(OP_PUT);
  op_set_value_int32(op, 12345);

  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL_UINT32(12345, op->value.int32_value);

  op_destroy(op);
}

void test_op_set_value_int32_zero(void) {
  op_t *op = op_create(OP_PUT);
  op_set_value_int32(op, 0);

  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL_UINT32(0, op->value.int32_value);

  op_destroy(op);
}

void test_op_set_value_str(void) {
  op_t *op = op_create(OP_PUT);
  const char *test_val = "test_value";
  op_set_value_str(op, test_val);

  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL_STRING(test_val, op->value.str_value);
  TEST_ASSERT_NOT_EQUAL(test_val, op->value.str_value); // Should be duplicated

  op_destroy(op);
}

void test_op_set_value_str_overwrites_int(void) {
  op_t *op = op_create(OP_PUT);
  op_set_value_int32(op, 999);
  op_set_value_str(op, "new_string");

  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL_STRING("new_string", op->value.str_value);

  op_destroy(op);
}

void test_op_set_value_tag_counter(void) {
  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  const char *tag = "purchase:prod123";
  uint32_t entity_id = 999;
  uint32_t increment = 5;

  op_set_value_tag_counter(op, tag, entity_id, increment);

  TEST_ASSERT_EQUAL(OP_VALUE_TAG_COUNTER_DATA, op->value_type);
  TEST_ASSERT_NOT_NULL(op->value.tag_counter_data);
  TEST_ASSERT_EQUAL_STRING(tag, op->value.tag_counter_data->tag);
  TEST_ASSERT_NOT_EQUAL(
      tag, op->value.tag_counter_data->tag); // Should be duplicated
  TEST_ASSERT_EQUAL_UINT32(entity_id, op->value.tag_counter_data->entity_id);
  TEST_ASSERT_EQUAL_UINT32(increment, op->value.tag_counter_data->increment);

  op_destroy(op);
}

void test_op_set_value_tag_counter_overwrites_str(void) {
  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_value_str(op, "old_string");
  op_set_value_tag_counter(op, "tag:test", 123, 1);

  TEST_ASSERT_EQUAL(OP_VALUE_TAG_COUNTER_DATA, op->value_type);
  TEST_ASSERT_NOT_NULL(op->value.tag_counter_data);
  TEST_ASSERT_EQUAL_STRING("tag:test", op->value.tag_counter_data->tag);

  op_destroy(op);
}

// --- GETTER TESTS ---

void test_op_getters(void) {
  eng_container_db_key_t db_key = {
      .container_name = "container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 42}}};

  op_t *op = op_create(OP_COND_PUT);
  op_set_target(op, OP_TARGET_INT32, &db_key);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);
  op_set_value_int32(op, 100);

  TEST_ASSERT_EQUAL(OP_COND_PUT, op_get_type(op));
  TEST_ASSERT_EQUAL(OP_TARGET_INT32, op_get_target_type(op));
  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op_get_value_type(op));
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op_get_condition_type(op));
  TEST_ASSERT_EQUAL_UINT32(100, op_get_value_int32(op));

  const eng_container_db_key_t *key = op_get_db_key(op);
  TEST_ASSERT_NOT_NULL(key);
  TEST_ASSERT_EQUAL(42, key->db_key.key.i);

  op_destroy(op);
}

void test_op_get_value_wrong_type(void) {
  op_t *op = op_create(OP_PUT);
  op_set_value_int32(op, 123);

  TEST_ASSERT_NULL(op_get_value_str(op));
  TEST_ASSERT_NULL(op_get_value_tag_counter(op));

  op_destroy(op);
}

// --- HELPER FUNCTION TESTS ---

void test_op_create_str_val_success(void) {
  eng_container_db_key_t db_key = {
      .container_name = "test_container",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "test_key"}}};
  const char *val = "test_value";

  op_t *op = op_create_str_val(&db_key, OP_PUT, OP_TARGET_STRING,
                               COND_PUT_IF_EXISTING_LESS_THAN, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_PUT, op->op_type);
  TEST_ASSERT_EQUAL(OP_TARGET_STRING, op->target_type);
  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL_STRING(db_key.db_key.key.s, op->db_key.db_key.key.s);
  TEST_ASSERT_EQUAL_STRING(val, op->value.str_value);
  TEST_ASSERT_NOT_EQUAL(val, op->value.str_value); // Should be duplicated

  op_destroy(op);
}

void test_op_create_str_val_null_args(void) {
  eng_container_db_key_t db_key = {
      .container_name = "c",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "k"}}};
  const char *val = "v";

  TEST_ASSERT_NULL(
      op_create_str_val(NULL, OP_PUT, OP_TARGET_STRING, COND_PUT_NONE, val));
  TEST_ASSERT_NULL(op_create_str_val(&db_key, OP_PUT, OP_TARGET_STRING,
                                     COND_PUT_NONE, NULL));
}

void test_op_create_int32_val_success(void) {
  eng_container_db_key_t db_key = {
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 456;

  op_t *op = op_create_int32_val(&db_key, OP_ADD_VALUE, OP_TARGET_INT32,
                                 COND_PUT_NONE, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_ADD_VALUE, op->op_type);
  TEST_ASSERT_EQUAL(OP_TARGET_INT32, op->target_type);
  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL(COND_PUT_NONE, op->cond_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL(db_key.db_key.key.i, op->db_key.db_key.key.i);
  TEST_ASSERT_EQUAL_UINT32(val, op->value.int32_value);

  op_destroy(op);
}

void test_op_create_int32_val_with_zero_value(void) {
  eng_container_db_key_t db_key = {
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 0;

  op_t *op =
      op_create_int32_val(&db_key, OP_PUT, OP_TARGET_INT32, COND_PUT_NONE, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL_UINT32(0, op->value.int32_value);

  op_destroy(op);
}

void test_op_create_tag_counter_increment(void) {
  eng_container_db_key_t db_key = {
      .container_name = "tag_container",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "tag_key"}}};
  const char *tag = "purchase:prod123";
  uint32_t entity_id = 999;
  uint32_t increment = 1;

  op_t *op =
      op_create_tag_counter_increment(&db_key, tag, entity_id, increment);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_INCREMENT_TAG_COUNTER, op->op_type);
  TEST_ASSERT_EQUAL(OP_TARGET_TAG_COUNTER, op->target_type);
  TEST_ASSERT_EQUAL(OP_VALUE_TAG_COUNTER_DATA, op->value_type);
  TEST_ASSERT_EQUAL_STRING(tag, op->value.tag_counter_data->tag);
  TEST_ASSERT_EQUAL(entity_id, op->value.tag_counter_data->entity_id);
  TEST_ASSERT_EQUAL(increment, op->value.tag_counter_data->increment);

  op_destroy(op);
}

// --- MAIN RUNNER ---

int main(void) {
  UNITY_BEGIN();

  // Lifecycle
  RUN_TEST(test_op_create_destroy);
  RUN_TEST(test_op_destroy_null);

  // Setters
  RUN_TEST(test_op_set_target);
  RUN_TEST(test_op_set_condition);
  RUN_TEST(test_op_set_value_int32);
  RUN_TEST(test_op_set_value_int32_zero);
  RUN_TEST(test_op_set_value_str);
  RUN_TEST(test_op_set_value_str_overwrites_int);
  RUN_TEST(test_op_set_value_tag_counter);
  RUN_TEST(test_op_set_value_tag_counter_overwrites_str);

  // Getters
  RUN_TEST(test_op_getters);
  RUN_TEST(test_op_get_value_wrong_type);

  // Helper functions
  RUN_TEST(test_op_create_str_val_success);
  RUN_TEST(test_op_create_str_val_null_args);
  RUN_TEST(test_op_create_int32_val_success);
  RUN_TEST(test_op_create_int32_val_with_zero_value);
  RUN_TEST(test_op_create_tag_counter_increment);

  return UNITY_END();
}