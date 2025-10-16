#include "engine/consumer/consumer_validate.h"
#include "engine/op/op.h"
#include "unity.h"
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

void container_free_db_key_contents(eng_container_db_key_t *db_key) {
  (void)db_key;
}

// --- NULL AND BASIC VALIDATION ---

void test_validate_null_op(void) {
  TEST_ASSERT_FALSE(consumer_validate_op(NULL));
}

void test_validate_uninitialized_op(void) {
  op_t *op = op_create(OP_PUT);
  // No target or value set
  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

// --- TARGET TYPE VALIDATION ---

void test_validate_missing_target(void) {
  op_t *op = op_create(OP_PUT);
  op_set_value_int32(op, 123);
  // No target set
  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_invalid_target_for_increment(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_target(op, OP_TARGET_INT32, &key); // Wrong target type
  op_set_value_tag_counter(op, "tag:test", 1, 1);

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

// --- VALUE TYPE VALIDATION ---

void test_validate_missing_value(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  // No value set
  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_empty_string_value(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "");

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_wrong_value_type_for_increment(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_target(op, OP_TARGET_TAG_COUNTER, &key);
  op_set_value_int32(op, 1); // Wrong value type

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

// --- DB KEY VALIDATION ---

void test_validate_missing_container_name(void) {
  eng_container_db_key_t key = {
      .container_name = NULL,
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "value");

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_empty_container_name(void) {
  eng_container_db_key_t key = {
      .container_name = "",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "value");

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_missing_string_key(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = NULL}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "value");

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_empty_string_key(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = ""}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "value");

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

// --- CONDITIONAL PUT VALIDATION ---

void test_validate_cond_put_missing_condition(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 1}}};

  op_t *op = op_create(OP_COND_PUT);
  op_set_target(op, OP_TARGET_INT32, &key);
  op_set_value_int32(op, 100);
  // No condition set

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_cond_put_success(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 1}}};

  op_t *op = op_create(OP_COND_PUT);
  op_set_target(op, OP_TARGET_INT32, &key);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);
  op_set_value_int32(op, 100);

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

// --- ADD_VALUE VALIDATION ---

void test_validate_add_value_wrong_target(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_ADD_VALUE);
  op_set_target(op, OP_TARGET_STRING, &key); // Should be INT32 or BITMAP
  op_set_value_int32(op, 5);

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_add_value_wrong_value_type(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 1}}};

  op_t *op = op_create(OP_ADD_VALUE);
  op_set_target(op, OP_TARGET_INT32, &key);
  op_set_value_str(op, "5"); // Should be INT32

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_add_value_success(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 1}}};

  op_t *op = op_create(OP_ADD_VALUE);
  op_set_target(op, OP_TARGET_INT32, &key);
  op_set_value_int32(op, 5);

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

// --- TAG COUNTER VALIDATION ---

void test_validate_tag_counter_missing_tag(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_target(op, OP_TARGET_TAG_COUNTER, &key);
  op_set_value_tag_counter(op, "", 1, 1); // Empty tag

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_tag_counter_zero_increment(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_target(op, OP_TARGET_TAG_COUNTER, &key);
  op_set_value_tag_counter(op, "tag:test", 1, 0); // Zero increment

  TEST_ASSERT_FALSE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_tag_counter_success(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "key"}}};

  op_t *op = op_create(OP_INCREMENT_TAG_COUNTER);
  op_set_target(op, OP_TARGET_TAG_COUNTER, &key);
  op_set_value_tag_counter(op, "purchase:prod123", 999, 1);

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

// --- VALID OPERATIONS ---

void test_validate_put_int_success(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 42}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_INT32, &key);
  op_set_value_int32(op, 123);

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_put_string_success(void) {
  eng_container_db_key_t key = {
      .container_name = "test",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "mykey"}}};

  op_t *op = op_create(OP_PUT);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "myvalue");

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

void test_validate_cache_success(void) {
  eng_container_db_key_t key = {
      .container_name = "cache",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "cachekey"}}};

  op_t *op = op_create(OP_CACHE);
  op_set_target(op, OP_TARGET_STRING, &key);
  op_set_value_str(op, "cached_value");

  TEST_ASSERT_TRUE(consumer_validate_op(op));
  op_destroy(op);
}

// --- MAIN RUNNER ---

int main(void) {
  UNITY_BEGIN();

  // Basic validation
  RUN_TEST(test_validate_null_op);
  RUN_TEST(test_validate_uninitialized_op);

  // Target validation
  RUN_TEST(test_validate_missing_target);
  RUN_TEST(test_validate_invalid_target_for_increment);

  // Value validation
  RUN_TEST(test_validate_missing_value);
  RUN_TEST(test_validate_empty_string_value);
  RUN_TEST(test_validate_wrong_value_type_for_increment);

  // DB key validation
  RUN_TEST(test_validate_missing_container_name);
  RUN_TEST(test_validate_empty_container_name);
  RUN_TEST(test_validate_missing_string_key);
  RUN_TEST(test_validate_empty_string_key);

  // Conditional put validation
  RUN_TEST(test_validate_cond_put_missing_condition);
  RUN_TEST(test_validate_cond_put_success);

  // Add value validation
  RUN_TEST(test_validate_add_value_wrong_target);
  RUN_TEST(test_validate_add_value_wrong_value_type);
  RUN_TEST(test_validate_add_value_success);

  // Tag counter validation
  RUN_TEST(test_validate_tag_counter_missing_tag);
  RUN_TEST(test_validate_tag_counter_zero_increment);
  RUN_TEST(test_validate_tag_counter_success);

  // Valid operations
  RUN_TEST(test_validate_put_int_success);
  RUN_TEST(test_validate_put_string_success);
  RUN_TEST(test_validate_cache_success);

  return UNITY_END();
}