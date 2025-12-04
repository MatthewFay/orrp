#include "engine/consumer/consumer_schema.h"
#include "engine/container/container_types.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}

void tearDown(void) {}

// ============================================================================
// Test Group: Value Type Mapping
// ============================================================================

void test_get_value_type_system_dbs(void) {
  eng_container_db_key_t key = {0};
  key.dc_type = CONTAINER_TYPE_SYS;

  // Entity ID (String) -> Internal ID (Int)
  key.sys_db_type = SYS_DB_ENT_ID_TO_INT;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_INT32,
                    consumer_schema_get_cache_value_type(&key));

  // Internal ID (Int) -> Entity ID (String)
  key.sys_db_type = SYS_DB_INT_TO_ENT_ID;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_STR,
                    consumer_schema_get_cache_value_type(&key));

  // Metadata
  key.sys_db_type = SYS_DB_METADATA;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_INT32,
                    consumer_schema_get_cache_value_type(&key));
}

void test_get_value_type_user_dbs(void) {
  eng_container_db_key_t key = {0};
  key.dc_type = CONTAINER_TYPE_USR;

  // Inverted Index: Tag -> Bitmap
  key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_BM,
                    consumer_schema_get_cache_value_type(&key));

  // Event -> Entity: Int -> Int
  key.usr_db_type = USER_DB_EVENT_TO_ENTITY;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_INT32,
                    consumer_schema_get_cache_value_type(&key));

  // Counters: Key -> Int
  key.usr_db_type = USER_DB_COUNTER_STORE;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_INT32,
                    consumer_schema_get_cache_value_type(&key));

  // Count Index: Int -> Bitmap
  key.usr_db_type = USER_DB_COUNT_INDEX;
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_BM,
                    consumer_schema_get_cache_value_type(&key));
}

void test_get_value_type_unknown_should_return_unknown(void) {
  eng_container_db_key_t key = {0};
  key.dc_type = 99; // Invalid type
  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_UNKNOWN,
                    consumer_schema_get_cache_value_type(&key));

  TEST_ASSERT_EQUAL(CONSUMER_CACHE_ENTRY_VAL_UNKNOWN,
                    consumer_schema_get_cache_value_type(NULL));
}

// ============================================================================
// Test Group: Operation Validation (PUT)
// ============================================================================

void test_validate_put_to_bitmap_db_valid(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_PUT;
  op.value_type = OP_VALUE_BITMAP;

  // Target a Bitmap DB
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
  TEST_ASSERT_NULL(res.error_msg);
}

void test_validate_put_to_bitmap_db_invalid_type(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_PUT;
  op.value_type = OP_VALUE_INT32; // Wrong value type for PUT

  // Target a Bitmap DB
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("PUT to bitmap database requires bitmap value",
                           res.error_msg);
}

void test_validate_put_to_int_db_valid(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_PUT;
  op.value_type = OP_VALUE_INT32;

  // Target an Int DB
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USER_DB_COUNTER_STORE;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
}

// ============================================================================
// Test Group: Operation Validation (ADD)
// ============================================================================

void test_validate_add_to_bitmap_db_valid(void) {
  // Adding to a bitmap set means adding an Integer ID to it
  op_t op = {0};
  op.op_type = OP_TYPE_ADD_VALUE;
  op.value_type = OP_VALUE_INT32;

  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
}

void test_validate_add_to_bitmap_db_invalid_value(void) {
  // Cannot "ADD" a bitmap to a bitmap (that would be merge/union, not add
  // value)
  op_t op = {0};
  op.op_type = OP_TYPE_ADD_VALUE;
  op.value_type = OP_VALUE_BITMAP;

  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("ADD to bitmap requires int32 value", res.error_msg);
}

void test_validate_add_to_int_db_valid(void) {
  // Incrementing a counter
  op_t op = {0};
  op.op_type = OP_TYPE_ADD_VALUE;
  op.value_type = OP_VALUE_INT32;

  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USER_DB_COUNTER_STORE;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
}

void test_validate_add_to_string_db_invalid(void) {
  // Strings don't support arithmetic add
  op_t op = {0};
  op.op_type = OP_TYPE_ADD_VALUE;
  op.value_type = OP_VALUE_STRING;

  op.db_key.dc_type = CONTAINER_TYPE_SYS;
  op.db_key.sys_db_type = SYS_DB_INT_TO_ENT_ID;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("ADD operation not supported for string databases",
                           res.error_msg);
}

// ============================================================================
// Test Group: Operation Validation (COND_PUT)
// ============================================================================

void test_validate_cond_put_int_db_valid(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_COND_PUT;
  op.cond_type = 999;
  op.value_type = OP_VALUE_INT32;

  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USER_DB_COUNTER_STORE;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
}

void test_validate_cond_put_bitmap_db_invalid(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_COND_PUT;
  op.cond_type = 999;
  op.value_type = OP_VALUE_INT32;

  // Bitmaps don't support conditional put logic in this schema
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("Conditional put only supported for int32 databases",
                           res.error_msg);
}

void test_validate_cond_put_missing_condition(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_COND_PUT;
  op.cond_type = COND_PUT_NONE; // ERROR
  op.value_type = OP_VALUE_INT32;

  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USER_DB_COUNTER_STORE;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("Conditional put missing condition type",
                           res.error_msg);
}

// ============================================================================
// Test Group: Operation Validation (CACHE)
// ============================================================================

void test_validate_cache_op_valid(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_CACHE;
  op.value_type = OP_VALUE_BITMAP;

  // Cache loading a bitmap
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_TRUE(res.valid);
}

void test_validate_cache_op_mismatch(void) {
  op_t op = {0};
  op.op_type = OP_TYPE_CACHE;
  op.value_type = OP_VALUE_INT32; // Mismatch

  // Target is Bitmap
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  schema_validation_result_t res = consumer_schema_validate_op(&op);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("CACHE to bitmap database requires bitmap value",
                           res.error_msg);
}

// ============================================================================
// Test Group: Message Structure Validation
// ============================================================================

void test_validate_msg_null_checks(void) {
  schema_validation_result_t res = consumer_schema_validate_msg(NULL);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("Message is NULL", res.error_msg);

  op_queue_msg_t msg = {0};
  res = consumer_schema_validate_msg(&msg);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("Message operation is NULL", res.error_msg);

  op_t op = {0};
  msg.op = &op;
  res = consumer_schema_validate_msg(&msg);
  TEST_ASSERT_FALSE(res.valid);
  TEST_ASSERT_EQUAL_STRING("Message serialized key is NULL", res.error_msg);
}

void test_validate_msg_valid_delegates_to_op(void) {
  // Setup a valid op
  op_t op = {0};
  op.op_type = OP_TYPE_PUT;
  op.value_type = OP_VALUE_INT32;
  op.db_key.dc_type = CONTAINER_TYPE_USR;
  op.db_key.usr_db_type = USER_DB_COUNTER_STORE;

  // Setup a valid msg
  op_queue_msg_t msg = {0};
  msg.op = &op;
  msg.ser_db_key = "some_valid_key";

  schema_validation_result_t res = consumer_schema_validate_msg(&msg);
  TEST_ASSERT_TRUE(res.valid);
}

int main(void) {
  UNITY_BEGIN();

  // Mapping
  RUN_TEST(test_get_value_type_system_dbs);
  RUN_TEST(test_get_value_type_user_dbs);
  RUN_TEST(test_get_value_type_unknown_should_return_unknown);

  // PUT
  RUN_TEST(test_validate_put_to_bitmap_db_valid);
  RUN_TEST(test_validate_put_to_bitmap_db_invalid_type);
  RUN_TEST(test_validate_put_to_int_db_valid);

  // ADD
  RUN_TEST(test_validate_add_to_bitmap_db_valid);
  RUN_TEST(test_validate_add_to_bitmap_db_invalid_value);
  RUN_TEST(test_validate_add_to_int_db_valid);
  RUN_TEST(test_validate_add_to_string_db_invalid);

  // COND PUT
  RUN_TEST(test_validate_cond_put_int_db_valid);
  RUN_TEST(test_validate_cond_put_bitmap_db_invalid);
  RUN_TEST(test_validate_cond_put_missing_condition);

  // CACHE
  RUN_TEST(test_validate_cache_op_valid);
  RUN_TEST(test_validate_cache_op_mismatch);

  // MSG STRUCT
  RUN_TEST(test_validate_msg_null_checks);
  RUN_TEST(test_validate_msg_valid_delegates_to_op);

  return UNITY_END();
}