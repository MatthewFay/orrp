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
  op_t *op = op_create(OP_TYPE_PUT);
  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_TYPE_PUT, op->op_type);
  TEST_ASSERT_EQUAL(OP_VALUE_NONE, op->value_type);
  TEST_ASSERT_EQUAL(COND_PUT_NONE, op->cond_type);
  op_destroy(op);
}

void test_op_destroy_null(void) {
  op_destroy(NULL);
  TEST_PASS();
}

void test_op_create_all_types(void) {
  op_t *op_put = op_create(OP_TYPE_PUT);
  TEST_ASSERT_EQUAL(OP_TYPE_PUT, op_put->op_type);
  op_destroy(op_put);

  op_t *op_add = op_create(OP_TYPE_ADD_VALUE);
  TEST_ASSERT_EQUAL(OP_TYPE_ADD_VALUE, op_add->op_type);
  op_destroy(op_add);

  op_t *op_cond = op_create(OP_TYPE_COND_PUT);
  TEST_ASSERT_EQUAL(OP_TYPE_COND_PUT, op_cond->op_type);
  op_destroy(op_cond);

  op_t *op_cache = op_create(OP_TYPE_CACHE);
  TEST_ASSERT_EQUAL(OP_TYPE_CACHE, op_cache->op_type);
  op_destroy(op_cache);
}

// --- SETTER TESTS ---

void test_op_set_target_string_key(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_INVERTED_EVENT_INDEX,
      .container_name = "test_container",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "test_key"}}};

  op_t *op = op_create(OP_TYPE_PUT);
  op_set_target(op, &db_key);

  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, op->db_key.dc_type);
  TEST_ASSERT_EQUAL(USER_DB_INVERTED_EVENT_INDEX, op->db_key.user_db_type);
  TEST_ASSERT_EQUAL_STRING(db_key.container_name, op->db_key.container_name);
  TEST_ASSERT_EQUAL_STRING(db_key.db_key.key.s, op->db_key.db_key.key.s);

  op_destroy(op);
}

void test_op_set_target_int_key(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_SYSTEM,
      .sys_db_type = SYS_DB_ENT_ID_TO_INT,
      .container_name = NULL,
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 12345}}};

  op_t *op = op_create(OP_TYPE_PUT);
  op_set_target(op, &db_key);

  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, op->db_key.dc_type);
  TEST_ASSERT_EQUAL(SYS_DB_ENT_ID_TO_INT, op->db_key.sys_db_type);
  TEST_ASSERT_EQUAL(12345, op->db_key.db_key.key.i);

  op_destroy(op);
}

void test_op_set_condition(void) {
  op_t *op = op_create(OP_TYPE_COND_PUT);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);

  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);

  op_destroy(op);
}

void test_op_set_value_int32(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_int32(op, 12345);

  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL_UINT32(12345, op->value.int32);

  op_destroy(op);
}

void test_op_set_value_int32_zero(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_int32(op, 0);

  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL_UINT32(0, op->value.int32);

  op_destroy(op);
}

void test_op_set_value_str(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  const char *test_val = "test_value";
  op_set_value_str(op, test_val);

  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL_STRING(test_val, op->value.str);
  TEST_ASSERT_NOT_EQUAL(test_val, op->value.str); // Should be duplicated

  op_destroy(op);
}

void test_op_set_value_str_empty_string(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_str(op, "");

  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL_STRING("", op->value.str);

  op_destroy(op);
}

void test_op_set_value_str_overwrites_int(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_int32(op, 999);
  op_set_value_str(op, "new_string");

  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL_STRING("new_string", op->value.str);

  op_destroy(op);
}

void test_op_set_value_int32_overwrites_str(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_str(op, "old_string");
  op_set_value_int32(op, 42);

  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL_UINT32(42, op->value.int32);

  op_destroy(op);
}

// --- GETTER TESTS ---

void test_op_getters(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_COUNTER_STORE,
      .container_name = "container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 42}}};

  op_t *op = op_create(OP_TYPE_COND_PUT);
  op_set_target(op, &db_key);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);
  op_set_value_int32(op, 100);

  TEST_ASSERT_EQUAL(OP_TYPE_COND_PUT, op_get_type(op));
  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op_get_value_type(op));
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op_get_condition_type(op));
  TEST_ASSERT_EQUAL_UINT32(100, op_get_value_int32(op));

  const eng_container_db_key_t *key = op_get_db_key(op);
  TEST_ASSERT_NOT_NULL(key);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, key->dc_type);
  TEST_ASSERT_EQUAL(42, key->db_key.key.i);

  op_destroy(op);
}

void test_op_get_value_wrong_type(void) {
  op_t *op = op_create(OP_TYPE_PUT);
  op_set_value_int32(op, 123);

  TEST_ASSERT_NULL(op_get_value_str(op));
  TEST_ASSERT_EQUAL_UINT32(0, op_get_value_int32(NULL)); // NULL op

  op_destroy(op);
}

void test_op_get_null_op(void) {
  TEST_ASSERT_EQUAL(OP_TYPE_NONE, op_get_type(NULL));
  TEST_ASSERT_EQUAL(OP_VALUE_NONE, op_get_value_type(NULL));
  TEST_ASSERT_EQUAL(COND_PUT_NONE, op_get_condition_type(NULL));
  TEST_ASSERT_NULL(op_get_db_key(NULL));
  TEST_ASSERT_EQUAL_UINT32(0, op_get_value_int32(NULL));
  TEST_ASSERT_NULL(op_get_value_str(NULL));
}

// --- HELPER FUNCTION TESTS ---

void test_op_create_str_val_success(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_SYSTEM,
      .sys_db_type = SYS_DB_INT_TO_ENT_ID,
      .container_name = NULL,
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "test_key"}}};
  const char *val = "test_value";

  op_t *op = op_create_str_val(&db_key, OP_TYPE_PUT,
                               COND_PUT_IF_EXISTING_LESS_THAN, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_TYPE_PUT, op->op_type);
  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op->value_type);
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, op->db_key.dc_type);
  TEST_ASSERT_EQUAL_STRING(db_key.db_key.key.s, op->db_key.db_key.key.s);
  TEST_ASSERT_EQUAL_STRING(val, op->value.str);
  TEST_ASSERT_NOT_EQUAL(val, op->value.str); // Should be duplicated

  op_destroy(op);
}

void test_op_create_str_val_null_args(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_INVERTED_EVENT_INDEX,
      .container_name = "c",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "k"}}};
  const char *val = "v";

  TEST_ASSERT_NULL(op_create_str_val(NULL, OP_TYPE_PUT, COND_PUT_NONE, val));
  TEST_ASSERT_NULL(
      op_create_str_val(&db_key, OP_TYPE_PUT, COND_PUT_NONE, NULL));
}

void test_op_create_int32_val_success(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_EVENT_TO_ENTITY,
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 456;

  op_t *op =
      op_create_int32_val(&db_key, OP_TYPE_ADD_VALUE, COND_PUT_NONE, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL(OP_TYPE_ADD_VALUE, op->op_type);
  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op->value_type);
  TEST_ASSERT_EQUAL(COND_PUT_NONE, op->cond_type);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, op->db_key.dc_type);
  TEST_ASSERT_EQUAL(USER_DB_EVENT_TO_ENTITY, op->db_key.user_db_type);
  TEST_ASSERT_EQUAL(db_key.db_key.key.i, op->db_key.db_key.key.i);
  TEST_ASSERT_EQUAL_UINT32(val, op->value.int32);

  op_destroy(op);
}

void test_op_create_int32_val_with_zero_value(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_COUNTER_STORE,
      .container_name = "int_container",
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 123}}};
  uint32_t val = 0;

  op_t *op = op_create_int32_val(&db_key, OP_TYPE_PUT, COND_PUT_NONE, val);

  TEST_ASSERT_NOT_NULL(op);
  TEST_ASSERT_EQUAL_UINT32(0, op->value.int32);

  op_destroy(op);
}

void test_op_create_int32_val_null_db_key(void) {
  TEST_ASSERT_NULL(op_create_int32_val(NULL, OP_TYPE_PUT, COND_PUT_NONE, 42));
}

// --- INTEGRATION TESTS ---

void test_op_full_workflow_string(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_SYSTEM,
      .sys_db_type = SYS_DB_INT_TO_ENT_ID,
      .container_name = NULL,
      .db_key = {.type = DB_KEY_INTEGER, .key = {.i = 999}}};

  op_t *op = op_create(OP_TYPE_PUT);
  op_set_target(op, &db_key);
  op_set_value_str(op, "entity_id_string");

  TEST_ASSERT_EQUAL(OP_TYPE_PUT, op_get_type(op));
  TEST_ASSERT_EQUAL(OP_VALUE_STRING, op_get_value_type(op));
  TEST_ASSERT_EQUAL_STRING("entity_id_string", op_get_value_str(op));

  const eng_container_db_key_t *retrieved_key = op_get_db_key(op);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, retrieved_key->dc_type);
  TEST_ASSERT_EQUAL(999, retrieved_key->db_key.key.i);

  op_destroy(op);
}

void test_op_full_workflow_int32(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_COUNTER_STORE,
      .container_name = "user_123",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "counter_key"}}};

  op_t *op = op_create(OP_TYPE_ADD_VALUE);
  op_set_target(op, &db_key);
  op_set_value_int32(op, 5);

  TEST_ASSERT_EQUAL(OP_TYPE_ADD_VALUE, op_get_type(op));
  TEST_ASSERT_EQUAL(OP_VALUE_INT32, op_get_value_type(op));
  TEST_ASSERT_EQUAL_UINT32(5, op_get_value_int32(op));

  const eng_container_db_key_t *retrieved_key = op_get_db_key(op);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, retrieved_key->dc_type);
  TEST_ASSERT_EQUAL_STRING("user_123", retrieved_key->container_name);

  op_destroy(op);
}

void test_op_conditional_put(void) {
  eng_container_db_key_t db_key = {
      .dc_type = CONTAINER_TYPE_USER,
      .user_db_type = USER_DB_COUNTER_STORE,
      .container_name = "user_456",
      .db_key = {.type = DB_KEY_STRING, .key = {.s = "max_value"}}};

  op_t *op = op_create(OP_TYPE_COND_PUT);
  op_set_target(op, &db_key);
  op_set_condition(op, COND_PUT_IF_EXISTING_LESS_THAN);
  op_set_value_int32(op, 100);

  TEST_ASSERT_EQUAL(OP_TYPE_COND_PUT, op->op_type);
  TEST_ASSERT_EQUAL(COND_PUT_IF_EXISTING_LESS_THAN, op->cond_type);
  TEST_ASSERT_EQUAL_UINT32(100, op->value.int32);

  op_destroy(op);
}

// --- MAIN RUNNER ---

int main(void) {
  UNITY_BEGIN();

  // Lifecycle
  RUN_TEST(test_op_create_destroy);
  RUN_TEST(test_op_destroy_null);
  RUN_TEST(test_op_create_all_types);

  // Setters
  RUN_TEST(test_op_set_target_string_key);
  RUN_TEST(test_op_set_target_int_key);
  RUN_TEST(test_op_set_condition);
  RUN_TEST(test_op_set_value_int32);
  RUN_TEST(test_op_set_value_int32_zero);
  RUN_TEST(test_op_set_value_str);
  RUN_TEST(test_op_set_value_str_empty_string);
  RUN_TEST(test_op_set_value_str_overwrites_int);
  RUN_TEST(test_op_set_value_int32_overwrites_str);

  // Getters
  RUN_TEST(test_op_getters);
  RUN_TEST(test_op_get_value_wrong_type);
  RUN_TEST(test_op_get_null_op);

  // Helper functions
  RUN_TEST(test_op_create_str_val_success);
  RUN_TEST(test_op_create_str_val_null_args);
  RUN_TEST(test_op_create_int32_val_success);
  RUN_TEST(test_op_create_int32_val_with_zero_value);
  RUN_TEST(test_op_create_int32_val_null_db_key);

  // Integration
  RUN_TEST(test_op_full_workflow_string);
  RUN_TEST(test_op_full_workflow_int32);
  RUN_TEST(test_op_conditional_put);

  return UNITY_END();
}