#include "core/db.h"
#include "engine/container/container.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// =============================================================================
// Test Fixtures
// =============================================================================

void setUp(void) {
  // Setup if needed
}

void tearDown(void) {
  // Cleanup if needed
}

// =============================================================================
// Container Creation Tests
// =============================================================================

void test_create_system_container_succeeds(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);

  TEST_ASSERT_NOT_NULL(c);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, c->type);
  TEST_ASSERT_NULL(c->env);
  TEST_ASSERT_NULL(c->name);
  TEST_ASSERT_NOT_NULL(c->data.sys);

  free(c->data.sys);
  free(c);
}

void test_create_user_container_succeeds(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);

  TEST_ASSERT_NOT_NULL(c);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, c->type);
  TEST_ASSERT_NULL(c->env);
  TEST_ASSERT_NULL(c->name);
  TEST_ASSERT_NOT_NULL(c->data.usr);

  free(c->data.usr);
  free(c);
}

// =============================================================================
// Container Close Tests
// =============================================================================

void test_close_null_container_is_safe(void) {
  // Should not crash
  eng_container_close(NULL);
  TEST_PASS();
}

void test_close_system_container_with_null_env_succeeds(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);
  c->env = NULL;

  // Should not crash
  eng_container_close(c);
  TEST_PASS();
}

void test_close_user_container_with_null_env_succeeds(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->env = NULL;

  // Should not crash
  eng_container_close(c);
  TEST_PASS();
}

void test_close_container_with_name_succeeds(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);
  c->name = strdup("test_container");

  // Should free name and not crash
  eng_container_close(c);
  TEST_PASS();
}

void test_close_system_container_with_name_and_null_env(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);
  c->name = strdup("test_system");
  c->env = NULL;

  eng_container_close(c);
  TEST_PASS();
}

void test_close_user_container_with_name_and_null_env(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->name = strdup("test_user");
  c->env = NULL;

  eng_container_close(c);
  TEST_PASS();
}

// =============================================================================
// Get DB Handle Tests
// =============================================================================

void test_get_db_handle_for_user_container_inverted_event_index(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->data.usr->inverted_event_index_db = 42;

  MDB_dbi dbi_out;
  bool result =
      eng_container_get_db_handle(c, USER_DB_INVERTED_EVENT_INDEX, &dbi_out);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(42, dbi_out);

  free(c->data.usr);
  free(c);
}

void test_get_db_handle_for_user_container_event_to_entity(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->data.usr->event_to_entity_db = 100;

  MDB_dbi dbi_out;
  bool result =
      eng_container_get_db_handle(c, USER_DB_EVENT_TO_ENTITY, &dbi_out);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(100, dbi_out);

  free(c->data.usr);
  free(c);
}

void test_get_db_handle_for_user_container_metadata(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->data.usr->user_dc_metadata_db = 200;

  MDB_dbi dbi_out;
  bool result = eng_container_get_db_handle(c, USER_DB_METADATA, &dbi_out);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(200, dbi_out);

  free(c->data.usr);
  free(c);
}

void test_get_db_handle_for_user_container_counter_store(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->data.usr->counter_store_db = 300;

  MDB_dbi dbi_out;
  bool result = eng_container_get_db_handle(c, USER_DB_COUNTER_STORE, &dbi_out);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(300, dbi_out);

  free(c->data.usr);
  free(c);
}

void test_get_db_handle_for_user_container_count_index(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);
  c->data.usr->count_index_db = 400;

  MDB_dbi dbi_out;
  bool result = eng_container_get_db_handle(c, USER_DB_COUNT_INDEX, &dbi_out);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(400, dbi_out);

  free(c->data.usr);
  free(c);
}

void test_get_db_handle_with_null_container_returns_false(void) {
  MDB_dbi dbi_out;
  bool result = eng_container_get_db_handle(NULL, USER_DB_METADATA, &dbi_out);

  TEST_ASSERT_FALSE(result);
}

void test_get_db_handle_with_system_container_returns_false(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);

  MDB_dbi dbi_out;
  bool result = eng_container_get_db_handle(c, USER_DB_METADATA, &dbi_out);

  TEST_ASSERT_FALSE(result);

  free(c->data.sys);
  free(c);
}

void test_get_db_handle_with_invalid_db_type_returns_false(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);

  MDB_dbi dbi_out;
  bool result =
      eng_container_get_db_handle(c, (eng_dc_user_db_type_t)999, &dbi_out);

  TEST_ASSERT_FALSE(result);

  free(c->data.usr);
  free(c);
}

// =============================================================================
// Get DB Handle - Test All Cases
// =============================================================================

void test_get_db_handle_all_user_db_types(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);

  c->data.usr->inverted_event_index_db = 1;
  c->data.usr->event_to_entity_db = 2;
  c->data.usr->user_dc_metadata_db = 3;
  c->data.usr->counter_store_db = 4;
  c->data.usr->count_index_db = 5;

  MDB_dbi dbi_out;

  TEST_ASSERT_TRUE(
      eng_container_get_db_handle(c, USER_DB_INVERTED_EVENT_INDEX, &dbi_out));
  TEST_ASSERT_EQUAL(1, dbi_out);

  TEST_ASSERT_TRUE(
      eng_container_get_db_handle(c, USER_DB_EVENT_TO_ENTITY, &dbi_out));
  TEST_ASSERT_EQUAL(2, dbi_out);

  TEST_ASSERT_TRUE(eng_container_get_db_handle(c, USER_DB_METADATA, &dbi_out));
  TEST_ASSERT_EQUAL(3, dbi_out);

  TEST_ASSERT_TRUE(
      eng_container_get_db_handle(c, USER_DB_COUNTER_STORE, &dbi_out));
  TEST_ASSERT_EQUAL(4, dbi_out);

  TEST_ASSERT_TRUE(
      eng_container_get_db_handle(c, USER_DB_COUNT_INDEX, &dbi_out));
  TEST_ASSERT_EQUAL(5, dbi_out);

  free(c->data.usr);
  free(c);
}

// =============================================================================
// Free DB Key Contents Tests
// =============================================================================

void test_free_db_key_with_null_is_safe(void) {
  eng_Container_free_contents_db_key(NULL);
  TEST_PASS();
}

void test_free_db_key_with_string_key_frees_string(void) {
  eng_container_db_key_t db_key;
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup("string_key");

  eng_Container_free_contents_db_key(&db_key);

  // Can't directly verify free was called, but ensures no crash
  TEST_PASS();
}

void test_free_db_key_with_integer_key_only_frees_container_name(void) {
  eng_container_db_key_t db_key;
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = 42;

  eng_Container_free_contents_db_key(&db_key);

  TEST_PASS();
}

void test_free_db_key_with_null_container_name_is_safe(void) {
  eng_container_db_key_t db_key;
  db_key.container_name = NULL;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup("key");

  eng_Container_free_contents_db_key(&db_key);

  TEST_PASS();
}

void test_free_db_key_with_null_string_key_is_safe(void) {
  eng_container_db_key_t db_key;
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = NULL;

  eng_Container_free_contents_db_key(&db_key);

  TEST_PASS();
}

void test_free_db_key_with_all_nulls_is_safe(void) {
  eng_container_db_key_t db_key;
  db_key.container_name = NULL;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = NULL;

  eng_Container_free_contents_db_key(&db_key);

  TEST_PASS();
}

// =============================================================================
// Container Type Tests
// =============================================================================

void test_system_container_has_correct_type(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_SYSTEM);

  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, c->type);
  TEST_ASSERT_NOT_NULL(c->data.sys);

  free(c->data.sys);
  free(c);
}

void test_user_container_has_correct_type(void) {
  eng_container_t *c = eng_container_create(CONTAINER_TYPE_USER);

  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, c->type);
  TEST_ASSERT_NOT_NULL(c->data.usr);

  free(c->data.usr);
  free(c);
}

// =============================================================================
// Main Test Runner
// =============================================================================

int main(void) {
  UNITY_BEGIN();

  // Creation
  RUN_TEST(test_create_system_container_succeeds);
  RUN_TEST(test_create_user_container_succeeds);
  RUN_TEST(test_system_container_has_correct_type);
  RUN_TEST(test_user_container_has_correct_type);

  // Close
  RUN_TEST(test_close_null_container_is_safe);
  RUN_TEST(test_close_system_container_with_null_env_succeeds);
  RUN_TEST(test_close_user_container_with_null_env_succeeds);
  RUN_TEST(test_close_container_with_name_succeeds);
  RUN_TEST(test_close_system_container_with_name_and_null_env);
  RUN_TEST(test_close_user_container_with_name_and_null_env);

  // Get DB Handle - Individual
  RUN_TEST(test_get_db_handle_for_user_container_inverted_event_index);
  RUN_TEST(test_get_db_handle_for_user_container_event_to_entity);
  RUN_TEST(test_get_db_handle_for_user_container_metadata);
  RUN_TEST(test_get_db_handle_for_user_container_counter_store);
  RUN_TEST(test_get_db_handle_for_user_container_count_index);

  // Get DB Handle - Edge Cases
  RUN_TEST(test_get_db_handle_with_null_container_returns_false);
  RUN_TEST(test_get_db_handle_with_system_container_returns_false);
  RUN_TEST(test_get_db_handle_with_invalid_db_type_returns_false);
  RUN_TEST(test_get_db_handle_all_user_db_types);

  // Free DB Key
  RUN_TEST(test_free_db_key_with_null_is_safe);
  RUN_TEST(test_free_db_key_with_string_key_frees_string);
  RUN_TEST(test_free_db_key_with_integer_key_only_frees_container_name);
  RUN_TEST(test_free_db_key_with_null_container_name_is_safe);
  RUN_TEST(test_free_db_key_with_null_string_key_is_safe);
  RUN_TEST(test_free_db_key_with_all_nulls_is_safe);

  return UNITY_END();
}