#include "core/db.h"
#include "engine/container/container_db.h"
#include "engine/container/container_types.h"
#include "unity.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Test data directory
#define TEST_DATA_DIR "test_data"
#define TEST_CONTAINER_SIZE (10 * 1024 * 1024) // 10MB

// Helper to create test directory
static void create_test_dir(void) {
  struct stat st = {0};
  if (stat(TEST_DATA_DIR, &st) == -1) {
    mkdir(TEST_DATA_DIR, 0700);
  }
}

// Helper to recursively remove directory
static void remove_test_dir(void) {
  // char cmd[512];
  // snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DATA_DIR);
  // system(cmd);
}

void setUp(void) {
  remove_test_dir();
  create_test_dir();
}

void tearDown(void) { remove_test_dir(); }

// ============= container_close tests =============

void test_container_close_null(void) {
  // Should not crash
  container_close(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_container_close_user_container(void) {
  container_result_t result =
      create_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);

  // Should close without error
  container_close(result.container);
  TEST_ASSERT_TRUE(true);
}

void test_container_close_system_container(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);

  // Should close without error
  container_close(result.container);
  TEST_ASSERT_TRUE(true);
}

// ============= create_user_container tests =============

void test_create_user_container_success(void) {
  container_result_t result =
      create_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_NULL(result.error_msg);
  TEST_ASSERT_EQUAL(CONTAINER_OK, result.error_code);

  // Check container properties
  TEST_ASSERT_NOT_NULL(result.container->name);
  TEST_ASSERT_EQUAL_STRING("test_user", result.container->name);
  TEST_ASSERT_NOT_NULL(result.container->env);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USR, result.container->type);
  TEST_ASSERT_NOT_NULL(result.container->data.usr);

  // Verify all user databases are opened
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->inverted_event_index_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->event_to_entity_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->user_dc_metadata_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->counter_store_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->count_index_db);

  container_close(result.container);
}

void test_create_user_container_with_different_names(void) {
  container_result_t result1 =
      create_user_container("container1", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t result2 =
      create_user_container("container2", TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_TRUE(result1.success);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_EQUAL_STRING("container1", result1.container->name);
  TEST_ASSERT_EQUAL_STRING("container2", result2.container->name);

  container_close(result1.container);
  container_close(result2.container);
}

void test_create_user_container_path_too_long(void) {
  char long_name[300];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';

  container_result_t result =
      create_user_container(long_name, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_PATH_TOO_LONG, result.error_code);
  TEST_ASSERT_NOT_NULL(result.error_msg);
}

void test_create_user_container_invalid_data_dir(void) {
  // Try to create in non-existent directory
  container_result_t result = create_user_container(
      "test", "/nonexistent/path/xyz", TEST_CONTAINER_SIZE);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_ENV_CREATE, result.error_code);
}

// ============= create_system_container tests =============

void test_create_system_container_success(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_NULL(result.error_msg);
  TEST_ASSERT_EQUAL(CONTAINER_OK, result.error_code);

  // Check container properties
  TEST_ASSERT_NOT_NULL(result.container->name);
  TEST_ASSERT_EQUAL_STRING(SYS_CONTAINER_NAME, result.container->name);
  TEST_ASSERT_NOT_NULL(result.container->env);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYS, result.container->type);
  TEST_ASSERT_NOT_NULL(result.container->data.sys);

  // Verify all system databases are opened
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->ent_id_to_int_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->int_to_ent_id_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->sys_dc_metadata_db);

  container_close(result.container);
}

void test_create_system_container_invalid_data_dir(void) {
  container_result_t result =
      create_system_container("/nonexistent/path/xyz", TEST_CONTAINER_SIZE);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_ENV_CREATE, result.error_code);
}

void test_create_system_container_reopen(void) {
  // Create and close
  container_result_t result1 =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result1.success);
  container_close(result1.container);

  // Reopen - should succeed
  container_result_t result2 =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_NOT_NULL(result2.container);

  container_close(result2.container);
}

// ============= cdb_get_user_db_handle tests =============

void test_cdb_get_user_db_handle_null_container(void) {
  MDB_dbi db_out;
  bool result =
      cdb_get_user_db_handle(NULL, USR_DB_INVERTED_EVENT_INDEX, &db_out);
  TEST_ASSERT_FALSE(result);
}

void test_cdb_get_user_db_handle_null_output(void) {
  container_result_t result =
      create_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  bool get_result = cdb_get_user_db_handle(result.container,
                                           USR_DB_INVERTED_EVENT_INDEX, NULL);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

void test_cdb_get_user_db_handle_wrong_container_type(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result = cdb_get_user_db_handle(
      result.container, USR_DB_INVERTED_EVENT_INDEX, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

void test_cdb_get_user_db_handle_all_db_types(void) {
  container_result_t result =
      create_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;

  // Test each database type
  TEST_ASSERT_TRUE(cdb_get_user_db_handle(
      result.container, USR_DB_INVERTED_EVENT_INDEX, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->inverted_event_index_db,
                    db_out);

  TEST_ASSERT_TRUE(cdb_get_user_db_handle(result.container,
                                          USER_DB_EVENT_TO_ENTITY, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->event_to_entity_db, db_out);

  TEST_ASSERT_TRUE(
      cdb_get_user_db_handle(result.container, USR_DB_METADATA, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->user_dc_metadata_db, db_out);

  TEST_ASSERT_TRUE(
      cdb_get_user_db_handle(result.container, USER_DB_COUNTER_STORE, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->counter_store_db, db_out);

  TEST_ASSERT_TRUE(
      cdb_get_user_db_handle(result.container, USER_DB_COUNT_INDEX, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->count_index_db, db_out);

  container_close(result.container);
}

void test_cdb_get_user_db_handle_invalid_db_type(void) {
  container_result_t result =
      create_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result = cdb_get_user_db_handle(result.container,
                                           999, // invalid type
                                           &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

// ============= cdb_get_system_db_handle tests =============

void test_cdb_get_system_db_handle_null_container(void) {
  MDB_dbi db_out;
  bool result = cdb_get_system_db_handle(NULL, SYS_DB_ENT_ID_TO_INT, &db_out);
  TEST_ASSERT_FALSE(result);
}

void test_cdb_get_system_db_handle_null_output(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  bool get_result =
      cdb_get_system_db_handle(result.container, SYS_DB_ENT_ID_TO_INT, NULL);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

void test_cdb_get_system_db_handle_wrong_container_type(void) {
  container_result_t result =
      create_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result =
      cdb_get_system_db_handle(result.container, SYS_DB_ENT_ID_TO_INT, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

void test_cdb_get_system_db_handle_all_db_types(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;

  // Test each database type
  TEST_ASSERT_TRUE(cdb_get_system_db_handle(result.container,
                                            SYS_DB_ENT_ID_TO_INT, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->ent_id_to_int_db, db_out);

  TEST_ASSERT_TRUE(cdb_get_system_db_handle(result.container,
                                            SYS_DB_INT_TO_ENT_ID, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->int_to_ent_id_db, db_out);

  TEST_ASSERT_TRUE(
      cdb_get_system_db_handle(result.container, SYS_DB_METADATA, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->sys_dc_metadata_db, db_out);

  container_close(result.container);
}

void test_cdb_get_system_db_handle_invalid_db_type(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result = cdb_get_system_db_handle(result.container,
                                             999, // invalid type
                                             &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

// ============= cdb_free_db_key_contents tests =============

void test_cdb_free_db_key_contents_null(void) {
  // Should not crash
  cdb_free_db_key_contents(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_cdb_free_db_key_contents_with_string_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.container_name = strdup("test_container");
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup("test_key");

  // Should free without error
  cdb_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

void test_cdb_free_db_key_contents_with_int_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.container_name = strdup("test_container");
  db_key.db_key.type = DB_KEY_INTEGER;
  db_key.db_key.key.i = 42;

  // Should free without error (string key should not be freed)
  cdb_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

void test_cdb_free_db_key_contents_null_container_name(void) {
  eng_container_db_key_t db_key = {0};
  db_key.container_name = NULL;
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup("test_key");

  // Should handle gracefully
  cdb_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

// ============= Integration tests =============

void test_create_user_and_system_containers_together(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t usr_result =
      create_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_TRUE(sys_result.success);
  TEST_ASSERT_TRUE(usr_result.success);
  TEST_ASSERT_NOT_EQUAL(sys_result.container->env, usr_result.container->env);

  container_close(sys_result.container);
  container_close(usr_result.container);
}

void test_multiple_user_containers(void) {
  container_result_t result1 =
      create_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t result2 =
      create_user_container("user2", TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t result3 =
      create_user_container("user3", TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  TEST_ASSERT_TRUE(result1.success);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_TRUE(result3.success);

  container_close(result1.container);
  container_close(result2.container);
  container_close(result3.container);
}

// ============= Main test runner =============

int main(void) {
  UNITY_BEGIN();

  // container_close tests
  RUN_TEST(test_container_close_null);
  RUN_TEST(test_container_close_user_container);
  RUN_TEST(test_container_close_system_container);

  // create_user_container tests
  RUN_TEST(test_create_user_container_success);
  RUN_TEST(test_create_user_container_with_different_names);
  RUN_TEST(test_create_user_container_path_too_long);
  RUN_TEST(test_create_user_container_invalid_data_dir);

  // create_system_container tests
  RUN_TEST(test_create_system_container_success);
  RUN_TEST(test_create_system_container_invalid_data_dir);
  RUN_TEST(test_create_system_container_reopen);

  // cdb_get_user_db_handle tests
  RUN_TEST(test_cdb_get_user_db_handle_null_container);
  RUN_TEST(test_cdb_get_user_db_handle_null_output);
  RUN_TEST(test_cdb_get_user_db_handle_wrong_container_type);
  RUN_TEST(test_cdb_get_user_db_handle_all_db_types);
  RUN_TEST(test_cdb_get_user_db_handle_invalid_db_type);

  // cdb_get_system_db_handle tests
  RUN_TEST(test_cdb_get_system_db_handle_null_container);
  RUN_TEST(test_cdb_get_system_db_handle_null_output);
  RUN_TEST(test_cdb_get_system_db_handle_wrong_container_type);
  RUN_TEST(test_cdb_get_system_db_handle_all_db_types);
  RUN_TEST(test_cdb_get_system_db_handle_invalid_db_type);

  // cdb_free_db_key_contents tests
  RUN_TEST(test_cdb_free_db_key_contents_null);
  RUN_TEST(test_cdb_free_db_key_contents_with_string_key);
  RUN_TEST(test_cdb_free_db_key_contents_with_int_key);
  RUN_TEST(test_cdb_free_db_key_contents_null_container_name);

  // Integration tests
  RUN_TEST(test_create_user_and_system_containers_together);
  RUN_TEST(test_multiple_user_containers);

  return UNITY_END();
}