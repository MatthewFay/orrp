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

// Helper to safely remove specific test files
static void remove_test_files(void) {
  // Remove system container files
  char path[512];
  snprintf(path, sizeof(path), "%s/%s.mdb", TEST_DATA_DIR, SYS_CONTAINER_NAME);
  unlink(path);

  snprintf(path, sizeof(path), "%s/%s_ent.bin", TEST_DATA_DIR,
           SYS_CONTAINER_NAME);
  unlink(path);

  // Remove known user container files
  const char *user_names[] = {"test_user", "test",       "user1",      "user2",
                              "user3",     "container1", "container2", NULL};
  for (int i = 0; user_names[i] != NULL; i++) {
    snprintf(path, sizeof(path), "%s/%s.mdb", TEST_DATA_DIR, user_names[i]);
    unlink(path);

    snprintf(path, sizeof(path), "%s/%s_evt_ent.bin", TEST_DATA_DIR,
             user_names[i]);
    unlink(path);
  }

  // Remove the test directory itself (will only succeed if empty)
  rmdir(TEST_DATA_DIR);
}

void setUp(void) {
  remove_test_files();
  create_test_dir();
}

void tearDown(void) { remove_test_files(); }

// ============= container_close tests =============

void test_container_close_null(void) {
  // Should not crash
  container_close(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_container_close_user_container(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);

  // Should close without error
  container_close(result.container);
  container_close(sys_result.container);
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

  // Verify all system databases are opened (non-zero handles)
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->int_to_entity_id_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->str_to_entity_id_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.sys->sys_dc_metadata_db);
  TEST_ASSERT_NOT_EQUAL(0,
                        result.container->data.sys->index_registry_global_db);

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

// ============= create_user_container tests =============

void test_create_user_container_success(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

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
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->user_dc_metadata_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->events_db);
  TEST_ASSERT_NOT_EQUAL(0, result.container->data.usr->index_registry_local_db);

  container_close(result.container);
  container_close(sys_result.container);
}

void test_create_user_container_with_sys_txn(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  // Create a read transaction for system container
  MDB_txn *sys_txn = db_create_txn(sys_result.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);

  container_result_t result =
      open_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);

  // Abort the transaction (read-only, so abort is fine)
  db_abort_txn(sys_txn);

  container_close(result.container);
  container_close(sys_result.container);
}

void test_create_user_container_with_different_names(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result1 =
      open_user_container("container1", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  container_result_t result2 =
      open_user_container("container2", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_TRUE(result1.success);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_EQUAL_STRING("container1", result1.container->name);
  TEST_ASSERT_EQUAL_STRING("container2", result2.container->name);

  container_close(result1.container);
  container_close(result2.container);
  container_close(sys_result.container);
}

void test_create_user_container_path_too_long(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  char long_name[300];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';

  container_result_t result =
      open_user_container(long_name, TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_PATH_TOO_LONG, result.error_code);
  TEST_ASSERT_NOT_NULL(result.error_msg);

  container_close(sys_result.container);
}

void test_create_user_container_invalid_data_dir(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  // Try to create in non-existent directory
  container_result_t result =
      open_user_container("test", "/nonexistent/path/xyz", TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_ENV_CREATE, result.error_code);

  container_close(sys_result.container);
}

void test_create_user_container_null_system_container(void) {
  container_result_t result = open_user_container(
      "test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE, NULL, NULL, true);

  // Should fail gracefully
  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
}

void test_create_user_container_null_name(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container(NULL, TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);

  container_close(sys_result.container);
}

void test_create_user_container_null_data_dir(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result = open_user_container(
      "test", NULL, TEST_CONTAINER_SIZE, sys_result.container, NULL, true);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);

  container_close(sys_result.container);
}

void test_create_user_container_zero_size(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result = open_user_container(
      "test", TEST_DATA_DIR, 0, sys_result.container, NULL, true);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);

  container_close(sys_result.container);
}

void test_create_user_container_reuses_provided_txn(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  // Create a read transaction
  MDB_txn *sys_txn = db_create_txn(sys_result.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);

  // Create multiple user containers with the same transaction
  container_result_t result1 =
      open_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);
  TEST_ASSERT_TRUE(result1.success);

  container_result_t result2 =
      open_user_container("user2", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);
  TEST_ASSERT_TRUE(result2.success);

  // Transaction should still be valid
  db_abort_txn(sys_txn);

  container_close(result1.container);
  container_close(result2.container);
  container_close(sys_result.container);
}

// ============= cdb_get_db_handle tests =============

void test_cdb_get_db_handle_null_container(void) {
  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  bool result = cdb_get_db_handle(NULL, &db_key, &db_out);
  TEST_ASSERT_FALSE(result);
}

void test_cdb_get_db_handle_null_db_key(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result = cdb_get_db_handle(result.container, NULL, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
}

void test_cdb_get_db_handle_null_output(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result.success);

  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;

  bool get_result = cdb_get_db_handle(result.container, &db_key, NULL);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
  container_close(sys_result.container);
}

void test_cdb_get_db_handle_user_dbs(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  // Test Inverted Event Index
  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->inverted_event_index_db,
                    db_out);

  // Test Metadata
  db_key.usr_db_type = USR_DB_METADATA;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->user_dc_metadata_db, db_out);

  // Test Events DB
  db_key.usr_db_type = USR_DB_EVENTS;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->events_db, db_out);

  // Test Index Registry Local
  db_key.usr_db_type = USR_DB_INDEX_REGISTRY_LOCAL;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.usr->index_registry_local_db,
                    db_out);

  container_close(result.container);
  container_close(sys_result.container);
}

void test_cdb_get_db_handle_system_dbs(void) {
  container_result_t result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  // Test Int to Entity ID
  db_key.sys_db_type = SYS_DB_INT_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->int_to_entity_id_db, db_out);

  // Test String to Entity ID
  db_key.sys_db_type = SYS_DB_STR_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->str_to_entity_id_db, db_out);

  // Test Metadata
  db_key.sys_db_type = SYS_DB_METADATA;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->sys_dc_metadata_db, db_out);

  // Test Index Registry Global
  db_key.sys_db_type = SYS_DB_INDEX_REGISTRY_GLOBAL;
  TEST_ASSERT_TRUE(cdb_get_db_handle(result.container, &db_key, &db_out));
  TEST_ASSERT_EQUAL(result.container->data.sys->index_registry_global_db,
                    db_out);

  container_close(result.container);
}

void test_cdb_get_db_handle_invalid_db_type(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = 999; // invalid type

  bool get_result = cdb_get_db_handle(result.container, &db_key, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
  container_close(sys_result.container);
}

void test_cdb_get_db_handle_index_db_null_key(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result =
      open_user_container("test", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = USR_DB_INDEX;
  db_key.index_key = NULL; // Should fail with NULL index_key

  bool get_result = cdb_get_db_handle(result.container, &db_key, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_close(result.container);
  container_close(sys_result.container);
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
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = 42;

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

void test_cdb_free_db_key_contents_with_index_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.dc_type = CONTAINER_TYPE_USR;
  db_key.usr_db_type = USR_DB_INDEX;
  db_key.container_name = strdup("test_container");
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = 42;
  db_key.index_key = strdup("my_index");

  // Should free all allocated fields
  cdb_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

void test_cdb_free_db_key_contents_index_key_wrong_type(void) {
  eng_container_db_key_t db_key = {0};
  db_key.dc_type = CONTAINER_TYPE_SYS; // System container
  db_key.container_name = strdup("test_container");
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = 42;
  db_key.index_key = strdup("my_index");

  // Should NOT free index_key since it's not USR_DB_INDEX
  cdb_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

// ============= Integration tests =============

void test_create_user_and_system_containers_together(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t usr_result =
      open_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_TRUE(sys_result.success);
  TEST_ASSERT_TRUE(usr_result.success);
  TEST_ASSERT_NOT_EQUAL(sys_result.container->env, usr_result.container->env);

  container_close(usr_result.container);
  container_close(sys_result.container);
}

void test_multiple_user_containers(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  container_result_t result1 =
      open_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  container_result_t result2 =
      open_user_container("user2", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  container_result_t result3 =
      open_user_container("user3", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);

  TEST_ASSERT_TRUE(result1.success);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_TRUE(result3.success);

  container_close(result1.container);
  container_close(result2.container);
  container_close(result3.container);
  container_close(sys_result.container);
}

void test_user_container_reopen(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  // Create and close
  container_result_t result1 =
      open_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result1.success);
  container_close(result1.container);

  // Reopen - should succeed (not new, so won't init index)
  container_result_t result2 =
      open_user_container("test_user", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, NULL, true);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_NOT_NULL(result2.container);

  container_close(result2.container);
  container_close(sys_result.container);
}

void test_multiple_containers_with_shared_txn(void) {
  container_result_t sys_result =
      create_system_container(TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(sys_result.success);

  // Create one transaction to share across multiple container creations
  MDB_txn *sys_txn = db_create_txn(sys_result.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);

  container_result_t result1 =
      open_user_container("user1", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);
  TEST_ASSERT_TRUE(result1.success);

  container_result_t result2 =
      open_user_container("user2", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);
  TEST_ASSERT_TRUE(result2.success);

  container_result_t result3 =
      open_user_container("user3", TEST_DATA_DIR, TEST_CONTAINER_SIZE,
                          sys_result.container, sys_txn, true);
  TEST_ASSERT_TRUE(result3.success);

  db_abort_txn(sys_txn);

  container_close(result1.container);
  container_close(result2.container);
  container_close(result3.container);
  container_close(sys_result.container);
}

// ============= Main test runner =============

int main(void) {
  UNITY_BEGIN();

  // container_close tests
  RUN_TEST(test_container_close_null, true);
  RUN_TEST(test_container_close_user_container);
  RUN_TEST(test_container_close_system_container);

  // create_system_container tests
  RUN_TEST(test_create_system_container_success);
  RUN_TEST(test_create_system_container_invalid_data_dir);
  RUN_TEST(test_create_system_container_reopen);

  // create_user_container tests
  RUN_TEST(test_create_user_container_success);
  RUN_TEST(test_create_user_container_with_sys_txn);
  RUN_TEST(test_create_user_container_with_different_names);
  RUN_TEST(test_create_user_container_path_too_long);
  RUN_TEST(test_create_user_container_invalid_data_dir);
  RUN_TEST(test_create_user_container_null_system_container);
  RUN_TEST(test_create_user_container_null_name);
  RUN_TEST(test_create_user_container_null_data_dir);
  RUN_TEST(test_create_user_container_zero_size);
  RUN_TEST(test_create_user_container_reuses_provided_txn);

  // cdb_get_db_handle tests
  RUN_TEST(test_cdb_get_db_handle_null_container);
  RUN_TEST(test_cdb_get_db_handle_null_db_key);
  RUN_TEST(test_cdb_get_db_handle_null_output);
  RUN_TEST(test_cdb_get_db_handle_user_dbs);
  RUN_TEST(test_cdb_get_db_handle_system_dbs);
  RUN_TEST(test_cdb_get_db_handle_invalid_db_type);
  RUN_TEST(test_cdb_get_db_handle_index_db_null_key);

  // cdb_free_db_key_contents tests
  RUN_TEST(test_cdb_free_db_key_contents_null, true);
  RUN_TEST(test_cdb_free_db_key_contents_with_string_key);
  RUN_TEST(test_cdb_free_db_key_contents_with_int_key);
  RUN_TEST(test_cdb_free_db_key_contents_null_container_name);
  RUN_TEST(test_cdb_free_db_key_contents_with_index_key);
  RUN_TEST(test_cdb_free_db_key_contents_index_key_wrong_type);

  // Integration tests
  RUN_TEST(test_create_user_and_system_containers_together);
  RUN_TEST(test_multiple_user_containers);
  RUN_TEST(test_user_container_reopen);
  RUN_TEST(test_multiple_containers_with_shared_txn);

  return UNITY_END();
}