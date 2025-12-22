#include "core/db.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

// Test globals
static MDB_env *test_env = NULL;
static MDB_dbi test_db;
static char test_db_path[256];

// Helper function to create unique test database path
void create_test_db_path(void) {
  srand((unsigned int)time(NULL));
  snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_db_%d_%d.lmdb",
           getpid(), rand());
}

// Test fixture setup
void setUp(void) {
  create_test_db_path();

  // Create test environment
  test_env = db_create_env(test_db_path, 10 * 1024 * 1024, 10); // 10MB, 10 DBs
  TEST_ASSERT_NOT_NULL(test_env);

  // Open test database
  bool result = db_open(test_env, "test_db", &test_db);
  TEST_ASSERT_TRUE(result);
}

// Test fixture teardown
void tearDown(void) {
  if (test_env) {
    db_close(test_env, test_db);
    db_env_close(test_env);
    test_env = NULL;
  }

  // Clean up test file
  unlink(test_db_path);
}

// Test db_create_env
void test_db_create_env_success(void) {
  char path[256];
  snprintf(path, sizeof(path), "/tmp/test_env_%d.lmdb", rand());

  MDB_env *env = db_create_env(path, 1024 * 1024, 5);
  TEST_ASSERT_NOT_NULL(env);

  db_env_close(env);
  unlink(path);
}

void test_db_create_env_null_path(void) {
  MDB_env *env = db_create_env(NULL, 1024 * 1024, 5);
  TEST_ASSERT_NULL(env);
}

void test_db_create_env_zero_map_size(void) {
  char path[256];
  snprintf(path, sizeof(path), "/tmp/test_env_zero_%d.lmdb", rand());

  MDB_env *env = db_create_env(path, 0, 5);
  // Should fail with zero map size
  TEST_ASSERT_NULL(env);

  unlink(path);
}

// Test db_open
void test_db_open_success(void) {
  MDB_dbi db;
  bool result = db_open(test_env, "new_test_db", &db);
  TEST_ASSERT_TRUE(result);

  db_close(test_env, db);
}

void test_db_open_null_env(void) {
  MDB_dbi db;
  bool result = db_open(NULL, "test_db", &db);
  TEST_ASSERT_FALSE(result);
}

void test_db_open_null_db_out(void) {
  bool result = db_open(test_env, "test_db", NULL);
  TEST_ASSERT_FALSE(result);
}

// Test db_create_txn
void test_db_create_txn_read_write(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  db_abort_txn(txn);
}

void test_db_create_txn_read_only(void) {
  MDB_txn *txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(txn);

  db_abort_txn(txn);
}

void test_db_create_txn_null_env(void) {
  MDB_txn *txn = db_create_txn(NULL, false);
  TEST_ASSERT_NULL(txn);
}

// Test db_put with string keys
void test_db_put_string_key_success(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "test_key"};
  const char *value = "test_value";

  bool result = db_put(test_db, txn, &key, value, strlen(value), false);
  TEST_ASSERT_TRUE(result);

  bool commit_result = db_commit_txn(txn);
  TEST_ASSERT_TRUE(commit_result);
}

void test_db_put_string_key_auto_commit(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "auto_commit_key"};
  const char *value = "auto_commit_value";

  bool result = db_put(test_db, txn, &key, value, strlen(value), true);
  TEST_ASSERT_TRUE(result);
}

void test_db_put_integer_key_success(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = DB_KEY_U32, .key.i = 42};
  const char *value = "integer_key_value";

  bool result = db_put(test_db, txn, &key, value, strlen(value), false);
  TEST_ASSERT_TRUE(result);

  bool commit_result = db_commit_txn(txn);
  TEST_ASSERT_TRUE(commit_result);
}

void test_db_put_null_txn(void) {
  db_key_t key = {.type = DB_KEY_STRING, .key.s = "test_key"};
  const char *value = "test_value";

  bool result = db_put(test_db, NULL, &key, value, strlen(value), false);
  TEST_ASSERT_FALSE(result);
}

void test_db_put_null_key(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  const char *value = "test_value";

  bool result = db_put(test_db, txn, NULL, value, strlen(value), false);
  TEST_ASSERT_FALSE(result);

  db_abort_txn(txn);
}

void test_db_put_invalid_key_type(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = 99, .key.s = "invalid_type"}; // Invalid type
  const char *value = "test_value";

  bool result = db_put(test_db, txn, &key, value, strlen(value), false);
  TEST_ASSERT_FALSE(result);

  db_abort_txn(txn);
}

// Test db_get with string keys
void test_db_get_string_key_found(void) {
  // First put a value
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "get_test_key"};
  const char *expected_value = "get_test_value";

  bool put_result = db_put(test_db, put_txn, &key, expected_value,
                           strlen(expected_value), true);
  TEST_ASSERT_TRUE(put_result);

  // Now try to get it
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
  TEST_ASSERT_NOT_NULL(result.value);
  TEST_ASSERT_EQUAL(strlen(expected_value), result.value_len);
  TEST_ASSERT_EQUAL_MEMORY(expected_value, result.value, result.value_len);

  db_get_result_clear(&result);
  db_abort_txn(get_txn);
}

void test_db_get_string_key_not_found(void) {
  MDB_txn *txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "nonexistent_key"};
  db_get_result_t result;

  bool get_result = db_get(test_db, txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_NOT_FOUND, result.status);
  TEST_ASSERT_NULL(result.value);
  TEST_ASSERT_EQUAL(0, result.value_len);

  db_abort_txn(txn);
}

void test_db_get_integer_key_found(void) {
  // First put a value
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  db_key_t key = {.type = DB_KEY_U32, .key.i = 123};
  const char *expected_value = "integer_value";

  bool put_result = db_put(test_db, put_txn, &key, expected_value,
                           strlen(expected_value), true);
  TEST_ASSERT_TRUE(put_result);

  // Now try to get it
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
  TEST_ASSERT_NOT_NULL(result.value);
  TEST_ASSERT_EQUAL(strlen(expected_value), result.value_len);
  TEST_ASSERT_EQUAL_MEMORY(expected_value, result.value, result.value_len);

  db_get_result_clear(&result);
  db_abort_txn(get_txn);
}

void test_db_get_null_inputs(void) {
  MDB_txn *txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "test_key"};
  db_get_result_t result;

  // Test null txn
  bool result1 = db_get(test_db, NULL, &key, &result);
  TEST_ASSERT_FALSE(result1);

  // Test null key
  bool result2 = db_get(test_db, txn, NULL, &result);
  TEST_ASSERT_FALSE(result2);

  // Test null result_out
  bool result3 = db_get(test_db, txn, &key, NULL);
  TEST_ASSERT_FALSE(result3);

  db_abort_txn(txn);
}

// Test binary data storage and retrieval
void test_db_put_get_binary_data(void) {
  // Put binary data
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "binary_key"};
  char binary_data[] = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD};
  size_t binary_size = sizeof(binary_data);

  bool put_result =
      db_put(test_db, put_txn, &key, binary_data, binary_size, true);
  TEST_ASSERT_TRUE(put_result);

  // Get binary data
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
  TEST_ASSERT_EQUAL(binary_size, result.value_len);
  TEST_ASSERT_EQUAL_MEMORY(binary_data, result.value, binary_size);

  db_get_result_clear(&result);
  db_abort_txn(get_txn);
}

// Test large data storage
void test_db_put_get_large_data(void) {
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "large_data_key"};

  // Create large data (1KB)
  char *large_data = malloc(1024);
  TEST_ASSERT_NOT_NULL(large_data);

  for (int i = 0; i < 1024; i++) {
    large_data[i] = (char)(i % 256);
  }

  bool put_result = db_put(test_db, put_txn, &key, large_data, 1024, true);
  TEST_ASSERT_TRUE(put_result);

  // Get large data
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
  TEST_ASSERT_EQUAL(1024, result.value_len);
  TEST_ASSERT_EQUAL_MEMORY(large_data, result.value, 1024);

  free(large_data);
  db_get_result_clear(&result);
  db_abort_txn(get_txn);
}

// Test integer key ordering
void test_db_integer_key_ordering(void) {
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  // Put values with different integer keys
  uint32_t keys[] = {100, 50, 200, 1, 999};
  const char *values[] = {"val100", "val50", "val200", "val1", "val999"};
  size_t num_keys = sizeof(keys) / sizeof(keys[0]);

  for (size_t i = 0; i < num_keys; i++) {
    db_key_t key = {.type = DB_KEY_U32, .key.i = keys[i]};
    bool result =
        db_put(test_db, put_txn, &key, values[i], strlen(values[i]), false);
    TEST_ASSERT_TRUE(result);
  }

  bool commit_result = db_commit_txn(put_txn);
  TEST_ASSERT_TRUE(commit_result);

  // Verify we can retrieve all values
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  for (size_t i = 0; i < num_keys; i++) {
    db_key_t key = {.type = DB_KEY_U32, .key.i = keys[i]};
    db_get_result_t result;

    bool get_result = db_get(test_db, get_txn, &key, &result);
    TEST_ASSERT_TRUE(get_result);
    TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
    TEST_ASSERT_EQUAL_MEMORY(values[i], result.value, strlen(values[i]));

    db_get_result_clear(&result);
  }

  db_abort_txn(get_txn);
}

// Test overwriting values
void test_db_put_overwrite_value(void) {
  MDB_txn *txn1 = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn1);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "overwrite_key"};
  const char *value1 = "original_value";

  bool put_result1 = db_put(test_db, txn1, &key, value1, strlen(value1), true);
  TEST_ASSERT_TRUE(put_result1);

  // Overwrite with new value
  MDB_txn *txn2 = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn2);

  const char *value2 = "new_value";
  bool put_result2 = db_put(test_db, txn2, &key, value2, strlen(value2), true);
  TEST_ASSERT_TRUE(put_result2);

  // Verify new value
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);
  TEST_ASSERT_EQUAL_MEMORY(value2, result.value, strlen(value2));

  db_get_result_clear(&result);
  db_abort_txn(get_txn);
}

// Test db_get_result_clear
void test_db_get_result_clear_null(void) {
  // Should not crash
  db_get_result_clear(NULL);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

void test_db_get_result_clear_valid(void) {
  // Put and get a value to create a valid result
  MDB_txn *put_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(put_txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "free_test_key"};
  const char *value = "free_test_value";

  bool put_result = db_put(test_db, put_txn, &key, value, strlen(value), true);
  TEST_ASSERT_TRUE(put_result);

  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_OK, result.status);

  // Should not crash when freeing valid result
  db_get_result_clear(&result);
  TEST_ASSERT_TRUE(true);

  db_abort_txn(get_txn);
}

// Test transaction abort
void test_db_abort_txn_null(void) {
  // Should not crash
  db_abort_txn(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_db_abort_txn_valid(void) {
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  // Should not crash
  db_abort_txn(txn);
  TEST_ASSERT_TRUE(true);
}

// Test transaction rollback behavior
void test_db_transaction_rollback(void) {
  // Put a value but abort the transaction
  MDB_txn *abort_txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(abort_txn);

  db_key_t key = {.type = DB_KEY_STRING, .key.s = "rollback_key"};
  const char *value = "rollback_value";

  bool put_result =
      db_put(test_db, abort_txn, &key, value, strlen(value), false);
  TEST_ASSERT_TRUE(put_result);

  // Abort instead of commit
  db_abort_txn(abort_txn);

  // Verify value was not stored
  MDB_txn *get_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(get_txn);

  db_get_result_t result;
  bool get_result = db_get(test_db, get_txn, &key, &result);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL(DB_GET_NOT_FOUND, result.status);

  db_abort_txn(get_txn);
}

// Test db_close and db_env_close with null pointers
void test_db_close_null_inputs(void) {
  // Should not crash
  db_close(NULL, test_db);
  db_close(test_env, 0); // 0 is invalid dbi but should handle gracefully
  TEST_ASSERT_TRUE(true);
}

void test_db_env_close_null(void) {
  // Should not crash
  db_env_close(NULL);
  TEST_ASSERT_TRUE(true);
}

// Main test runner
int main(void) {
  UNITY_BEGIN();

  // db_create_env tests
  RUN_TEST(test_db_create_env_success);
  RUN_TEST(test_db_create_env_null_path);
  RUN_TEST(test_db_create_env_zero_map_size);

  // db_open tests
  RUN_TEST(test_db_open_success);
  RUN_TEST(test_db_open_null_env);
  RUN_TEST(test_db_open_null_db_out);

  // db_create_txn tests
  RUN_TEST(test_db_create_txn_read_write);
  RUN_TEST(test_db_create_txn_read_only);
  RUN_TEST(test_db_create_txn_null_env);

  // db_put tests
  RUN_TEST(test_db_put_string_key_success);
  RUN_TEST(test_db_put_string_key_auto_commit);
  RUN_TEST(test_db_put_integer_key_success);
  RUN_TEST(test_db_put_null_txn);
  RUN_TEST(test_db_put_null_key);
  RUN_TEST(test_db_put_invalid_key_type);

  // db_get tests
  RUN_TEST(test_db_get_string_key_found);
  RUN_TEST(test_db_get_string_key_not_found);
  RUN_TEST(test_db_get_integer_key_found);
  RUN_TEST(test_db_get_null_inputs);

  // Advanced functionality tests
  RUN_TEST(test_db_put_get_binary_data);
  RUN_TEST(test_db_put_get_large_data);
  RUN_TEST(test_db_integer_key_ordering);
  RUN_TEST(test_db_put_overwrite_value);

  // Memory management tests
  RUN_TEST(test_db_get_result_clear_null);
  RUN_TEST(test_db_get_result_clear_valid);

  // Transaction tests
  RUN_TEST(test_db_abort_txn_null);
  RUN_TEST(test_db_abort_txn_valid);
  RUN_TEST(test_db_transaction_rollback);

  // Cleanup tests
  RUN_TEST(test_db_close_null_inputs);
  RUN_TEST(test_db_env_close_null);

  return UNITY_END();
}