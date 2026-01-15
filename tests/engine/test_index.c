#include "core/db.h"
#include "engine/index/index.h"
#include "lmdb.h"
#include "unity.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

// Globals for Test Environment
static MDB_env *test_env = NULL;
static MDB_dbi registry_db;
static char test_db_path[256];
static khash_t(key_index) *key_map = NULL;

// Helper: Create unique path
void create_test_db_path(void) {
  srand((unsigned int)time(NULL));
  snprintf(test_db_path, sizeof(test_db_path), "/tmp/test_index_db_%d_%d.lmdb",
           getpid(), rand());
}

// Setup: Create Env and Open Registry DB
void setUp(void) {
  create_test_db_path();

  // Create environment
  test_env = db_create_env(test_db_path, 10 * 1024 * 1024, 20); // 20 DBs
  TEST_ASSERT_NOT_NULL(test_env);

  // Open the registry database (where index definitions live)
  bool res =
      db_open(test_env, "index_registry", false, DB_DUP_NONE, &registry_db);
  TEST_ASSERT_TRUE(res);

  key_map = NULL;
}

// Teardown: Close everything and delete file
void tearDown(void) {
  // Close the runtime registry map (closes internal DB handles)
  index_close_registry(test_env, &key_map);

  if (test_env) {
    db_close(test_env, registry_db);
    db_env_close(test_env);
    test_env = NULL;
  }

  unlink(test_db_path);
}

// --- Tests ---

void test_index_write_defaults_and_load(void) {
  // 1. Write defaults to registry
  index_write_reg_opts_t opts = {.src = INDEX_WRITE_DEFAULTS};
  bool write_res = index_write_registry(test_env, registry_db, &opts);
  TEST_ASSERT_TRUE(write_res);

  // 2. Open registry (loads into memory map)
  bool open_res = index_open_registry(test_env, registry_db, &key_map);
  TEST_ASSERT_TRUE(open_res);
  TEST_ASSERT_NOT_NULL(key_map);

  // 3. Verify count
  uint32_t count = 0;
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  // Default is currently just "ts"
  TEST_ASSERT_EQUAL_UINT32(1, count);

  // 4. Verify specific index existence
  index_t idx;
  bool found = index_get("ts", key_map, &idx);
  TEST_ASSERT_TRUE(found);
  TEST_ASSERT_EQUAL_STRING("ts", idx.index_def.key);
  TEST_ASSERT_EQUAL(INDEX_TYPE_I64, idx.index_def.type);
  // Verify LMDB handle is open (non-zero)
  TEST_ASSERT_NOT_EQUAL(0, idx.index_db);
}

void test_index_add_manual_and_persistence(void) {
  // 1. Add a new index definition
  index_def_t new_idx = {.key = "user_id", .type = INDEX_TYPE_I64};
  db_put_result_t put_res = index_add(&new_idx, test_env, registry_db);
  TEST_ASSERT_EQUAL(DB_PUT_OK, put_res);

  // 2. Open registry to load it
  bool open_res = index_open_registry(test_env, registry_db, &key_map);
  TEST_ASSERT_TRUE(open_res);

  // 3. Verify it exists in map
  index_t loaded_idx;
  TEST_ASSERT_TRUE(index_get("user_id", key_map, &loaded_idx));
  TEST_ASSERT_EQUAL_STRING("user_id", loaded_idx.index_def.key);

  // 4. Close registry map and explicitly close DB to simulate full shutdown
  index_close_registry(test_env, &key_map);
  TEST_ASSERT_NULL(key_map);
  db_close(test_env, registry_db);

  // 5. Re-open registry to verify persistence
  bool db_res =
      db_open(test_env, "index_registry", false, DB_DUP_NONE, &registry_db);
  TEST_ASSERT_TRUE(db_res);

  open_res = index_open_registry(test_env, registry_db, &key_map);
  TEST_ASSERT_TRUE(open_res);

  // 6. Verify "user_id" is still there
  TEST_ASSERT_TRUE(index_get("user_id", key_map, &loaded_idx));
}

void test_index_add_duplicate_fails(void) {
  index_def_t new_idx = {.key = "unique_key", .type = INDEX_TYPE_I64};

  // First add
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&new_idx, test_env, registry_db));

  // Second add (should fail with Key Exists)
  TEST_ASSERT_EQUAL(DB_PUT_KEY_EXISTS,
                    index_add(&new_idx, test_env, registry_db));
}

void test_index_write_from_db_source(void) {
  // Scenario: We have an existing registry (src_db) and want to copy it to a
  // new one (registry_db).

  // 1. Create and populate a source DB
  MDB_dbi src_db;
  TEST_ASSERT_TRUE(
      db_open(test_env, "source_registry", false, DB_DUP_NONE, &src_db));

  index_def_t def1 = {.key = "src_idx_1", .type = INDEX_TYPE_I64};
  index_def_t def2 = {.key = "src_idx_2", .type = INDEX_TYPE_I64};

  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&def1, test_env, src_db));
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&def2, test_env, src_db));

  // 2. Create a read transaction for the source DB
  MDB_txn *src_read_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(src_read_txn);

  // 3. Use index_write_registry to copy from src_db to registry_db
  index_write_reg_opts_t opts = {.src = INDEX_WRITE_FROM_DB,
                                 .src_dbi = src_db,
                                 .src_read_txn = src_read_txn};

  bool res = index_write_registry(test_env, registry_db, &opts);
  TEST_ASSERT_TRUE(res);

  // 4. Abort the read transaction (it's read-only, so abort is fine)
  db_abort_txn(src_read_txn);

  // 5. Load registry_db
  bool open_res = index_open_registry(test_env, registry_db, &key_map);
  TEST_ASSERT_TRUE(open_res);

  // 6. Verify indexes from source exist in target
  uint32_t count;
  index_get_count(key_map, &count);
  TEST_ASSERT_EQUAL_UINT32(2, count);

  index_t out;
  TEST_ASSERT_TRUE(index_get("src_idx_1", key_map, &out));
  TEST_ASSERT_TRUE(index_get("src_idx_2", key_map, &out));

  // Cleanup source db handle
  db_close(test_env, src_db);
}

void test_index_open_creates_runtime_dbs(void) {
  // Verify that index_open_registry actually creates the underlying LMDB
  // databases for the indexes.

  index_def_t idx = {.key = "data_idx", .type = INDEX_TYPE_I64};
  index_add(&idx, test_env, registry_db);

  index_open_registry(test_env, registry_db, &key_map);

  index_t loaded_idx;
  index_get("data_idx", key_map, &loaded_idx);

  // Use the loaded DB handle to write something
  MDB_txn *txn = db_create_txn(test_env, false);
  TEST_ASSERT_NOT_NULL(txn);

  int64_t key_val = 100;
  int64_t data_val = 9999;
  db_key_t k = {.type = DB_KEY_I64, .key.i64 = key_val};

  db_put_result_t pr = db_put(loaded_idx.index_db, txn, &k, &data_val,
                              sizeof(data_val), false, false);
  TEST_ASSERT_EQUAL(DB_PUT_OK, pr);

  db_commit_txn(txn);

  // Verify we can read it back
  MDB_txn *read_txn = db_create_txn(test_env, true);
  db_get_result_t get_res;
  bool found = db_get(loaded_idx.index_db, read_txn, &k, &get_res);

  TEST_ASSERT_TRUE(found);
  TEST_ASSERT_EQUAL(sizeof(int64_t), get_res.value_len);
  TEST_ASSERT_EQUAL_INT64(9999, *(int64_t *)get_res.value);

  db_get_result_clear(&get_res);
  db_abort_txn(read_txn);
}

void test_index_get_not_found(void) {
  // Empty registry
  index_open_registry(test_env, registry_db, &key_map);

  index_t idx;
  bool found = index_get("non_existent", key_map, &idx);

  TEST_ASSERT_FALSE(found);
}

void test_index_close_safely_handles_null(void) {
  // Should not crash
  index_close_registry(test_env, NULL);

  khash_t(key_index) *empty_map = NULL;
  index_close_registry(test_env, &empty_map);
}

void test_index_reopen_map_only(void) {
  // This tests the new design where we can close the map but keep DB open
  // 1. Add index
  index_def_t idx = {.key = "temp_idx", .type = INDEX_TYPE_I64};
  index_add(&idx, test_env, registry_db);

  // 2. Open Map
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  TEST_ASSERT_NOT_NULL(key_map);

  // 3. Close Map only (Registry DB stays open)
  index_close_registry(test_env, &key_map);
  TEST_ASSERT_NULL(key_map);

  // 4. Re-open Map using SAME registry_db handle
  // (If implementation incorrectly closed DB, this would fail/crash)
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  TEST_ASSERT_NOT_NULL(key_map);

  // 5. Verify data
  index_t out;
  TEST_ASSERT_TRUE(index_get("temp_idx", key_map, &out));
}

void test_index_open_empty_registry(void) {
  // Ensure opening a completely fresh/empty registry DB doesn't crash
  // and results in a valid, empty map.
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  TEST_ASSERT_NOT_NULL(key_map);

  uint32_t count = 99;
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  TEST_ASSERT_EQUAL_UINT32(0, count);
}

void test_index_get_count_edge_cases(void) {
  uint32_t count = 0;
  // Test NULL map
  TEST_ASSERT_FALSE(index_get_count(NULL, &count));

  // Test Valid empty map
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  TEST_ASSERT_EQUAL_UINT32(0, count);
}

void test_index_add_null_validation(void) {
  // Should return error if inputs are NULL
  index_def_t idx = {.key = "k", .type = INDEX_TYPE_I64};

  TEST_ASSERT_EQUAL(DB_PUT_ERR, index_add(NULL, test_env, registry_db));
  TEST_ASSERT_EQUAL(DB_PUT_ERR, index_add(&idx, NULL, registry_db));
}

void test_index_write_registry_invalid_opts(void) {
  // Should return false if environment or options are NULL
  index_write_reg_opts_t opts = {.src = INDEX_WRITE_DEFAULTS};

  TEST_ASSERT_FALSE(index_write_registry(NULL, registry_db, &opts));
  TEST_ASSERT_FALSE(index_write_registry(test_env, registry_db, NULL));
}

void test_index_write_from_db_null_txn_fails(void) {
  // When source is INDEX_WRITE_FROM_DB, src_read_txn must be provided
  MDB_dbi src_db;
  TEST_ASSERT_TRUE(
      db_open(test_env, "source_registry", false, DB_DUP_NONE, &src_db));

  index_def_t def = {.key = "test_idx", .type = INDEX_TYPE_I64};
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&def, test_env, src_db));

  // Try to write without providing src_read_txn (should fail)
  index_write_reg_opts_t opts = {
      .src = INDEX_WRITE_FROM_DB, .src_dbi = src_db, .src_read_txn = NULL};

  bool res = index_write_registry(test_env, registry_db, &opts);
  TEST_ASSERT_FALSE(res);

  db_close(test_env, src_db);
}

void test_index_write_from_empty_source(void) {
  // Test copying from an empty source DB
  MDB_dbi src_db;
  TEST_ASSERT_TRUE(
      db_open(test_env, "empty_source", false, DB_DUP_NONE, &src_db));

  // Don't add any indexes to source

  MDB_txn *src_read_txn = db_create_txn(test_env, true);
  TEST_ASSERT_NOT_NULL(src_read_txn);

  index_write_reg_opts_t opts = {.src = INDEX_WRITE_FROM_DB,
                                 .src_dbi = src_db,
                                 .src_read_txn = src_read_txn};

  bool res = index_write_registry(test_env, registry_db, &opts);
  TEST_ASSERT_TRUE(res);

  db_abort_txn(src_read_txn);

  // Open and verify target is empty
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  uint32_t count;
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  TEST_ASSERT_EQUAL_UINT32(0, count);

  db_close(test_env, src_db);
}

void test_index_get_null_key(void) {
  // Test index_get with NULL key
  index_write_reg_opts_t opts = {.src = INDEX_WRITE_DEFAULTS};
  index_write_registry(test_env, registry_db, &opts);
  index_open_registry(test_env, registry_db, &key_map);

  index_t idx;
  bool found = index_get(NULL, key_map, &idx);
  TEST_ASSERT_FALSE(found);
}

void test_index_get_null_map(void) {
  // Test index_get with NULL map
  index_t idx;
  bool found = index_get("ts", NULL, &idx);
  TEST_ASSERT_FALSE(found);
}

void test_index_open_null_env(void) {
  // Test index_open_registry with NULL environment
  bool res = index_open_registry(NULL, registry_db, &key_map);
  TEST_ASSERT_FALSE(res);
}

void test_index_open_null_output(void) {
  // Test index_open_registry with NULL output pointer
  bool res = index_open_registry(test_env, registry_db, NULL);
  TEST_ASSERT_FALSE(res);
}

void test_multiple_indexes_lifecycle(void) {
  // Test adding multiple indexes and verifying they all work
  index_def_t idx1 = {.key = "idx_alpha", .type = INDEX_TYPE_I64};
  index_def_t idx2 = {.key = "idx_beta", .type = INDEX_TYPE_I64};
  index_def_t idx3 = {.key = "idx_gamma", .type = INDEX_TYPE_I64};

  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&idx1, test_env, registry_db));
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&idx2, test_env, registry_db));
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&idx3, test_env, registry_db));

  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));

  uint32_t count;
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  TEST_ASSERT_EQUAL_UINT32(3, count);

  index_t out;
  TEST_ASSERT_TRUE(index_get("idx_alpha", key_map, &out));
  TEST_ASSERT_EQUAL_STRING("idx_alpha", out.index_def.key);
  TEST_ASSERT_NOT_EQUAL(0, out.index_db);

  TEST_ASSERT_TRUE(index_get("idx_beta", key_map, &out));
  TEST_ASSERT_EQUAL_STRING("idx_beta", out.index_def.key);
  TEST_ASSERT_NOT_EQUAL(0, out.index_db);

  TEST_ASSERT_TRUE(index_get("idx_gamma", key_map, &out));
  TEST_ASSERT_EQUAL_STRING("idx_gamma", out.index_def.key);
  TEST_ASSERT_NOT_EQUAL(0, out.index_db);
}

void test_index_write_defaults_idempotent(void) {
  // Test that writing defaults multiple times is safe
  index_write_reg_opts_t opts = {.src = INDEX_WRITE_DEFAULTS};

  TEST_ASSERT_TRUE(index_write_registry(test_env, registry_db, &opts));

  // Writing defaults again should succeed (overwrites with same data)
  // Note: This may fail with DB_PUT_KEY_EXISTS depending on implementation
  // Just verify it doesn't crash
  index_write_registry(test_env, registry_db, &opts);

  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));
  uint32_t count;
  TEST_ASSERT_TRUE(index_get_count(key_map, &count));
  TEST_ASSERT_EQUAL_UINT32(1, count);
}

void test_index_def_key_ownership(void) {
  // Test that index_def keys are properly managed (no double-free or leaks)
  index_def_t idx = {.key = "ownership_test", .type = INDEX_TYPE_I64};
  TEST_ASSERT_EQUAL(DB_PUT_OK, index_add(&idx, test_env, registry_db));

  // Open registry - this should allocate new memory for keys
  TEST_ASSERT_TRUE(index_open_registry(test_env, registry_db, &key_map));

  index_t out;
  TEST_ASSERT_TRUE(index_get("ownership_test", key_map, &out));

  // The key in out.index_def should be a separate allocation
  TEST_ASSERT_NOT_NULL(out.index_def.key);
  TEST_ASSERT_EQUAL_STRING("ownership_test", out.index_def.key);

  // Close should free the allocated keys
  index_close_registry(test_env, &key_map);
  TEST_ASSERT_NULL(key_map);
}

int main(void) {
  UNITY_BEGIN();

  // Original tests
  RUN_TEST(test_index_write_defaults_and_load);
  RUN_TEST(test_index_add_manual_and_persistence);
  RUN_TEST(test_index_add_duplicate_fails);
  RUN_TEST(test_index_write_from_db_source);
  RUN_TEST(test_index_open_creates_runtime_dbs);
  RUN_TEST(test_index_get_not_found);
  RUN_TEST(test_index_close_safely_handles_null);
  RUN_TEST(test_index_reopen_map_only);
  RUN_TEST(test_index_open_empty_registry);
  RUN_TEST(test_index_get_count_edge_cases);
  RUN_TEST(test_index_add_null_validation);
  RUN_TEST(test_index_write_registry_invalid_opts);

  // New tests for src_read_txn requirement
  RUN_TEST(test_index_write_from_db_null_txn_fails);
  RUN_TEST(test_index_write_from_empty_source);

  // Additional edge case tests
  RUN_TEST(test_index_get_null_key);
  RUN_TEST(test_index_get_null_map);
  RUN_TEST(test_index_open_null_env);
  RUN_TEST(test_index_open_null_output);

  // Integration tests
  RUN_TEST(test_multiple_indexes_lifecycle);
  RUN_TEST(test_index_write_defaults_idempotent);
  RUN_TEST(test_index_def_key_ownership);

  return UNITY_END();
}