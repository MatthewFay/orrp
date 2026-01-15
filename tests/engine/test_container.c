#include "core/db.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "unity.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Test configuration
#define TEST_DATA_DIR "test_data"
#define TEST_CACHE_CAPACITY 5
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
  const char *user_names[] = {
      "test_user",   "test",        "cached",
      "c1",          "c2",          "c3",
      "persistent",  "shared",      "user1",
      "user2",       "user3",       "with_txn",
      "without_txn", "txn_test_1",  "txn_test_2",
      "txn_test_3",  "container_0", "container_1",
      "container_2", "container_3", "container_4",
      "rapid_0",     "rapid_1",     "rapid_2",
      "rapid_3",     "rapid_4",     "rapid_5",
      "rapid_6",     "rapid_7",     "rapid_8",
      "rapid_9",     "rapid_10",    "rapid_11",
      "rapid_12",    "rapid_13",    "rapid_14",
      "rapid_15",    "rapid_16",    "rapid_17",
      "rapid_18",    "rapid_19",    "user_with-dash.dot",
      NULL};

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

void tearDown(void) {
  container_shutdown();
  remove_test_files();
}

// ============= Initialization tests =============

void test_container_init_success(void) {
  bool result =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result);

  // Verify system container was created
  container_result_t sys_result = container_get_system();
  TEST_ASSERT_TRUE(sys_result.success);
  TEST_ASSERT_NOT_NULL(sys_result.container);
}

void test_container_init_double_init_fails(void) {
  bool first =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(first);

  bool second =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_FALSE(second);
}

void test_container_init_null_data_dir_fails(void) {
  bool result = container_init(TEST_CACHE_CAPACITY, NULL, TEST_CONTAINER_SIZE);
  TEST_ASSERT_FALSE(result);
}

void test_container_init_zero_capacity_fails(void) {
  bool result = container_init(0, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_FALSE(result);
}

void test_container_init_zero_size_fails(void) {
  bool result = container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, 0);
  TEST_ASSERT_FALSE(result);
}

void test_container_shutdown_without_init(void) {
  // Should not crash
  container_shutdown();
  TEST_ASSERT_TRUE(true);
}

void test_container_shutdown_idempotent(void) {
  bool result =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result);

  container_shutdown();
  container_shutdown(); // Second call should be safe
  TEST_ASSERT_TRUE(true);
}

// ============= System container tests =============

void test_get_system_container_success(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_system();

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYS, result.container->type);
  TEST_ASSERT_NOT_NULL(result.container->name);
  TEST_ASSERT_EQUAL_STRING(SYS_CONTAINER_NAME, result.container->name);
  TEST_ASSERT_NULL(result.error_msg);
}

void test_get_system_container_without_init(void) {
  container_result_t result = container_get_system();

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_NOT_INITIALIZED, result.error_code);
  TEST_ASSERT_NOT_NULL(result.error_msg);
}

void test_get_system_container_multiple_times(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result1 = container_get_system();
  container_result_t result2 = container_get_system();

  TEST_ASSERT_TRUE(result1.success);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_EQUAL_PTR(result1.container, result2.container);
}

void test_system_container_has_all_databases(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_system();
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  db_key.sys_db_type = SYS_DB_INT_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.sys_db_type = SYS_DB_STR_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.sys_db_type = SYS_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));
}

// ============= User container tests =============

void test_get_user_container_success(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test_user", NULL);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USR, result.container->type);
  TEST_ASSERT_NOT_NULL(result.container->name);
  TEST_ASSERT_EQUAL_STRING("test_user", result.container->name);
  TEST_ASSERT_NULL(result.error_msg);

  container_release(result.container);
}

void test_get_user_container_with_sys_txn(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // Get system container and create a read transaction
  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);

  MDB_txn *sys_txn = db_create_txn(sys.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);

  // Create user container with explicit transaction
  container_result_t result = container_get_or_create_user("with_txn", sys_txn);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL_STRING("with_txn", result.container->name);

  db_abort_txn(sys_txn);
  container_release(result.container);
}

void test_get_user_container_without_sys_txn(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // Create user container without explicit transaction
  container_result_t result = container_get_or_create_user("without_txn", NULL);

  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL_STRING("without_txn", result.container->name);

  container_release(result.container);
}

void test_get_user_container_txn_reuse(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);

  // Create one transaction to reuse
  MDB_txn *sys_txn = db_create_txn(sys.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);

  // Create multiple containers with same transaction
  container_result_t r1 = container_get_or_create_user("txn_test_1", sys_txn);
  TEST_ASSERT_TRUE(r1.success);

  container_result_t r2 = container_get_or_create_user("txn_test_2", sys_txn);
  TEST_ASSERT_TRUE(r2.success);

  container_result_t r3 = container_get_or_create_user("txn_test_3", sys_txn);
  TEST_ASSERT_TRUE(r3.success);

  db_abort_txn(sys_txn);

  container_release(r1.container);
  container_release(r2.container);
  container_release(r3.container);
}

void test_get_user_container_cached_no_txn_needed(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // First access creates the container (txn will be used if new)
  container_result_t r1 = container_get_or_create_user("cached", NULL);
  TEST_ASSERT_TRUE(r1.success);
  container_release(r1.container);

  // Second access gets from cache (no txn needed since not creating)
  container_result_t r2 = container_get_or_create_user("cached", NULL);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_EQUAL_PTR(r1.container, r2.container);
  container_release(r2.container);
}

void test_get_user_container_without_init(void) {
  container_result_t result = container_get_or_create_user("test_user", NULL);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_NOT_INITIALIZED, result.error_code);
}

void test_get_user_container_null_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user(NULL, NULL);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_INVALID_NAME, result.error_code);
}

void test_get_user_container_empty_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("", NULL);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_INVALID_NAME, result.error_code);
}

void test_get_user_container_system_name_rejected(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result =
      container_get_or_create_user(SYS_CONTAINER_NAME, NULL);

  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_INVALID_NAME, result.error_code);
}

void test_user_container_has_all_databases(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test_user", NULL);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.usr_db_type = USR_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.usr_db_type = USR_DB_EVENTS;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  container_release(result.container);
}

void test_user_container_persists_across_restarts(void) {
  // First session: create container
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t result1 = container_get_or_create_user("persistent", NULL);
  TEST_ASSERT_TRUE(result1.success);
  container_release(result1.container);
  container_shutdown();

  // Second session: access same container
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t result2 = container_get_or_create_user("persistent", NULL);
  TEST_ASSERT_TRUE(result2.success);
  TEST_ASSERT_EQUAL_STRING("persistent", result2.container->name);
  container_release(result2.container);
}

// ============= Caching tests =============

void test_container_cached_on_second_access(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result1 = container_get_or_create_user("cached", NULL);
  TEST_ASSERT_TRUE(result1.success);
  eng_container_t *first_ptr = result1.container;
  container_release(result1.container);

  container_result_t result2 = container_get_or_create_user("cached", NULL);
  TEST_ASSERT_TRUE(result2.success);

  // Should be the same pointer (cached)
  TEST_ASSERT_EQUAL_PTR(first_ptr, result2.container);
  container_release(result2.container);
}

void test_cache_capacity_respected(void) {
  container_init(3, TEST_DATA_DIR, TEST_CONTAINER_SIZE); // Small cache

  container_result_t r1 = container_get_or_create_user("c1", NULL);
  container_result_t r2 = container_get_or_create_user("c2", NULL);
  container_result_t r3 = container_get_or_create_user("c3", NULL);

  TEST_ASSERT_TRUE(r1.success);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_TRUE(r3.success);

  container_release(r1.container);
  container_release(r2.container);
  container_release(r3.container);
}

void test_lru_eviction(void) {
  container_init(2, TEST_DATA_DIR, TEST_CONTAINER_SIZE); // Cache of 2

  container_result_t r1 = container_get_or_create_user("c1", NULL);
  container_release(r1.container);
  eng_container_t *c1_ptr = r1.container;

  container_result_t r2 = container_get_or_create_user("c2", NULL);
  container_release(r2.container);

  // Access c3, should evict c1 (LRU)
  container_result_t r3 = container_get_or_create_user("c3", NULL);
  container_release(r3.container);

  // Access c1 again - should be a new pointer (was evicted)
  container_result_t r1_new = container_get_or_create_user("c1", NULL);
  TEST_ASSERT_NOT_EQUAL(c1_ptr, r1_new.container);
  container_release(r1_new.container);
}

void test_container_with_references_not_evicted(void) {
  container_init(2, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t r1 = container_get_or_create_user("c1", NULL);
  // Don't release r1 - keep reference

  container_result_t r2 = container_get_or_create_user("c2", NULL);
  container_release(r2.container);

  container_result_t r3 = container_get_or_create_user("c3", NULL);
  container_release(r3.container);

  // c1 should still be valid (has reference)
  TEST_ASSERT_NOT_NULL(r1.container);
  TEST_ASSERT_EQUAL_STRING("c1", r1.container->name);

  container_release(r1.container);
}

void test_multiple_references_to_same_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t r1 = container_get_or_create_user("shared", NULL);
  container_result_t r2 = container_get_or_create_user("shared", NULL);
  container_result_t r3 = container_get_or_create_user("shared", NULL);

  TEST_ASSERT_EQUAL_PTR(r1.container, r2.container);
  TEST_ASSERT_EQUAL_PTR(r2.container, r3.container);

  container_release(r1.container);
  container_release(r2.container);
  container_release(r3.container);
}

// ============= DB Handle Access tests =============

void test_get_db_handle_null_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = USR_DB_METADATA;

  bool result = container_get_db_handle(NULL, &db_key, &db_out);
  TEST_ASSERT_FALSE(result);
}

void test_get_db_handle_null_db_key(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test", NULL);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  bool get_result = container_get_db_handle(result.container, NULL, &db_out);
  TEST_ASSERT_FALSE(get_result);

  container_release(result.container);
}

void test_get_db_handle_null_output(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test", NULL);
  TEST_ASSERT_TRUE(result.success);

  eng_container_db_key_t db_key = {0};
  db_key.usr_db_type = USR_DB_METADATA;

  bool get_result = container_get_db_handle(result.container, &db_key, NULL);
  TEST_ASSERT_FALSE(get_result);

  container_release(result.container);
}

void test_all_user_db_types_accessible(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test", NULL);
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  db_key.usr_db_type = USR_DB_INVERTED_EVENT_INDEX;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.usr_db_type = USR_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.usr_db_type = USR_DB_EVENTS;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.usr_db_type = USR_DB_INDEX_REGISTRY_LOCAL;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  container_release(result.container);
}

void test_all_system_db_types_accessible(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_system();
  TEST_ASSERT_TRUE(result.success);

  MDB_dbi db_out;
  eng_container_db_key_t db_key = {0};

  db_key.sys_db_type = SYS_DB_STR_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.sys_db_type = SYS_DB_INT_TO_ENTITY_ID;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.sys_db_type = SYS_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));

  db_key.sys_db_type = SYS_DB_INDEX_REGISTRY_GLOBAL;
  TEST_ASSERT_TRUE(container_get_db_handle(result.container, &db_key, &db_out));
}

// ============= DB Key cleanup tests =============

void test_free_db_key_null(void) {
  // Should not crash
  container_free_db_key_contents(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_free_db_key_with_string_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_STRING;
  db_key.db_key.key.s = strdup("key");

  container_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

void test_free_db_key_with_int_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = 42;

  container_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

void test_free_db_key_with_index_key(void) {
  eng_container_db_key_t db_key = {0};
  db_key.dc_type = CONTAINER_TYPE_USR;
  db_key.usr_db_type = USR_DB_INDEX;
  db_key.container_name = strdup("test");
  db_key.db_key.type = DB_KEY_U32;
  db_key.db_key.key.u32 = 42;
  db_key.index_key = strdup("my_index");

  container_free_db_key_contents(&db_key);
  TEST_ASSERT_TRUE(true);
}

// ============= Thread safety tests =============

typedef struct {
  int thread_id;
  int num_operations;
} thread_arg_t;

static void *concurrent_access_thread(void *arg) {
  thread_arg_t *targ = (thread_arg_t *)arg;

  for (int i = 0; i < targ->num_operations; i++) {
    char name[32];
    snprintf(name, sizeof(name), "container_%d", i % 5);

    container_result_t result = container_get_or_create_user(name, NULL);
    if (result.success) {
      usleep(100); // Small delay
      container_release(result.container);
    }
  }

  return NULL;
}

void test_concurrent_access(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  pthread_t threads[4];
  thread_arg_t args[4];

  for (int i = 0; i < 4; i++) {
    args[i].thread_id = i;
    args[i].num_operations = 10;
    pthread_create(&threads[i], NULL, concurrent_access_thread, &args[i]);
  }

  for (int i = 0; i < 4; i++) {
    pthread_join(threads[i], NULL);
  }

  TEST_ASSERT_TRUE(true);
}

static void *concurrent_same_container_thread(void *arg) {
  thread_arg_t *targ = (thread_arg_t *)arg;

  for (int i = 0; i < targ->num_operations; i++) {
    container_result_t result = container_get_or_create_user("shared", NULL);
    if (result.success) {
      usleep(50);
      container_release(result.container);
    }
  }

  return NULL;
}

void test_concurrent_same_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  pthread_t threads[4];
  thread_arg_t args[4];

  for (int i = 0; i < 4; i++) {
    args[i].thread_id = i;
    args[i].num_operations = 20;
    pthread_create(&threads[i], NULL, concurrent_same_container_thread,
                   &args[i]);
  }

  for (int i = 0; i < 4; i++) {
    pthread_join(threads[i], NULL);
  }

  TEST_ASSERT_TRUE(true);
}

// ============= Edge case tests =============

void test_very_long_container_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  char long_name[300];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';

  container_result_t result = container_get_or_create_user(long_name, NULL);

  // Should fail due to path length
  TEST_ASSERT_FALSE(result.success);
}

void test_special_characters_in_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result =
      container_get_or_create_user("user_with-dash.dot", NULL);
  TEST_ASSERT_TRUE(result.success);
  container_release(result.container);
}

void test_rapid_create_and_release(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  for (int i = 0; i < 20; i++) {
    char name[32];
    snprintf(name, sizeof(name), "rapid_%d", i);
    container_result_t result = container_get_or_create_user(name, NULL);
    TEST_ASSERT_TRUE(result.success);
    container_release(result.container);
  }
  TEST_ASSERT_TRUE(true);
}
void test_release_null_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  // Should not crash
  container_release(NULL);
  TEST_ASSERT_TRUE(true);
}
void test_release_without_init(void) {
  container_result_t result = container_get_or_create_user("test", NULL);
  // Should not crash even though container_get failed
  container_release(result.container);
  TEST_ASSERT_TRUE(true);
}
void test_system_container_not_released(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);
  // Releasing system container should be safe (no-op)
  container_release(sys.container);
  // System container should still be accessible
  container_result_t sys2 = container_get_system();
  TEST_ASSERT_TRUE(sys2.success);
  TEST_ASSERT_EQUAL_PTR(sys.container, sys2.container);
}
// ============= Integration tests =============
void test_full_lifecycle(void) {
  // Initialize
  bool init =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(init);
  // Get system container
  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);
  // Create multiple user containers
  container_result_t u1 = container_get_or_create_user("user1", NULL);
  container_result_t u2 = container_get_or_create_user("user2", NULL);
  TEST_ASSERT_TRUE(u1.success);
  TEST_ASSERT_TRUE(u2.success);
  // Access databases
  MDB_dbi db;
  eng_container_db_key_t db_key = {0};
  db_key.sys_db_type = SYS_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(sys.container, &db_key, &db));
  db_key.usr_db_type = USR_DB_METADATA;
  TEST_ASSERT_TRUE(container_get_db_handle(u1.container, &db_key, &db));
  // Release containers
  container_release(u1.container);
  container_release(u2.container);
  // Shutdown
  container_shutdown();
  TEST_ASSERT_TRUE(true);
}
void test_init_shutdown_reinit(void) {
  // First initialization
  bool init1 =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(init1);
  container_result_t u1 = container_get_or_create_user("user1", NULL);
  TEST_ASSERT_TRUE(u1.success);
  container_release(u1.container);
  container_shutdown();
  // Re-initialization should work
  bool init2 =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(init2);
  // Should be able to access same container
  container_result_t u2 = container_get_or_create_user("user1", NULL);
  TEST_ASSERT_TRUE(u2.success);
  TEST_ASSERT_EQUAL_STRING("user1", u2.container->name);
  container_release(u2.container);
}
void test_full_lifecycle_with_shared_txn(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);
  // Create one transaction for multiple container creations
  MDB_txn *sys_txn = db_create_txn(sys.container->env, true);
  TEST_ASSERT_NOT_NULL(sys_txn);
  container_result_t u1 = container_get_or_create_user("user1", sys_txn);
  container_result_t u2 = container_get_or_create_user("user2", sys_txn);
  container_result_t u3 = container_get_or_create_user("user3", sys_txn);
  TEST_ASSERT_TRUE(u1.success);
  TEST_ASSERT_TRUE(u2.success);
  TEST_ASSERT_TRUE(u3.success);
  db_abort_txn(sys_txn);
  container_release(u1.container);
  container_release(u2.container);
  container_release(u3.container);
}
// ============= Main test runner =============
int main(void) {
  UNITY_BEGIN();
  // Initialization tests
  RUN_TEST(test_container_init_success);
  RUN_TEST(test_container_init_double_init_fails);
  RUN_TEST(test_container_init_null_data_dir_fails);
  RUN_TEST(test_container_init_zero_capacity_fails);
  RUN_TEST(test_container_init_zero_size_fails);
  RUN_TEST(test_container_shutdown_without_init);
  RUN_TEST(test_container_shutdown_idempotent);
  // System container tests
  RUN_TEST(test_get_system_container_success);
  RUN_TEST(test_get_system_container_without_init);
  RUN_TEST(test_get_system_container_multiple_times);
  RUN_TEST(test_system_container_has_all_databases);
  // User container tests
  RUN_TEST(test_get_user_container_success);
  RUN_TEST(test_get_user_container_with_sys_txn);
  RUN_TEST(test_get_user_container_without_sys_txn);
  RUN_TEST(test_get_user_container_txn_reuse);
  RUN_TEST(test_get_user_container_cached_no_txn_needed);
  RUN_TEST(test_get_user_container_without_init);
  RUN_TEST(test_get_user_container_null_name);
  RUN_TEST(test_get_user_container_empty_name);
  RUN_TEST(test_get_user_container_system_name_rejected);
  RUN_TEST(test_user_container_has_all_databases);
  RUN_TEST(test_user_container_persists_across_restarts);
  // Caching tests
  RUN_TEST(test_container_cached_on_second_access);
  RUN_TEST(test_cache_capacity_respected);
  RUN_TEST(test_lru_eviction);
  RUN_TEST(test_container_with_references_not_evicted);
  RUN_TEST(test_multiple_references_to_same_container);
  // DB Handle Access tests
  RUN_TEST(test_get_db_handle_null_container);
  RUN_TEST(test_get_db_handle_null_db_key);
  RUN_TEST(test_get_db_handle_null_output);
  RUN_TEST(test_all_user_db_types_accessible);
  RUN_TEST(test_all_system_db_types_accessible);
  // DB Key cleanup tests
  RUN_TEST(test_free_db_key_null);
  RUN_TEST(test_free_db_key_with_string_key);
  RUN_TEST(test_free_db_key_with_int_key);
  RUN_TEST(test_free_db_key_with_index_key);
  // Thread safety tests
  RUN_TEST(test_concurrent_access);
  RUN_TEST(test_concurrent_same_container);
  // Edge case tests
  RUN_TEST(test_very_long_container_name);
  RUN_TEST(test_special_characters_in_name);
  RUN_TEST(test_rapid_create_and_release);
  RUN_TEST(test_release_null_container);
  RUN_TEST(test_release_without_init);
  RUN_TEST(test_system_container_not_released);
  // Integration tests
  RUN_TEST(test_full_lifecycle);
  RUN_TEST(test_init_shutdown_reinit);
  RUN_TEST(test_full_lifecycle_with_shared_txn);
  return UNITY_END();
}