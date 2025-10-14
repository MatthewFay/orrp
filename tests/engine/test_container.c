#include "core/db.h"
#include "engine/container/container.h"
#include "unity.h"
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

// Test configuration
#define TEST_DATA_DIR "test_data"
#define TEST_CACHE_CAPACITY 4
#define TEST_CONTAINER_SIZE (1024 * 1024) // 1MB

// Helper function to clean up test directory
static void cleanup_test_dir(void) {
  // char cmd[256];
  // snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DATA_DIR);
  // system(cmd);
}

// Unity setup - runs before each test
void setUp(void) { cleanup_test_dir(); }

// Unity teardown - runs after each test
void tearDown(void) {
  container_shutdown();
  cleanup_test_dir();
}

// ============================================================================
// Initialization Tests
// ============================================================================

void test_container_init_success(void) {
  bool result =
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  TEST_ASSERT_TRUE(result);

  // Verify data directory was created
  struct stat st = {0};
  TEST_ASSERT_EQUAL_INT(0, stat(TEST_DATA_DIR, &st));
  TEST_ASSERT_TRUE(S_ISDIR(st.st_mode));
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
  TEST_PASS();
}

void test_container_shutdown_idempotent(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_shutdown();
  container_shutdown(); // Second call should be safe
  TEST_PASS();
}

// ============================================================================
// System Container Tests
// ============================================================================

void test_get_system_container_success(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_system();
  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_SYSTEM, result.container->type);
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

  container_result_t r1 = container_get_system();
  container_result_t r2 = container_get_system();

  TEST_ASSERT_TRUE(r1.success);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_EQUAL_PTR(r1.container, r2.container); // Same instance
}

void test_system_container_has_all_databases(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_system();
  TEST_ASSERT_TRUE(result.success);

  eng_container_t *sys = result.container;
  TEST_ASSERT_NOT_NULL(sys->env);
  TEST_ASSERT_NOT_NULL(sys->data.sys);

  // Verify we can get all system DB handles
  MDB_dbi db;
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(sys, SYS_DB_ENT_ID_TO_INT, &db));
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(sys, SYS_DB_INT_TO_ENT_ID, &db));
  TEST_ASSERT_TRUE(container_get_system_db_handle(sys, SYS_DB_METADATA, &db));
}

// ============================================================================
// User Container Tests
// ============================================================================

void test_get_user_container_success(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test_container");
  TEST_ASSERT_TRUE(result.success);
  TEST_ASSERT_NOT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_TYPE_USER, result.container->type);
  TEST_ASSERT_EQUAL_STRING("test_container", result.container->name);
  TEST_ASSERT_NULL(result.error_msg);

  container_release(result.container);
}

void test_get_user_container_without_init(void) {
  container_result_t result = container_get_or_create_user("test");
  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_NULL(result.container);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_NOT_INITIALIZED, result.error_code);
}

void test_get_user_container_null_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user(NULL);
  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_INVALID_NAME, result.error_code);
}

void test_get_user_container_empty_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("");
  TEST_ASSERT_FALSE(result.success);
  TEST_ASSERT_EQUAL(CONTAINER_ERR_INVALID_NAME, result.error_code);
}

void test_user_container_has_all_databases(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t result = container_get_or_create_user("test");
  TEST_ASSERT_TRUE(result.success);

  eng_container_t *usr = result.container;
  TEST_ASSERT_NOT_NULL(usr->env);
  TEST_ASSERT_NOT_NULL(usr->data.usr);

  // Verify we can get all user DB handles
  MDB_dbi db;
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(usr, USER_DB_INVERTED_EVENT_INDEX, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(usr, USER_DB_EVENT_TO_ENTITY, &db));
  TEST_ASSERT_TRUE(container_get_user_db_handle(usr, USER_DB_METADATA, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(usr, USER_DB_COUNTER_STORE, &db));
  TEST_ASSERT_TRUE(container_get_user_db_handle(usr, USER_DB_COUNT_INDEX, &db));

  container_release(usr);
}

void test_user_container_persists_across_restarts(void) {
  // First session: create container
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t r1 = container_get_or_create_user("persistent");
  TEST_ASSERT_TRUE(r1.success);
  container_release(r1.container);
  container_shutdown();

  // Second session: should load existing container
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t r2 = container_get_or_create_user("persistent");
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_EQUAL_STRING("persistent", r2.container->name);
  container_release(r2.container);
}

// ============================================================================
// Caching Tests
// ============================================================================

void test_container_cached_on_second_access(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t r1 = container_get_or_create_user("cached");
  TEST_ASSERT_TRUE(r1.success);
  eng_container_t *first_ptr = r1.container;
  container_release(r1.container);

  container_result_t r2 = container_get_or_create_user("cached");
  TEST_ASSERT_TRUE(r2.success);
  eng_container_t *second_ptr = r2.container;

  // Should be the same instance (cached)
  TEST_ASSERT_EQUAL_PTR(first_ptr, second_ptr);
  container_release(r2.container);
}

void test_cache_capacity_respected(void) {
  container_init(2, TEST_DATA_DIR, TEST_CONTAINER_SIZE); // Small cache

  container_result_t r1 = container_get_or_create_user("c1");
  container_result_t r2 = container_get_or_create_user("c2");
  container_release(r1.container);
  container_result_t r3 = container_get_or_create_user("c3");

  TEST_ASSERT_TRUE(r1.success);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_TRUE(r3.success);

  container_release(r2.container);
  container_release(r3.container);

  // Verify cache stats
  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_EQUAL(1, stats.evictions);
}

void test_lru_eviction(void) {
  container_init(2, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // Load c1 and c2 (fills cache)
  container_result_t r1 = container_get_or_create_user("c1");
  container_result_t r2 = container_get_or_create_user("c2");
  container_release(r1.container);
  container_release(r2.container);

  // Access c1 again (makes c1 most recently used, c2 is LRU)
  r1 = container_get_or_create_user("c1");
  container_release(r1.container);

  // Load c3 (should evict c2, the LRU)
  container_result_t r3 = container_get_or_create_user("c3");
  container_release(r3.container);

  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_EQUAL(2, stats.cache_size); // c1 and c3 should be cached
  TEST_ASSERT_EQUAL(1, stats.evictions);
}

void test_container_with_references_not_evicted(void) {
  container_init(2, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t r1 = container_get_or_create_user("c1");
  container_result_t r2 = container_get_or_create_user("c2");
  // Don't release r1 and r2 - they have references

  // Try to load c3 (cache is full but all entries have refs)
  container_result_t r3 = container_get_or_create_user("c3");
  TEST_ASSERT_TRUE(r3.success);

  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_EQUAL(3, stats.cache_size); // Cache grew beyond capacity

  container_release(r1.container);
  container_release(r2.container);
  container_release(r3.container);
}

void test_multiple_references_to_same_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_result_t r1 = container_get_or_create_user("shared");
  container_result_t r2 = container_get_or_create_user("shared");
  container_result_t r3 = container_get_or_create_user("shared");

  TEST_ASSERT_EQUAL_PTR(r1.container, r2.container);
  TEST_ASSERT_EQUAL_PTR(r1.container, r3.container);

  // Release all references
  container_release(r1.container);
  container_release(r2.container);
  container_release(r3.container);

  // Should still be cached (ref count should be 0 now)
  container_result_t r4 = container_get_or_create_user("shared");
  TEST_ASSERT_EQUAL_PTR(r1.container, r4.container);
  container_release(r4.container);
}

// ============================================================================
// DB Handle Access Tests
// ============================================================================

void test_get_user_db_handle_null_container(void) {
  MDB_dbi db;
  bool result = container_get_user_db_handle(NULL, USER_DB_METADATA, &db);
  TEST_ASSERT_FALSE(result);
}

void test_get_user_db_handle_null_output(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t r = container_get_or_create_user("test");

  bool result =
      container_get_user_db_handle(r.container, USER_DB_METADATA, NULL);
  TEST_ASSERT_FALSE(result);

  container_release(r.container);
}

void test_get_user_db_handle_wrong_container_type(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t sys = container_get_system();

  MDB_dbi db;
  bool result =
      container_get_user_db_handle(sys.container, USER_DB_METADATA, &db);
  TEST_ASSERT_FALSE(result);
}

void test_get_system_db_handle_null_container(void) {
  MDB_dbi db;
  bool result = container_get_system_db_handle(NULL, SYS_DB_METADATA, &db);
  TEST_ASSERT_FALSE(result);
}

void test_get_system_db_handle_wrong_container_type(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t usr = container_get_or_create_user("test");

  MDB_dbi db;
  bool result =
      container_get_system_db_handle(usr.container, SYS_DB_METADATA, &db);
  TEST_ASSERT_FALSE(result);

  container_release(usr.container);
}

void test_all_user_db_types_accessible(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t r = container_get_or_create_user("test");

  MDB_dbi db;
  TEST_ASSERT_TRUE(container_get_user_db_handle(
      r.container, USER_DB_INVERTED_EVENT_INDEX, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(r.container, USER_DB_EVENT_TO_ENTITY, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(r.container, USER_DB_METADATA, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(r.container, USER_DB_COUNTER_STORE, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(r.container, USER_DB_COUNT_INDEX, &db));

  container_release(r.container);
}

void test_all_system_db_types_accessible(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_result_t r = container_get_system();

  MDB_dbi db;
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(r.container, SYS_DB_ENT_ID_TO_INT, &db));
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(r.container, SYS_DB_INT_TO_ENT_ID, &db));
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(r.container, SYS_DB_METADATA, &db));
}

// ============================================================================
// Statistics Tests
// ============================================================================

void test_stats_initial_state(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  container_stats_t stats;
  container_get_stats(&stats);

  TEST_ASSERT_EQUAL(0, stats.cache_size);
  TEST_ASSERT_EQUAL(TEST_CACHE_CAPACITY, stats.cache_capacity);
  TEST_ASSERT_EQUAL(0, stats.cache_hits);
  TEST_ASSERT_EQUAL(0, stats.cache_misses);
  TEST_ASSERT_EQUAL(0, stats.evictions);
}

void test_stats_track_hits_and_misses(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // First access = miss
  container_result_t r1 = container_get_or_create_user("test");
  container_release(r1.container);

  // Second access = hit
  container_result_t r2 = container_get_or_create_user("test");
  container_release(r2.container);

  container_stats_t stats;
  container_get_stats(&stats);

  TEST_ASSERT_EQUAL(1, stats.cache_size);
  TEST_ASSERT_EQUAL(1, stats.cache_hits);
  TEST_ASSERT_EQUAL(1, stats.cache_misses);
}

void test_stats_null_output(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_get_stats(NULL); // Should not crash
  TEST_PASS();
}

// ============================================================================
// Error String Tests
// ============================================================================

void test_error_string_all_codes(void) {
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_OK));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_NOT_INITIALIZED));
  TEST_ASSERT_NOT_NULL(
      container_error_string(CONTAINER_ERR_ALREADY_INITIALIZED));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_INVALID_NAME));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_INVALID_TYPE));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_ALLOC));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_PATH_TOO_LONG));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_ENV_CREATE));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_DB_OPEN));
  TEST_ASSERT_NOT_NULL(container_error_string(CONTAINER_ERR_CACHE_FULL));
}

void test_error_string_unknown_code(void) {
  const char *msg = container_error_string(9999);
  TEST_ASSERT_NOT_NULL(msg);
  TEST_ASSERT_EQUAL_STRING("Unknown error", msg);
}

// ============================================================================
// DB Key Cleanup Tests
// ============================================================================

void test_free_db_key_null(void) {
  container_free_db_key_contents(NULL); // Should not crash
  TEST_PASS();
}

void test_free_db_key_with_string_key(void) {
  eng_container_db_key_t key = {0};
  key.container_name = strdup("test");
  key.db_key.type = DB_KEY_STRING;
  key.db_key.key.s = strdup("mykey");

  container_free_db_key_contents(&key);
  // If this doesn't crash, the test passes
  TEST_PASS();
}

void test_free_db_key_with_int_key(void) {
  eng_container_db_key_t key = {0};
  key.container_name = strdup("test");
  key.db_key.type = DB_KEY_INTEGER;
  key.db_key.key.i = 42;

  container_free_db_key_contents(&key);
  TEST_PASS();
}

// ============================================================================
// Thread Safety Tests
// ============================================================================

typedef struct {
  int thread_id;
  int iterations;
  bool success;
} thread_test_data_t;

static void *thread_access_containers(void *arg) {
  thread_test_data_t *data = (thread_test_data_t *)arg;
  data->success = true;

  for (int i = 0; i < data->iterations; i++) {
    char name[32];
    snprintf(name, sizeof(name), "container_%d_%d", data->thread_id, i % 3);

    container_result_t r = container_get_or_create_user(name);
    if (!r.success) {
      data->success = false;
      return NULL;
    }

    // Simulate some work
    usleep(100);

    container_release(r.container);
  }

  return NULL;
}

void test_concurrent_access(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  const int NUM_THREADS = 4;
  const int ITERATIONS = 10;
  pthread_t threads[NUM_THREADS];
  thread_test_data_t thread_data[NUM_THREADS];

  // Start threads
  for (int i = 0; i < NUM_THREADS; i++) {
    thread_data[i].thread_id = i;
    thread_data[i].iterations = ITERATIONS;
    thread_data[i].success = false;
    pthread_create(&threads[i], NULL, thread_access_containers,
                   &thread_data[i]);
  }

  // Wait for threads
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
    TEST_ASSERT_TRUE(thread_data[i].success);
  }
}

static void *thread_access_same_container(void *arg) {
  thread_test_data_t *data = (thread_test_data_t *)arg;
  data->success = true;

  for (int i = 0; i < data->iterations; i++) {
    container_result_t r = container_get_or_create_user("shared_container");
    if (!r.success) {
      data->success = false;
      return NULL;
    }

    usleep(50);
    container_release(r.container);
  }

  return NULL;
}

void test_concurrent_same_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  const int NUM_THREADS = 8;
  const int ITERATIONS = 20;
  pthread_t threads[NUM_THREADS];
  thread_test_data_t thread_data[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    thread_data[i].thread_id = i;
    thread_data[i].iterations = ITERATIONS;
    thread_data[i].success = false;
    pthread_create(&threads[i], NULL, thread_access_same_container,
                   &thread_data[i]);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
    TEST_ASSERT_TRUE(thread_data[i].success);
  }

  // Verify the container was only created once (cached)
  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_EQUAL(1, stats.cache_misses); // Only one miss (first creation)
  TEST_ASSERT_GREATER_THAN(0, stats.cache_hits); // Many hits
}

// ============================================================================
// Edge Case Tests
// ============================================================================

void test_very_long_container_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  char long_name[512];
  memset(long_name, 'a', sizeof(long_name) - 1);
  long_name[sizeof(long_name) - 1] = '\0';

  container_result_t r = container_get_or_create_user(long_name);
  // Should either succeed or fail gracefully with appropriate error
  if (!r.success) {
    TEST_ASSERT_EQUAL(CONTAINER_ERR_PATH_TOO_LONG, r.error_code);
  } else {
    container_release(r.container);
  }
}

void test_special_characters_in_name(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  // Test with various special characters (should work on most filesystems)
  const char *names[] = {"container-with-dash", "container_with_underscore",
                         "container.with.dots", "container123"};

  for (int i = 0; i < 4; i++) {
    container_result_t r = container_get_or_create_user(names[i]);
    if (r.success) {
      container_release(r.container);
    }
    // Some characters might not be valid on all filesystems
    // Just ensure we don't crash
  }

  TEST_PASS();
}

void test_rapid_create_and_release(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);

  for (int i = 0; i < 100; i++) {
    char name[32];
    snprintf(name, sizeof(name), "rapid_%d", i);

    container_result_t r = container_get_or_create_user(name);
    TEST_ASSERT_TRUE(r.success);
    container_release(r.container);
  }

  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_GREATER_THAN(0, stats.evictions); // Cache should have evicted
}

void test_release_null_container(void) {
  container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE);
  container_release(NULL); // Should not crash
  TEST_PASS();
}

void test_release_without_init(void) {
  container_result_t r = container_get_or_create_user("test");
  container_release(r.container); // Should not crash even though init failed
  TEST_PASS();
}

// ============================================================================
// Integration Tests
// ============================================================================

void test_full_lifecycle(void) {
  // Init
  TEST_ASSERT_TRUE(
      container_init(TEST_CACHE_CAPACITY, TEST_DATA_DIR, TEST_CONTAINER_SIZE));

  // Get system container
  container_result_t sys = container_get_system();
  TEST_ASSERT_TRUE(sys.success);

  // Create multiple user containers
  container_result_t c1 = container_get_or_create_user("user1");
  container_result_t c2 = container_get_or_create_user("user2");
  TEST_ASSERT_TRUE(c1.success);
  TEST_ASSERT_TRUE(c2.success);

  // Access databases
  MDB_dbi db;
  TEST_ASSERT_TRUE(
      container_get_system_db_handle(sys.container, SYS_DB_METADATA, &db));
  TEST_ASSERT_TRUE(
      container_get_user_db_handle(c1.container, USER_DB_METADATA, &db));

  // Release containers
  container_release(c1.container);
  container_release(c2.container);

  // Check stats
  container_stats_t stats;
  container_get_stats(&stats);
  TEST_ASSERT_EQUAL(2, stats.cache_size);

  // Shutdown
  container_shutdown();

  TEST_PASS();
}

// ============================================================================
// Main Test Runner
// ============================================================================

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
  RUN_TEST(test_get_user_container_without_init);
  RUN_TEST(test_get_user_container_null_name);
  RUN_TEST(test_get_user_container_empty_name);
  RUN_TEST(test_user_container_has_all_databases);
  RUN_TEST(test_user_container_persists_across_restarts);

  // Caching tests
  RUN_TEST(test_container_cached_on_second_access);
  RUN_TEST(test_cache_capacity_respected);
  RUN_TEST(test_lru_eviction);
  RUN_TEST(test_container_with_references_not_evicted);
  RUN_TEST(test_multiple_references_to_same_container);

  // DB Handle Access tests
  RUN_TEST(test_get_user_db_handle_null_container);
  RUN_TEST(test_get_user_db_handle_null_output);
  RUN_TEST(test_get_user_db_handle_wrong_container_type);
  RUN_TEST(test_get_system_db_handle_null_container);
  RUN_TEST(test_get_system_db_handle_wrong_container_type);
  RUN_TEST(test_all_user_db_types_accessible);
  RUN_TEST(test_all_system_db_types_accessible);

  // Statistics tests
  RUN_TEST(test_stats_initial_state);
  RUN_TEST(test_stats_track_hits_and_misses);
  RUN_TEST(test_stats_null_output);

  // Error string tests
  RUN_TEST(test_error_string_all_codes);
  RUN_TEST(test_error_string_unknown_code);

  // DB Key cleanup tests
  RUN_TEST(test_free_db_key_null);
  RUN_TEST(test_free_db_key_with_string_key);
  RUN_TEST(test_free_db_key_with_int_key);

  // Thread safety tests
  RUN_TEST(test_concurrent_access);
  RUN_TEST(test_concurrent_same_container);

  // Edge case tests
  RUN_TEST(test_very_long_container_name);
  RUN_TEST(test_special_characters_in_name);
  RUN_TEST(test_rapid_create_and_release);
  RUN_TEST(test_release_null_container);
  RUN_TEST(test_release_without_init);

  // Integration tests
  RUN_TEST(test_full_lifecycle);

  return UNITY_END();
}