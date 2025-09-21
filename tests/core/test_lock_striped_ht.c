#include "core/lock_striped_ht.h"
#include "unity.h"
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

// Test globals
static lock_striped_ht_t test_ht;

// Test fixture setup and teardown
void setUp(void) {
  // This is run before each test
}

void tearDown(void) {
  // This is run after each test
  lock_striped_ht_destroy(&test_ht);
}

// Basic initialization tests
void test_lock_striped_ht_init_string_mode(void) {
  bool result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(result);
}

void test_lock_striped_ht_init_int_mode(void) {
  bool result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(result);
}

void test_lock_striped_ht_init_null_ht(void) {
  bool result = lock_striped_ht_init_string(NULL);
  TEST_ASSERT_FALSE(result);
}

// String key tests
void test_lock_striped_ht_put_get_string_key(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  char *key = "test_key";
  char *value = "test_value";

  bool put_result = lock_striped_ht_put_string(&test_ht, key, value);
  TEST_ASSERT_TRUE(put_result);

  void *retrieved_value;
  bool get_result = lock_striped_ht_get_string(&test_ht, key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL_PTR(value, retrieved_value);
}

void test_lock_striped_ht_put_get_multiple_string_keys(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  struct {
    char *key;
    char *value;
  } test_data[] = {{"key1", "value1"},
                   {"key2", "value2"},
                   {"key3", "value3"},
                   {"long_key_name_12345", "long_value_67890"},
                   {"", "empty_key_value"}};

  size_t num_entries = sizeof(test_data) / sizeof(test_data[0]);

  // Put all entries
  for (size_t i = 0; i < num_entries; i++) {
    bool put_result = lock_striped_ht_put_string(&test_ht, test_data[i].key,
                                                 test_data[i].value);
    TEST_ASSERT_TRUE(put_result);
  }

  // Get and verify all entries
  for (size_t i = 0; i < num_entries; i++) {
    void *retrieved_value;
    bool get_result = lock_striped_ht_get_string(&test_ht, test_data[i].key,
                                                 &retrieved_value);
    TEST_ASSERT_TRUE(get_result);
    TEST_ASSERT_EQUAL_PTR(test_data[i].value, retrieved_value);
  }
}

void test_lock_striped_ht_get_nonexistent_string_key(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  void *retrieved_value;
  bool get_result =
      lock_striped_ht_get_string(&test_ht, "nonexistent", &retrieved_value);
  TEST_ASSERT_FALSE(get_result);
}

// Integer key tests
void test_lock_striped_ht_put_get_int_key(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  uint32_t key = 42;
  char *value = "forty_two";

  bool put_result = lock_striped_ht_put_int32(&test_ht, key, value);
  TEST_ASSERT_TRUE(put_result);

  void *retrieved_value;
  bool get_result = lock_striped_ht_get_int32(&test_ht, key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL_PTR(value, retrieved_value);
}

void test_lock_striped_ht_put_get_multiple_int_keys(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  struct {
    uint32_t key;
    char *value;
  } test_data[] = {{1, "zero"},
                   {2, "one"},
                   {100, "hundred"},
                   {999999, "big_number"},
                   {UINT32_MAX, "max_value"}};

  size_t num_entries = sizeof(test_data) / sizeof(test_data[0]);

  // Put all entries
  for (size_t i = 0; i < num_entries; i++) {
    bool put_result = lock_striped_ht_put_int32(&test_ht, test_data[i].key,
                                                test_data[i].value);
    TEST_ASSERT_TRUE(put_result);
  }

  // Get and verify all entries
  for (size_t i = 0; i < num_entries; i++) {
    void *retrieved_value;
    bool get_result =
        lock_striped_ht_get_int32(&test_ht, test_data[i].key, &retrieved_value);
    TEST_ASSERT_TRUE(get_result);
    TEST_ASSERT_EQUAL_PTR(test_data[i].value, retrieved_value);
  }
}

void test_lock_striped_ht_get_nonexistent_int_key(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  uint32_t key = 12345;
  void *retrieved_value;
  bool get_result = lock_striped_ht_get_int32(&test_ht, key, &retrieved_value);
  TEST_ASSERT_FALSE(get_result);
}

// Error condition tests
void test_lock_striped_ht_put_null_params(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  char *key = "test_key";
  char *value = "test_value";

  // Null hash table
  bool result1 = lock_striped_ht_put_string(NULL, key, value);
  TEST_ASSERT_FALSE(result1);

  // Null key
  bool result2 = lock_striped_ht_put_string(&test_ht, NULL, value);
  TEST_ASSERT_FALSE(result2);

  // Null value
  bool result3 = lock_striped_ht_put_string(&test_ht, key, NULL);
  TEST_ASSERT_FALSE(result3);
}

void test_lock_striped_ht_get_null_params(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  char *key = "test_key";
  void *retrieved_value;

  // Null hash table
  bool result1 = lock_striped_ht_get_string(NULL, key, &retrieved_value);
  TEST_ASSERT_FALSE(result1);

  // Null key
  bool result2 = lock_striped_ht_get_string(&test_ht, NULL, &retrieved_value);
  TEST_ASSERT_FALSE(result2);

  // Null output pointer
  bool result3 = lock_striped_ht_get_string(&test_ht, key, NULL);
  TEST_ASSERT_FALSE(result3);
}

void test_lock_striped_ht_destroy_null(void) {
  // Should not crash
  lock_striped_ht_destroy(NULL);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

// Value update tests
void test_lock_striped_ht_update_existing_key(void) {
  bool init_result = lock_striped_ht_init_string(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  char *key = "update_key";
  char *value1 = "original_value";
  char *value2 = "updated_value";

  // Insert original value
  bool put_result1 = lock_striped_ht_put_string(&test_ht, key, value1);
  TEST_ASSERT_TRUE(put_result1);

  // Verify original value
  void *retrieved_value;
  bool get_result1 =
      lock_striped_ht_get_string(&test_ht, key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result1);
  TEST_ASSERT_EQUAL_PTR(value1, retrieved_value);

  // try Update with new value - should fail
  bool put_result2 = lock_striped_ht_put_string(&test_ht, key, value2);
  TEST_ASSERT_FALSE(put_result2);

  // Verify original value
  get_result1 = lock_striped_ht_get_string(&test_ht, key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result1);
  TEST_ASSERT_EQUAL_PTR(value1, retrieved_value);
}

// Load tests with multiple threads
#define NUM_THREADS 8
#define OPS_PER_THREAD 1000
#define TOTAL_KEYS (NUM_THREADS * OPS_PER_THREAD)

typedef struct {
  lock_striped_ht_t *ht;
  int thread_id;
  int start_key;
  int num_ops;
  volatile int *success_count;
  volatile int *error_count;
} thread_test_data_t;

// Multi-threaded write test
void *writer_thread_func(void *arg) {
  thread_test_data_t *data = (thread_test_data_t *)arg;
  int local_success = 0;
  int local_error = 0;

  for (int i = 0; i < data->num_ops; i++) {
    uint32_t key = data->start_key + i;
    char *value = malloc(32);
    snprintf(value, 32, "value_%d_%d", data->thread_id, i);

    if (lock_striped_ht_put_int32(data->ht, key, value)) {
      local_success++;
    } else {
      local_error++;
      free(value);
    }

    // Small delay to increase contention
    if (i % 100 == 0) {
      usleep(1);
    }
  }

  __sync_fetch_and_add(data->success_count, local_success);
  __sync_fetch_and_add(data->error_count, local_error);

  return NULL;
}

void test_lock_striped_ht_concurrent_writes(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  pthread_t threads[NUM_THREADS];
  thread_test_data_t thread_data[NUM_THREADS];
  volatile int success_count = 0;
  volatile int error_count = 0;

  // Create threads
  for (int i = 0; i < NUM_THREADS; i++) {
    thread_data[i].ht = &test_ht;
    thread_data[i].thread_id = i;
    thread_data[i].start_key = (i * OPS_PER_THREAD) + 1;
    thread_data[i].num_ops = OPS_PER_THREAD;
    thread_data[i].success_count = &success_count;
    thread_data[i].error_count = &error_count;

    int result =
        pthread_create(&threads[i], NULL, writer_thread_func, &thread_data[i]);
    TEST_ASSERT_EQUAL(0, result);
  }

  // Wait for all threads
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  // Verify results
  TEST_ASSERT_EQUAL(TOTAL_KEYS, success_count);
  TEST_ASSERT_EQUAL(0, error_count);

  // Verify all keys can be retrieved
  int retrieved_count = 0;
  for (int i = 1; i <= TOTAL_KEYS; i++) {
    uint32_t key = i;
    void *value;
    if (lock_striped_ht_get_int32(&test_ht, key, &value)) {
      retrieved_count++;
    }
  }
  TEST_ASSERT_EQUAL(TOTAL_KEYS, retrieved_count);
}

// Mixed read/write test
void *mixed_rw_thread_func(void *arg) {
  thread_test_data_t *data = (thread_test_data_t *)arg;
  int local_success = 0;
  int local_duplicate_rejections = 0;

  srand(time(NULL) + data->thread_id);

  for (int i = 0; i < data->num_ops; i++) {
    uint32_t key = rand() % 1000; // Limited key space for contention

    if (rand() % 3 == 0) { // 33% writes, 67% reads
      char *value = malloc(32);
      snprintf(value, 32, "val_%d_%d", data->thread_id, i);

      if (lock_striped_ht_put_int32(data->ht, key, value)) {
        local_success++;
      } else {
        local_duplicate_rejections++; // Expected behavior - key already exists
        free(value);
      }
    } else {
      void *value;
      if (lock_striped_ht_get_int32(data->ht, key, &value)) {
        local_success++;
      }
      // Get can fail if key doesn't exist - not an error
    }

    if (i % 50 == 0) {
      usleep(1);
    }
  }

  __sync_fetch_and_add(data->success_count, local_success);
  __sync_fetch_and_add(data->error_count, local_duplicate_rejections);

  return NULL;
}

void test_lock_striped_ht_concurrent_mixed_operations(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  // Pre-populate with some data
  for (int i = 0; i < 100; i++) {
    uint32_t key = i;
    char *value = malloc(32);
    snprintf(value, 32, "initial_value_%d", i);
    lock_striped_ht_put_int32(&test_ht, key, value);
  }

  pthread_t threads[NUM_THREADS];
  thread_test_data_t thread_data[NUM_THREADS];
  volatile int success_count = 0;
  volatile int error_count = 0;

  // Create threads for mixed read/write operations
  for (int i = 0; i < NUM_THREADS; i++) {
    thread_data[i].ht = &test_ht;
    thread_data[i].thread_id = i;
    thread_data[i].num_ops = OPS_PER_THREAD / 2; // Fewer ops for mixed test
    thread_data[i].success_count = &success_count;
    thread_data[i].error_count = &error_count;

    int result = pthread_create(&threads[i], NULL, mixed_rw_thread_func,
                                &thread_data[i]);
    TEST_ASSERT_EQUAL(0, result);
  }

  // Wait for all threads
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  // Mixed operations will have duplicate key rejections (expected behavior)
  TEST_ASSERT_GREATER_THAN(0, success_count);
  printf("\nMixed operations: %d successes, %d duplicate key rejections "
         "(expected)\n",
         (int)success_count, (int)error_count);

  // Verify hash table is still functional
  uint32_t test_key = 9999;
  char *test_value = "post_test_value";
  bool put_result = lock_striped_ht_put_int32(&test_ht, test_key, test_value);
  TEST_ASSERT_TRUE(put_result);

  void *retrieved_value;
  bool get_result =
      lock_striped_ht_get_int32(&test_ht, test_key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL_PTR(test_value, retrieved_value);
}

// Stress test with high contention
void *stress_test_thread_func(void *arg) {
  thread_test_data_t *data = (thread_test_data_t *)arg;

  // All threads fight over same small set of keys
  for (int i = 0; i < data->num_ops; i++) {
    uint32_t key = i % 10; // Only 10 keys - high contention
    char *value = malloc(32);
    snprintf(value, 32, "stress_%d_%d", data->thread_id, i);

    // Put and immediately try to get
    lock_striped_ht_put_int32(data->ht, key, value);

    void *retrieved_value;
    lock_striped_ht_get_int32(data->ht, key, &retrieved_value);

    // No delay - maximum contention
  }

  return NULL;
}

void test_lock_striped_ht_high_contention_stress(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  pthread_t threads[NUM_THREADS];
  thread_test_data_t thread_data[NUM_THREADS];

  // Create threads for stress test
  for (int i = 0; i < NUM_THREADS; i++) {
    thread_data[i].ht = &test_ht;
    thread_data[i].thread_id = i;
    thread_data[i].num_ops = 200; // Fewer ops but maximum contention

    int result = pthread_create(&threads[i], NULL, stress_test_thread_func,
                                &thread_data[i]);
    TEST_ASSERT_EQUAL(0, result);
  }

  // Wait for all threads
  for (int i = 0; i < NUM_THREADS; i++) {
    pthread_join(threads[i], NULL);
  }

  // If we reach here without hanging/crashing, the stress test passed
  TEST_ASSERT_TRUE(true);

  // Verify hash table is still functional
  uint32_t test_key = 999;
  char *test_value = "post_stress_test";
  bool put_result = lock_striped_ht_put_int32(&test_ht, test_key, test_value);
  TEST_ASSERT_TRUE(put_result);

  void *retrieved_value;
  bool get_result =
      lock_striped_ht_get_int32(&test_ht, test_key, &retrieved_value);
  TEST_ASSERT_TRUE(get_result);
  TEST_ASSERT_EQUAL_PTR(test_value, retrieved_value);
}

// Performance benchmark test
void test_lock_striped_ht_performance_benchmark(void) {
  bool init_result = lock_striped_ht_init_int32(&test_ht);
  TEST_ASSERT_TRUE(init_result);

  const int benchmark_ops = 10000;
  struct timespec start, end;

  // Benchmark sequential writes
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int i = 0; i < benchmark_ops; i++) {
    uint32_t key = i;
    char *value = malloc(32);
    snprintf(value, 32, "benchmark_value_%d", i);
    lock_striped_ht_put_int32(&test_ht, key, value);
  }

  clock_gettime(CLOCK_MONOTONIC, &end);

  double write_time =
      (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
  double write_ops_per_sec = benchmark_ops / write_time;

  // Benchmark sequential reads
  clock_gettime(CLOCK_MONOTONIC, &start);

  for (int i = 0; i < benchmark_ops; i++) {
    uint32_t key = i;
    void *value;
    lock_striped_ht_get_int32(&test_ht, key, &value);
  }

  clock_gettime(CLOCK_MONOTONIC, &end);

  double read_time =
      (end.tv_sec - start.tv_sec) + (end.tv_nsec - start.tv_nsec) / 1e9;
  double read_ops_per_sec = benchmark_ops / read_time;

  // Print results (will be visible in test output)
  printf("\nPerformance Benchmark Results:\n");
  printf("Write ops/sec: %.0f\n", write_ops_per_sec);
  printf("Read ops/sec: %.0f\n", read_ops_per_sec);

  // Sanity check - should be reasonably fast
  TEST_ASSERT_GREATER_THAN(10000, write_ops_per_sec);
  TEST_ASSERT_GREATER_THAN(50000, read_ops_per_sec);
}

// Main test runner
int main(void) {
  UNITY_BEGIN();

  // Basic functionality tests
  RUN_TEST(test_lock_striped_ht_init_string_mode);
  RUN_TEST(test_lock_striped_ht_init_int_mode);
  RUN_TEST(test_lock_striped_ht_init_null_ht);

  // String key tests
  RUN_TEST(test_lock_striped_ht_put_get_string_key);
  RUN_TEST(test_lock_striped_ht_put_get_multiple_string_keys);
  RUN_TEST(test_lock_striped_ht_get_nonexistent_string_key);

  // Integer key tests
  RUN_TEST(test_lock_striped_ht_put_get_int_key);
  RUN_TEST(test_lock_striped_ht_put_get_multiple_int_keys);
  RUN_TEST(test_lock_striped_ht_get_nonexistent_int_key);

  // Error condition tests
  RUN_TEST(test_lock_striped_ht_put_null_params);
  RUN_TEST(test_lock_striped_ht_get_null_params);
  RUN_TEST(test_lock_striped_ht_destroy_null);

  // Update tests
  RUN_TEST(test_lock_striped_ht_update_existing_key);

  // Multi-threaded load tests
  RUN_TEST(test_lock_striped_ht_concurrent_writes);
  RUN_TEST(test_lock_striped_ht_concurrent_mixed_operations);
  RUN_TEST(test_lock_striped_ht_high_contention_stress);

  // Performance test
  RUN_TEST(test_lock_striped_ht_performance_benchmark);

  return UNITY_END();
}