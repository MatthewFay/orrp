#include "core/mmap_array.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

#define TEST_DB_PATH "test_mmap_array.bin"

mmap_array_t arr;

// Helpers
void clean_test_file() { unlink(TEST_DB_PATH); }

void setUp(void) {
  clean_test_file();
  memset(&arr, 0, sizeof(mmap_array_t));
}

void tearDown(void) {
  mmap_array_close(&arr);
  clean_test_file();
}

// -----------------------------------------------------------------------------
// Test Cases
// -----------------------------------------------------------------------------

void test_mmap_array_open_invalid_config_should_fail(void) {
  mmap_array_config_t config = {.path = NULL, // Invalid
                                .item_size = 4,
                                .initial_cap = 100};

  TEST_ASSERT_EQUAL(-1, mmap_array_open(&arr, &config));

  config.path = TEST_DB_PATH;
  config.item_size = 0; // Invalid
  TEST_ASSERT_EQUAL(-1, mmap_array_open(&arr, &config));
}

void test_mmap_array_basic_int32_storage(void) {
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = sizeof(uint32_t), .initial_cap = 100};

  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr, &config));

  uint32_t val1 = 12345;
  uint32_t val2 = 67890;

  // Write
  TEST_ASSERT_EQUAL(0, mmap_array_set(&arr, 0, &val1));
  TEST_ASSERT_EQUAL(0, mmap_array_set(&arr, 50, &val2));

  // Read back using locking pattern
  mmap_array_read_lock(&arr);

  uint32_t *read1 = (uint32_t *)mmap_array_get(&arr, 0);
  uint32_t *read2 = (uint32_t *)mmap_array_get(&arr, 50);
  uint32_t *read_empty =
      (uint32_t *)mmap_array_get(&arr, 25); // Should be 0 initialized

  TEST_ASSERT_NOT_NULL(read1);
  TEST_ASSERT_NOT_NULL(read2);
  TEST_ASSERT_NOT_NULL(read_empty);

  TEST_ASSERT_EQUAL_UINT32(val1, *read1);
  TEST_ASSERT_EQUAL_UINT32(val2, *read2);
  TEST_ASSERT_EQUAL_UINT32(0, *read_empty); // OS zeros new pages

  mmap_array_unlock(&arr);
}

void test_mmap_array_fixed_string_storage(void) {
  // Simulating Entity ID -> String Map (64 bytes)
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = 64, .initial_cap = 10};

  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr, &config));

  char entity_1[64] = "user_uuid_v4_abc_123";
  char entity_2[64] = "user_uuid_v4_xyz_789";

  TEST_ASSERT_EQUAL(0, mmap_array_set(&arr, 1, entity_1));
  TEST_ASSERT_EQUAL(0, mmap_array_set(&arr, 2, entity_2));

  mmap_array_read_lock(&arr);

  char *read1 = (char *)mmap_array_get(&arr, 1);
  char *read2 = (char *)mmap_array_get(&arr, 2);

  TEST_ASSERT_EQUAL_STRING(entity_1, read1);
  TEST_ASSERT_EQUAL_STRING(entity_2, read2);

  mmap_array_unlock(&arr);
}

void test_mmap_array_automatic_resize(void) {
  // Start small (hold 10 integers)
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = sizeof(uint32_t), .initial_cap = 10};
  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr, &config));

  // Write WAY beyond capacity (index 1000)
  // This triggers the exponential resize logic
  uint32_t far_val = 9999;
  TEST_ASSERT_EQUAL(0, mmap_array_set(&arr, 1000, &far_val));

  // Verify we can read it back
  mmap_array_read_lock(&arr);
  uint32_t *read_val = (uint32_t *)mmap_array_get(&arr, 1000);
  TEST_ASSERT_NOT_NULL(read_val);
  TEST_ASSERT_EQUAL_UINT32(far_val, *read_val);
  mmap_array_unlock(&arr);

  // Verify intermediate values are valid (and zeroed)
  mmap_array_read_lock(&arr);
  uint32_t *mid_val = (uint32_t *)mmap_array_get(&arr, 500);
  TEST_ASSERT_NOT_NULL(mid_val);
  TEST_ASSERT_EQUAL_UINT32(0, *mid_val);
  mmap_array_unlock(&arr);
}

void test_mmap_array_persistence_across_reopen(void) {
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = sizeof(uint32_t), .initial_cap = 100};

  // 1. Open and Write
  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr, &config));
  uint32_t val = 42;
  mmap_array_set(&arr, 10, &val);

  // 2. Explicit Sync (optional, but good for testing) and Close
  TEST_ASSERT_EQUAL(0, mmap_array_sync(&arr));
  mmap_array_close(&arr);
  memset(&arr, 0, sizeof(mmap_array_t)); // Clear struct memory

  // 3. Re-open same file
  mmap_array_t arr2;
  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr2, &config));

  // 4. Verify Data
  mmap_array_read_lock(&arr2);
  uint32_t *read_val = (uint32_t *)mmap_array_get(&arr2, 10);
  TEST_ASSERT_NOT_NULL(read_val);
  TEST_ASSERT_EQUAL_UINT32(42, *read_val);
  mmap_array_unlock(&arr2);

  mmap_array_close(&arr2);
}

void test_mmap_array_get_out_of_bounds_returns_null(void) {
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = sizeof(uint32_t), .initial_cap = 10};
  TEST_ASSERT_EQUAL(0, mmap_array_open(&arr, &config));

  mmap_array_read_lock(&arr);
  // Capacity is 10 (or page aligned equivalent), request 100000
  void *ptr = mmap_array_get(&arr, 100000);
  TEST_ASSERT_NULL(ptr);
  mmap_array_unlock(&arr);
}

void test_mmap_array_locking_api(void) {
  // Just verify the API returns success (0)
  mmap_array_config_t config = {
      .path = TEST_DB_PATH, .item_size = 4, .initial_cap = 10};
  mmap_array_open(&arr, &config);

  TEST_ASSERT_EQUAL(0, mmap_array_read_lock(&arr));
  TEST_ASSERT_EQUAL(0, mmap_array_unlock(&arr));

  TEST_ASSERT_EQUAL(0, mmap_array_write_lock(&arr));
  TEST_ASSERT_EQUAL(0, mmap_array_unlock(&arr));
}

// -----------------------------------------------------------------------------
// Main
// -----------------------------------------------------------------------------

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_mmap_array_open_invalid_config_should_fail);
  RUN_TEST(test_mmap_array_basic_int32_storage);
  RUN_TEST(test_mmap_array_fixed_string_storage);
  RUN_TEST(test_mmap_array_automatic_resize);
  RUN_TEST(test_mmap_array_persistence_across_reopen);
  RUN_TEST(test_mmap_array_get_out_of_bounds_returns_null);
  RUN_TEST(test_mmap_array_locking_api);
  return UNITY_END();
}