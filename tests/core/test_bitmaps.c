#include "core/bitmaps.h"
#include "unity.h"
#include <stdint.h>
#include <stdlib.h>

// Test fixture setup and teardown
void setUp(void) {
  // This is run before each test
}

void tearDown(void) {
  // This is run after each test
}

// Test bitmap_create
void test_bitmap_create_returns_valid_pointer(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);
  bitmap_free(bm);
}

void test_bitmap_create_initializes_bitmap(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  // New bitmap should not contain any values
  TEST_ASSERT_FALSE(bitmap_contains(bm, 0));
  TEST_ASSERT_FALSE(bitmap_contains(bm, 1));
  TEST_ASSERT_FALSE(bitmap_contains(bm, UINT32_MAX));

  bitmap_free(bm);
}

// Test bitmap_add and bitmap_contains
void test_bitmap_add_single_value(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 42);
  TEST_ASSERT_TRUE(bitmap_contains(bm, 42));

  bitmap_free(bm);
}

void test_bitmap_add_multiple_values(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  uint32_t values[] = {1, 100, 1000, 10000, UINT32_MAX};
  size_t num_values = sizeof(values) / sizeof(values[0]);

  for (size_t i = 0; i < num_values; i++) {
    bitmap_add(bm, values[i]);
  }

  for (size_t i = 0; i < num_values; i++) {
    TEST_ASSERT_TRUE(bitmap_contains(bm, values[i]));
  }

  // Test values not added
  TEST_ASSERT_FALSE(bitmap_contains(bm, 2));
  TEST_ASSERT_FALSE(bitmap_contains(bm, 999));

  bitmap_free(bm);
}

void test_bitmap_add_duplicate_values(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 42);
  bitmap_add(bm, 42); // Add same value again

  TEST_ASSERT_TRUE(bitmap_contains(bm, 42));

  bitmap_free(bm);
}

void test_bitmap_add_edge_values(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 0);
  bitmap_add(bm, UINT32_MAX);

  TEST_ASSERT_TRUE(bitmap_contains(bm, 0));
  TEST_ASSERT_TRUE(bitmap_contains(bm, UINT32_MAX));

  bitmap_free(bm);
}

// Test bitmap_add with NULL pointer
void test_bitmap_add_null_bitmap(void) {
  // Should not crash when called with NULL
  bitmap_add(NULL, 42);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

// Test bitmap_remove
void test_bitmap_remove_existing_value(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 42);
  TEST_ASSERT_TRUE(bitmap_contains(bm, 42));

  bitmap_remove(bm, 42);
  TEST_ASSERT_FALSE(bitmap_contains(bm, 42));

  bitmap_free(bm);
}

void test_bitmap_remove_non_existing_value(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  // Remove value that was never added
  bitmap_remove(bm, 42);
  TEST_ASSERT_FALSE(bitmap_contains(bm, 42));

  bitmap_free(bm);
}

void test_bitmap_remove_multiple_values(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  uint32_t values[] = {1, 100, 1000};
  size_t num_values = sizeof(values) / sizeof(values[0]);

  // Add all values
  for (size_t i = 0; i < num_values; i++) {
    bitmap_add(bm, values[i]);
  }

  // Remove some values
  bitmap_remove(bm, 1);
  bitmap_remove(bm, 1000);

  TEST_ASSERT_FALSE(bitmap_contains(bm, 1));
  TEST_ASSERT_TRUE(bitmap_contains(bm, 100));
  TEST_ASSERT_FALSE(bitmap_contains(bm, 1000));

  bitmap_free(bm);
}

void test_bitmap_remove_null_bitmap(void) {
  // Should not crash when called with NULL
  bitmap_remove(NULL, 42);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

// Test bitmap_contains
void test_bitmap_contains_empty_bitmap(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  TEST_ASSERT_FALSE(bitmap_contains(bm, 0));
  TEST_ASSERT_FALSE(bitmap_contains(bm, 42));
  TEST_ASSERT_FALSE(bitmap_contains(bm, UINT32_MAX));

  bitmap_free(bm);
}

void test_bitmap_contains_null_bitmap(void) {
  // Should return false for NULL bitmap
  TEST_ASSERT_FALSE(bitmap_contains(NULL, 42));
}

// Test bitmap_copy
void test_bitmap_copy_empty_bitmap(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  bitmap_t *copy = bitmap_copy(original);
  TEST_ASSERT_NOT_NULL(copy);
  TEST_ASSERT_NOT_EQUAL(original, copy); // Different pointers

  // Both should be empty
  TEST_ASSERT_FALSE(bitmap_contains(original, 42));
  TEST_ASSERT_FALSE(bitmap_contains(copy, 42));

  bitmap_free(original);
  bitmap_free(copy);
}

void test_bitmap_copy_populated_bitmap(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  uint32_t values[] = {1, 100, 1000, UINT32_MAX};
  size_t num_values = sizeof(values) / sizeof(values[0]);

  // Populate original
  for (size_t i = 0; i < num_values; i++) {
    bitmap_add(original, values[i]);
  }

  bitmap_t *copy = bitmap_copy(original);
  TEST_ASSERT_NOT_NULL(copy);
  TEST_ASSERT_NOT_EQUAL(original, copy);

  // Both should contain the same values
  for (size_t i = 0; i < num_values; i++) {
    TEST_ASSERT_TRUE(bitmap_contains(original, values[i]));
    TEST_ASSERT_TRUE(bitmap_contains(copy, values[i]));
  }

  bitmap_free(original);
  bitmap_free(copy);
}

void test_bitmap_copy_independence(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  bitmap_add(original, 42);

  bitmap_t *copy = bitmap_copy(original);
  TEST_ASSERT_NOT_NULL(copy);

  // Modify original, copy should be unaffected
  bitmap_add(original, 100);
  bitmap_remove(original, 42);

  TEST_ASSERT_FALSE(bitmap_contains(original, 42));
  TEST_ASSERT_TRUE(bitmap_contains(original, 100));

  TEST_ASSERT_TRUE(bitmap_contains(copy, 42));
  TEST_ASSERT_FALSE(bitmap_contains(copy, 100));

  bitmap_free(original);
  bitmap_free(copy);
}

void test_bitmap_copy_null_bitmap(void) {
  bitmap_t *copy = bitmap_copy(NULL);
  TEST_ASSERT_NULL(copy);
}

// Test bitmap_serialize and bitmap_deserialize
void test_bitmap_serialize_empty_bitmap(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  size_t size;
  void *buffer = bitmap_serialize(bm, &size);
  TEST_ASSERT_NOT_NULL(buffer);
  TEST_ASSERT_GREATER_THAN(0, size);

  free(buffer);
  bitmap_free(bm);
}

void test_bitmap_serialize_populated_bitmap(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 42);
  bitmap_add(bm, 100);

  size_t size;
  void *buffer = bitmap_serialize(bm, &size);
  TEST_ASSERT_NOT_NULL(buffer);
  TEST_ASSERT_GREATER_THAN(0, size);

  free(buffer);
  bitmap_free(bm);
}

void test_bitmap_serialize_null_inputs(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  size_t size;
  void *buffer1 = bitmap_serialize(NULL, &size);
  TEST_ASSERT_NULL(buffer1);

  void *buffer2 = bitmap_serialize(bm, NULL);
  TEST_ASSERT_NULL(buffer2);

  bitmap_free(bm);
}

void test_bitmap_deserialize_empty_bitmap(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  size_t size;
  void *buffer = bitmap_serialize(original, &size);
  TEST_ASSERT_NOT_NULL(buffer);

  bitmap_t *deserialized = bitmap_deserialize(buffer, size);
  TEST_ASSERT_NOT_NULL(deserialized);

  // Should be empty like original
  TEST_ASSERT_FALSE(bitmap_contains(deserialized, 42));

  free(buffer);
  bitmap_free(original);
  bitmap_free(deserialized);
}

void test_bitmap_deserialize_populated_bitmap(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  uint32_t values[] = {1, 42, 100, 1000};
  size_t num_values = sizeof(values) / sizeof(values[0]);

  for (size_t i = 0; i < num_values; i++) {
    bitmap_add(original, values[i]);
  }

  size_t size;
  void *buffer = bitmap_serialize(original, &size);
  TEST_ASSERT_NOT_NULL(buffer);

  bitmap_t *deserialized = bitmap_deserialize(buffer, size);
  TEST_ASSERT_NOT_NULL(deserialized);

  // Should contain same values as original
  for (size_t i = 0; i < num_values; i++) {
    TEST_ASSERT_TRUE(bitmap_contains(deserialized, values[i]));
  }

  // Should not contain values not in original
  TEST_ASSERT_FALSE(bitmap_contains(deserialized, 2));
  TEST_ASSERT_FALSE(bitmap_contains(deserialized, 999));

  free(buffer);
  bitmap_free(original);
  bitmap_free(deserialized);
}

void test_bitmap_deserialize_null_buffer(void) {
  bitmap_t *deserialized = bitmap_deserialize(NULL, 100);
  TEST_ASSERT_NULL(deserialized);
}

void test_bitmap_deserialize_invalid_size(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  bitmap_add(original, 42);

  size_t size;
  void *buffer = bitmap_serialize(original, &size);
  TEST_ASSERT_NOT_NULL(buffer);

  // Try to deserialize with wrong size
  bitmap_t *deserialized = bitmap_deserialize(buffer, size - 1);
  TEST_ASSERT_NULL(deserialized);

  free(buffer);
  bitmap_free(original);
}

// Test serialization round-trip
void test_bitmap_serialize_deserialize_roundtrip(void) {
  bitmap_t *original = bitmap_create();
  TEST_ASSERT_NOT_NULL(original);

  uint32_t values[] = {0, 1, 42, 100, 1000, 65536, UINT32_MAX};
  size_t num_values = sizeof(values) / sizeof(values[0]);

  for (size_t i = 0; i < num_values; i++) {
    bitmap_add(original, values[i]);
  }

  size_t size;
  void *buffer = bitmap_serialize(original, &size);
  TEST_ASSERT_NOT_NULL(buffer);

  bitmap_t *roundtrip = bitmap_deserialize(buffer, size);
  TEST_ASSERT_NOT_NULL(roundtrip);

  // All values should be preserved
  for (size_t i = 0; i < num_values; i++) {
    TEST_ASSERT_TRUE(bitmap_contains(roundtrip, values[i]));
  }

  // Values not added should still not be present
  TEST_ASSERT_FALSE(bitmap_contains(roundtrip, 2));
  TEST_ASSERT_FALSE(bitmap_contains(roundtrip, 999));

  free(buffer);
  bitmap_free(original);
  bitmap_free(roundtrip);
}

// Test bitmap_free
void test_bitmap_free_null_bitmap(void) {
  // Should not crash when called with NULL
  bitmap_free(NULL);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

void test_bitmap_free_valid_bitmap(void) {
  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NOT_NULL(bm);

  bitmap_add(bm, 42);

  // Should not crash
  bitmap_free(bm);
  TEST_ASSERT_TRUE(true); // If we reach here, it didn't crash
}

// --- Bitmap operation tests ---

void test_bitmap_and_basic(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 3);
  bitmap_t *result = bitmap_and(bm1, bm2);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_FALSE(bitmap_contains(result, 1));
  TEST_ASSERT_TRUE(bitmap_contains(result, 2));
  TEST_ASSERT_FALSE(bitmap_contains(result, 3));
  bitmap_free(bm1);
  bitmap_free(bm2);
  bitmap_free(result);
}

void test_bitmap_or_basic(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm2, 2);
  bitmap_t *result = bitmap_or(bm1, bm2);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_TRUE(bitmap_contains(result, 1));
  TEST_ASSERT_TRUE(bitmap_contains(result, 2));
  bitmap_free(bm1);
  bitmap_free(bm2);
  bitmap_free(result);
}

void test_bitmap_xor_basic(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 3);
  bitmap_t *result = bitmap_xor(bm1, bm2);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_TRUE(bitmap_contains(result, 1));
  TEST_ASSERT_FALSE(bitmap_contains(result, 2));
  TEST_ASSERT_TRUE(bitmap_contains(result, 3));
  bitmap_free(bm1);
  bitmap_free(bm2);
  bitmap_free(result);
}

void test_bitmap_not_basic(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm1, 3);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 4);
  bitmap_t *result = bitmap_not(bm1, bm2);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_TRUE(bitmap_contains(result, 1));
  TEST_ASSERT_FALSE(bitmap_contains(result, 2));
  TEST_ASSERT_TRUE(bitmap_contains(result, 3));
  TEST_ASSERT_FALSE(bitmap_contains(result, 4));
  bitmap_free(bm1);
  bitmap_free(bm2);
  bitmap_free(result);
}

void test_bitmap_and_inplace(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 3);
  bitmap_and_inplace(bm1, bm2);
  TEST_ASSERT_FALSE(bitmap_contains(bm1, 1));
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 2));
  TEST_ASSERT_FALSE(bitmap_contains(bm1, 3));
  bitmap_free(bm1);
  bitmap_free(bm2);
}

void test_bitmap_or_inplace(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm2, 2);
  bitmap_or_inplace(bm1, bm2);
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 1));
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 2));
  bitmap_free(bm1);
  bitmap_free(bm2);
}

void test_bitmap_xor_inplace(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 3);
  bitmap_xor_inplace(bm1, bm2);
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 1));
  TEST_ASSERT_FALSE(bitmap_contains(bm1, 2));
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 3));
  bitmap_free(bm1);
  bitmap_free(bm2);
}

void test_bitmap_not_inplace(void) {
  bitmap_t *bm1 = bitmap_create();
  bitmap_t *bm2 = bitmap_create();
  bitmap_add(bm1, 1);
  bitmap_add(bm1, 2);
  bitmap_add(bm1, 3);
  bitmap_add(bm2, 2);
  bitmap_add(bm2, 4);
  bitmap_not_inplace(bm1, bm2);
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 1));
  TEST_ASSERT_FALSE(bitmap_contains(bm1, 2));
  TEST_ASSERT_TRUE(bitmap_contains(bm1, 3));
  TEST_ASSERT_FALSE(bitmap_contains(bm1, 4));
  bitmap_free(bm1);
  bitmap_free(bm2);
}

void test_bitmap_op_null_inputs(void) {
  // All ops should return NULL or do nothing if any input is NULL
  TEST_ASSERT_NULL(bitmap_and(NULL, NULL));
  TEST_ASSERT_NULL(bitmap_or(NULL, NULL));
  TEST_ASSERT_NULL(bitmap_xor(NULL, NULL));
  TEST_ASSERT_NULL(bitmap_not(NULL, NULL));

  bitmap_t *bm = bitmap_create();
  TEST_ASSERT_NULL(bitmap_and(bm, NULL));
  TEST_ASSERT_NULL(bitmap_or(bm, NULL));
  TEST_ASSERT_NULL(bitmap_xor(bm, NULL));
  TEST_ASSERT_NULL(bitmap_not(bm, NULL));
  TEST_ASSERT_NULL(bitmap_and(NULL, bm));
  TEST_ASSERT_NULL(bitmap_or(NULL, bm));
  TEST_ASSERT_NULL(bitmap_xor(NULL, bm));
  TEST_ASSERT_NULL(bitmap_not(NULL, bm));

  // Inplace ops should not crash
  bitmap_and_inplace(NULL, bm);
  bitmap_or_inplace(NULL, bm);
  bitmap_xor_inplace(NULL, bm);
  bitmap_not_inplace(NULL, bm);
  bitmap_and_inplace(bm, NULL);
  bitmap_or_inplace(bm, NULL);
  bitmap_xor_inplace(bm, NULL);
  bitmap_not_inplace(bm, NULL);
  bitmap_free(bm);
}

// Main test runner
int main(void) {
  UNITY_BEGIN();

  // bitmap_create tests
  RUN_TEST(test_bitmap_create_returns_valid_pointer);
  RUN_TEST(test_bitmap_create_initializes_bitmap);

  // bitmap_add tests
  RUN_TEST(test_bitmap_add_single_value);
  RUN_TEST(test_bitmap_add_multiple_values);
  RUN_TEST(test_bitmap_add_duplicate_values);
  RUN_TEST(test_bitmap_add_edge_values);
  RUN_TEST(test_bitmap_add_null_bitmap);

  // bitmap_remove tests
  RUN_TEST(test_bitmap_remove_existing_value);
  RUN_TEST(test_bitmap_remove_non_existing_value);
  RUN_TEST(test_bitmap_remove_multiple_values);
  RUN_TEST(test_bitmap_remove_null_bitmap);

  // bitmap_contains tests
  RUN_TEST(test_bitmap_contains_empty_bitmap);
  RUN_TEST(test_bitmap_contains_null_bitmap);

  // bitmap_copy tests
  RUN_TEST(test_bitmap_copy_empty_bitmap);
  RUN_TEST(test_bitmap_copy_populated_bitmap);
  RUN_TEST(test_bitmap_copy_independence);
  RUN_TEST(test_bitmap_copy_null_bitmap);

  // bitmap_serialize tests
  RUN_TEST(test_bitmap_serialize_empty_bitmap);
  RUN_TEST(test_bitmap_serialize_populated_bitmap);
  RUN_TEST(test_bitmap_serialize_null_inputs);

  // bitmap_deserialize tests
  RUN_TEST(test_bitmap_deserialize_empty_bitmap);
  RUN_TEST(test_bitmap_deserialize_populated_bitmap);
  RUN_TEST(test_bitmap_deserialize_null_buffer);
  RUN_TEST(test_bitmap_deserialize_invalid_size);

  // Round-trip tests
  RUN_TEST(test_bitmap_serialize_deserialize_roundtrip);

  // bitmap_free tests
  RUN_TEST(test_bitmap_free_null_bitmap);
  RUN_TEST(test_bitmap_free_valid_bitmap);

  // bitmap operation tests
  RUN_TEST(test_bitmap_and_basic);
  RUN_TEST(test_bitmap_or_basic);
  RUN_TEST(test_bitmap_xor_basic);
  RUN_TEST(test_bitmap_not_basic);
  RUN_TEST(test_bitmap_and_inplace);
  RUN_TEST(test_bitmap_or_inplace);
  RUN_TEST(test_bitmap_xor_inplace);
  RUN_TEST(test_bitmap_not_inplace);
  RUN_TEST(test_bitmap_op_null_inputs);

  return UNITY_END();
}