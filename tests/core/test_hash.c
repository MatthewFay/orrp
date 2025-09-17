#include "core/hash.h"
#include "unity.h"
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Test fixture setup and teardown
void setUp(void) {
  // This is run before each test
}

void tearDown(void) {
  // This is run after each test
}

// Test basic functionality
void test_xxhash64_empty_input(void) {
  uint64_t result = xxhash64("", 0, 0);
  TEST_ASSERT_NOT_EQUAL(0, result); // Should produce some hash value
}

void test_xxhash64_single_byte(void) {
  const char input[] = "a";
  uint64_t result = xxhash64(input, 1, 0);
  TEST_ASSERT_NOT_EQUAL(0, result);
}

void test_xxhash64_short_string(void) {
  const char input[] = "hello";
  uint64_t result = xxhash64(input, strlen(input), 0);
  TEST_ASSERT_NOT_EQUAL(0, result);
}

void test_xxhash64_medium_string(void) {
  const char input[] = "hello world, this is a test string";
  uint64_t result = xxhash64(input, strlen(input), 0);
  TEST_ASSERT_NOT_EQUAL(0, result);
}

void test_xxhash64_long_string(void) {
  // Test with string longer than 32 bytes (triggers main loop)
  const char input[] = "this is a very long string that should be more than 32 "
                       "bytes to test the main processing loop";
  uint64_t result = xxhash64(input, strlen(input), 0);
  TEST_ASSERT_NOT_EQUAL(0, result);
}

// Test deterministic behavior - same input should produce same output
void test_xxhash64_deterministic(void) {
  const char input[] = "test string for deterministic check";
  uint64_t result1 = xxhash64(input, strlen(input), 0);
  uint64_t result2 = xxhash64(input, strlen(input), 0);

  TEST_ASSERT_EQUAL(result1, result2);
}

void test_xxhash64_deterministic_multiple_calls(void) {
  const char input[] = "another test string";
  uint64_t results[5];

  for (int i = 0; i < 5; i++) {
    results[i] = xxhash64(input, strlen(input), 42);
  }

  // All results should be identical
  for (int i = 1; i < 5; i++) {
    TEST_ASSERT_EQUAL(results[0], results[i]);
  }
}

// Test different seeds produce different results
void test_xxhash64_different_seeds(void) {
  const char input[] = "seed test string";
  uint64_t result1 = xxhash64(input, strlen(input), 0);
  uint64_t result2 = xxhash64(input, strlen(input), 1);
  uint64_t result3 = xxhash64(input, strlen(input), 0xDEADBEEF);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
  TEST_ASSERT_NOT_EQUAL(result1, result3);
  TEST_ASSERT_NOT_EQUAL(result2, result3);
}

void test_xxhash64_extreme_seeds(void) {
  const char input[] = "extreme seed test";
  uint64_t result1 = xxhash64(input, strlen(input), 0);
  uint64_t result2 = xxhash64(input, strlen(input), UINT64_MAX);
  uint64_t result3 = xxhash64(input, strlen(input), 0x8000000000000000ULL);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
  TEST_ASSERT_NOT_EQUAL(result1, result3);
  TEST_ASSERT_NOT_EQUAL(result2, result3);
}

// Test different input lengths produce different results
void test_xxhash64_different_lengths(void) {
  const char base[] = "this is a test string for length testing";

  uint64_t result1 = xxhash64(base, 1, 0);            // "t"
  uint64_t result2 = xxhash64(base, 5, 0);            // "this "
  uint64_t result3 = xxhash64(base, 10, 0);           // "this is a "
  uint64_t result4 = xxhash64(base, strlen(base), 0); // full string

  TEST_ASSERT_NOT_EQUAL(result1, result2);
  TEST_ASSERT_NOT_EQUAL(result1, result3);
  TEST_ASSERT_NOT_EQUAL(result1, result4);
  TEST_ASSERT_NOT_EQUAL(result2, result3);
  TEST_ASSERT_NOT_EQUAL(result2, result4);
  TEST_ASSERT_NOT_EQUAL(result3, result4);
}

// Test small differences in input produce different hashes (avalanche effect)
void test_xxhash64_avalanche_effect(void) {
  const char input1[] = "test string";
  const char input2[] = "test strinG";  // Single character change
  const char input3[] = "test string "; // Added space

  uint64_t result1 = xxhash64(input1, strlen(input1), 0);
  uint64_t result2 = xxhash64(input2, strlen(input2), 0);
  uint64_t result3 = xxhash64(input3, strlen(input3), 0);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
  TEST_ASSERT_NOT_EQUAL(result1, result3);
  TEST_ASSERT_NOT_EQUAL(result2, result3);
}

void test_xxhash64_single_bit_change(void) {
  char input1[] = "test";
  char input2[] = "test";
  input2[0] = 'u'; // Change 't' to 'u' (single bit difference)

  uint64_t result1 = xxhash64(input1, 4, 0);
  uint64_t result2 = xxhash64(input2, 4, 0);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
}

// Test binary data handling
void test_xxhash64_binary_data(void) {
  uint8_t binary1[] = {0x00, 0x01, 0x02, 0x03, 0xFF, 0xFE, 0xFD, 0xFC};
  uint8_t binary2[] = {0xFF, 0xFE, 0xFD, 0xFC, 0x00, 0x01, 0x02, 0x03};

  uint64_t result1 = xxhash64(binary1, sizeof(binary1), 0);
  uint64_t result2 = xxhash64(binary2, sizeof(binary2), 0);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
}

void test_xxhash64_null_bytes(void) {
  uint8_t data_with_nulls[] = {'a', 'b', '\0', 'c', 'd', '\0', '\0', 'e'};
  uint8_t data_without_nulls[] = {'a', 'b', 'x', 'c', 'd', 'y', 'z', 'e'};

  uint64_t result1 = xxhash64(data_with_nulls, sizeof(data_with_nulls), 0);
  uint64_t result2 =
      xxhash64(data_without_nulls, sizeof(data_without_nulls), 0);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
}

// Test edge cases for different data sizes
void test_xxhash64_various_sizes(void) {
  // Test sizes around algorithm boundaries
  char *test_data =
      "0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF";

  uint64_t results[10];
  size_t sizes[] = {0, 1, 4, 7, 8, 15, 16, 31, 32, 33};

  for (int i = 0; i < 10; i++) {
    results[i] = xxhash64(test_data, sizes[i], 0);
  }

  // All results should be different (except empty case handled separately)
  for (int i = 1; i < 10; i++) {
    for (int j = i + 1; j < 10; j++) {
      TEST_ASSERT_NOT_EQUAL(results[i], results[j]);
    }
  }
}

// Test large data
void test_xxhash64_large_data(void) {
  size_t large_size = 1024;
  char *large_data = malloc(large_size);
  TEST_ASSERT_NOT_NULL(large_data);

  // Fill with pattern
  for (size_t i = 0; i < large_size; i++) {
    large_data[i] = (char)(i % 256);
  }

  uint64_t result = xxhash64(large_data, large_size, 0);
  TEST_ASSERT_NOT_EQUAL(0, result);

  free(large_data);
}

void test_xxhash64_very_large_data(void) {
  size_t very_large_size = 10000;
  char *very_large_data = malloc(very_large_size);
  TEST_ASSERT_NOT_NULL(very_large_data);

  // Fill with repeating pattern
  for (size_t i = 0; i < very_large_size; i++) {
    very_large_data[i] = (char)((i * 7) % 256); // Some pattern
  }

  uint64_t result1 = xxhash64(very_large_data, very_large_size, 0);
  uint64_t result2 = xxhash64(very_large_data, very_large_size, 1);

  TEST_ASSERT_NOT_EQUAL(0, result1);
  TEST_ASSERT_NOT_EQUAL(0, result2);
  TEST_ASSERT_NOT_EQUAL(result1, result2);

  free(very_large_data);
}

// Test alignment and endianness handling
void test_xxhash64_unaligned_data(void) {
  // Create unaligned data by offsetting into a larger buffer
  char buffer[100];
  strcpy(buffer + 1, "unaligned test data that should work correctly");
  strcpy(buffer + 3, "offset test data that should also work correctly");

  uint64_t result1 = xxhash64(buffer + 1, strlen(buffer + 1), 0);
  uint64_t result2 = xxhash64(buffer + 3, strlen(buffer + 3), 0);

  TEST_ASSERT_NOT_EQUAL(result1, result2);
}

// Test consistency across multiple data patterns
void test_xxhash64_pattern_consistency(void) {
  struct {
    const char *data;
    uint64_t seed;
  } test_cases[] = {
      {"", 0},
      {"a", 0},
      {"abc", 0},
      {"message digest", 0},
      {"abcdefghijklmnopqrstuvwxyz", 0},
      {"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", 0},
      {"12345678901234567890123456789012345678901234567890123456789012345678901"
       "23456789",
       0},
      {"test", 123456789},
      {"test", UINT64_MAX}};

  size_t num_cases = sizeof(test_cases) / sizeof(test_cases[0]);
  uint64_t results[sizeof(test_cases) / sizeof(test_cases[0])];

  // Calculate all hashes
  for (size_t i = 0; i < num_cases; i++) {
    results[i] = xxhash64(test_cases[i].data, strlen(test_cases[i].data),
                          test_cases[i].seed);
  }

  // Verify they're all different (very high probability they should be)
  for (size_t i = 0; i < num_cases; i++) {
    for (size_t j = i + 1; j < num_cases; j++) {
      TEST_ASSERT_NOT_EQUAL(results[i], results[j]);
    }
  }
}

// Test return type is full 64-bit
void test_xxhash64_full_64bit_range(void) {
  // Test that we can get values in different parts of the 64-bit range
  uint64_t results[100];
  bool found_high_bit = false;
  bool found_mid_bit = false;

  // Generate many hashes with different seeds
  for (int i = 0; i < 100; i++) {
    char data[20];
    snprintf(data, sizeof(data), "test_data_%d", i);
    results[i] = xxhash64(data, strlen(data), (uint64_t)i);

    if (results[i] & 0x8000000000000000ULL)
      found_high_bit = true;
    if (results[i] & 0x0000000080000000ULL)
      found_mid_bit = true;
  }

  // We should find values that use the full 64-bit range
  TEST_ASSERT_TRUE(found_high_bit);
  TEST_ASSERT_TRUE(found_mid_bit);
}

// Test with zero-length input but non-null pointer
void test_xxhash64_zero_length_non_null(void) {
  const char input[] = "this string won't be read";
  uint64_t result = xxhash64(input, 0, 0);
  TEST_ASSERT_NOT_EQUAL(0, result);

  // Should be same as hashing empty string
  uint64_t empty_result = xxhash64("", 0, 0);
  TEST_ASSERT_EQUAL(empty_result, result);
}

// Test distribution properties (basic check)
void test_xxhash64_distribution_basic(void) {
  uint64_t hashes[256];
  int bucket_counts[16] = {0}; // Simple bucketing test

  // Generate hashes for sequential inputs
  for (int i = 0; i < 256; i++) {
    char input[10];
    snprintf(input, sizeof(input), "%d", i);
    hashes[i] = xxhash64(input, strlen(input), 0);

    // Simple distribution test - count hashes in buckets
    int bucket = (int)((hashes[i] >> 60) & 0xF);
    bucket_counts[bucket]++;
  }

  // Basic distribution check - no bucket should be completely empty
  // (this is a very weak test, but helps catch obvious problems)
  int empty_buckets = 0;
  for (int i = 0; i < 16; i++) {
    if (bucket_counts[i] == 0)
      empty_buckets++;
  }

  // Allow a few empty buckets, but not too many
  TEST_ASSERT_LESS_THAN(8, empty_buckets);
}

// Main test runner
int main(void) {
  UNITY_BEGIN();

  // Basic functionality tests
  RUN_TEST(test_xxhash64_empty_input);
  RUN_TEST(test_xxhash64_single_byte);
  RUN_TEST(test_xxhash64_short_string);
  RUN_TEST(test_xxhash64_medium_string);
  RUN_TEST(test_xxhash64_long_string);

  // Deterministic behavior tests
  RUN_TEST(test_xxhash64_deterministic);
  RUN_TEST(test_xxhash64_deterministic_multiple_calls);

  // Seed variation tests
  RUN_TEST(test_xxhash64_different_seeds);
  RUN_TEST(test_xxhash64_extreme_seeds);

  // Input variation tests
  RUN_TEST(test_xxhash64_different_lengths);
  RUN_TEST(test_xxhash64_avalanche_effect);
  RUN_TEST(test_xxhash64_single_bit_change);

  // Binary data tests
  RUN_TEST(test_xxhash64_binary_data);
  RUN_TEST(test_xxhash64_null_bytes);

  // Size boundary tests
  RUN_TEST(test_xxhash64_various_sizes);
  RUN_TEST(test_xxhash64_large_data);
  RUN_TEST(test_xxhash64_very_large_data);

  // Technical tests
  RUN_TEST(test_xxhash64_unaligned_data);
  RUN_TEST(test_xxhash64_pattern_consistency);
  RUN_TEST(test_xxhash64_full_64bit_range);
  RUN_TEST(test_xxhash64_zero_length_non_null);
  RUN_TEST(test_xxhash64_distribution_basic);

  return UNITY_END();
}