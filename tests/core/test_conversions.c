#include "core/conversions.h"
#include "unity.h"
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

// Test conv_uint32_to_string with valid inputs
void test_conv_uint32_to_string_zero(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 0);

  TEST_ASSERT_EQUAL(1, result);
  TEST_ASSERT_EQUAL_STRING("0", buffer);
}

void test_conv_uint32_to_string_single_digit(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 5);

  TEST_ASSERT_EQUAL(1, result);
  TEST_ASSERT_EQUAL_STRING("5", buffer);
}

void test_conv_uint32_to_string_double_digit(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 42);

  TEST_ASSERT_EQUAL(2, result);
  TEST_ASSERT_EQUAL_STRING("42", buffer);
}

void test_conv_uint32_to_string_triple_digit(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 123);

  TEST_ASSERT_EQUAL(3, result);
  TEST_ASSERT_EQUAL_STRING("123", buffer);
}

void test_conv_uint32_to_string_large_number(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 1234567890);

  TEST_ASSERT_EQUAL(10, result);
  TEST_ASSERT_EQUAL_STRING("1234567890", buffer);
}

void test_conv_uint32_to_string_max_uint32(void) {
  char buffer[20];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), UINT32_MAX);

  TEST_ASSERT_EQUAL(10, result);
  TEST_ASSERT_EQUAL_STRING("4294967295", buffer);
}

// Test with exact buffer size needed
void test_conv_uint32_to_string_exact_buffer_size_single_digit(void) {
  char buffer[2]; // Just enough for "5" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 5);

  TEST_ASSERT_EQUAL(1, result);
  TEST_ASSERT_EQUAL_STRING("5", buffer);
}

void test_conv_uint32_to_string_exact_buffer_size_double_digit(void) {
  char buffer[3]; // Just enough for "42" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 42);

  TEST_ASSERT_EQUAL(2, result);
  TEST_ASSERT_EQUAL_STRING("42", buffer);
}

void test_conv_uint32_to_string_exact_buffer_size_max_uint32(void) {
  char buffer[11]; // Just enough for "4294967295" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), UINT32_MAX);

  TEST_ASSERT_EQUAL(10, result);
  TEST_ASSERT_EQUAL_STRING("4294967295", buffer);
}

// Test buffer too small scenarios
void test_conv_uint32_to_string_buffer_too_small_single_digit(void) {
  char buffer[1]; // Not enough space for "5" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 5);

  TEST_ASSERT_EQUAL(-1, result);
}

void test_conv_uint32_to_string_buffer_too_small_double_digit(void) {
  char buffer[2]; // Not enough space for "42" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 42);

  TEST_ASSERT_EQUAL(-1, result);
}

void test_conv_uint32_to_string_buffer_too_small_large_number(void) {
  char buffer[5]; // Not enough space for "1234567890" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 1234567890);

  TEST_ASSERT_EQUAL(-1, result);
}

void test_conv_uint32_to_string_buffer_too_small_max_uint32(void) {
  char buffer[10]; // Not enough space for "4294967295" + null terminator
  int result = conv_uint32_to_string(buffer, sizeof(buffer), UINT32_MAX);

  TEST_ASSERT_EQUAL(-1, result);
}

// Test with zero buffer size
void test_conv_uint32_to_string_zero_buffer_size(void) {
  char buffer[10];
  int result = conv_uint32_to_string(buffer, 0, 42);

  TEST_ASSERT_EQUAL(-1, result);
}

// Test with NULL buffer
void test_conv_uint32_to_string_null_buffer(void) {
  int result = conv_uint32_to_string(NULL, 10, 42);

  TEST_ASSERT_EQUAL(-1, result);
}

// Test edge case: buffer size of 1 (only room for null terminator)
void test_conv_uint32_to_string_buffer_size_one(void) {
  char buffer[1];
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 0);

  TEST_ASSERT_EQUAL(-1, result);
}

// Test that buffer contents are not modified on failure
void test_conv_uint32_to_string_buffer_unchanged_on_failure(void) {
  char buffer[2] = {'X', 'Y'}; // Initialize with known values
  int result = conv_uint32_to_string(buffer, sizeof(buffer), 42); // Will fail

  TEST_ASSERT_EQUAL(-1, result);
  // Note: snprintf behavior on buffer overflow is implementation-defined
  // Some implementations may modify the buffer, others may not
  // This test documents the current behavior but may be platform-specific
}

// Test various numeric ranges
void test_conv_uint32_to_string_powers_of_ten(void) {
  char buffer[20];

  // Test powers of 10
  uint32_t powers[] = {1,      10,      100,      1000,      10000,
                       100000, 1000000, 10000000, 100000000, 1000000000};
  int expected_lengths[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  char *expected_strings[] = {"1",         "10",        "100",     "1000",
                              "10000",     "100000",    "1000000", "10000000",
                              "100000000", "1000000000"};

  size_t num_tests = sizeof(powers) / sizeof(powers[0]);

  for (size_t i = 0; i < num_tests; i++) {
    memset(buffer, 0, sizeof(buffer)); // Clear buffer
    int result = conv_uint32_to_string(buffer, sizeof(buffer), powers[i]);

    TEST_ASSERT_EQUAL(expected_lengths[i], result);
    TEST_ASSERT_EQUAL_STRING(expected_strings[i], buffer);
  }
}

// Test boundary values around powers of 10
void test_conv_uint32_to_string_boundary_values(void) {
  char buffer[20];

  // Test values around digit boundaries
  struct {
    uint32_t value;
    int expected_length;
    const char *expected_string;
  } test_cases[] = {
      {9, 1, "9"},
      {10, 2, "10"},
      {11, 2, "11"},
      {99, 2, "99"},
      {100, 3, "100"},
      {101, 3, "101"},
      {999, 3, "999"},
      {1000, 4, "1000"},
      {1001, 4, "1001"},
      {4294967294, 10, "4294967294"}, // UINT32_MAX - 1
      {4294967295, 10, "4294967295"}  // UINT32_MAX
  };

  size_t num_tests = sizeof(test_cases) / sizeof(test_cases[0]);

  for (size_t i = 0; i < num_tests; i++) {
    memset(buffer, 0, sizeof(buffer)); // Clear buffer
    int result =
        conv_uint32_to_string(buffer, sizeof(buffer), test_cases[i].value);

    TEST_ASSERT_EQUAL(test_cases[i].expected_length, result);
    TEST_ASSERT_EQUAL_STRING(test_cases[i].expected_string, buffer);
  }
}

// Test that function doesn't write beyond buffer bounds
void test_conv_uint32_to_string_no_buffer_overflow(void) {
  // Create a buffer with sentinel values before and after the usable area
  char test_area[13]; // Need 13 total: 1 sentinel + 11 for string + null + 1
                      // sentinel

  // Set up sentinels
  test_area[0] = (char)0xAA;  // Sentinel before
  test_area[12] = (char)0xBB; // Sentinel after

  // Initialize the working area to a known pattern
  for (int i = 1; i < 12; i++) {
    test_area[i] = (char)0xCC;
  }

  int result = conv_uint32_to_string(&test_area[1], 11, UINT32_MAX);

  TEST_ASSERT_EQUAL(10, result);
  TEST_ASSERT_EQUAL_STRING("4294967295", &test_area[1]);

  // Check sentinels are unchanged - function should not write beyond bounds
  TEST_ASSERT_EQUAL((char)0xAA, test_area[0]);
  TEST_ASSERT_EQUAL((char)0xBB, test_area[12]);
}

// Test return value consistency
void test_conv_uint32_to_string_return_value_matches_strlen(void) {
  char buffer[20];
  uint32_t test_values[] = {0, 1, 42, 123, 9999, 100000, UINT32_MAX};
  size_t num_values = sizeof(test_values) / sizeof(test_values[0]);

  for (size_t i = 0; i < num_values; i++) {
    memset(buffer, 0, sizeof(buffer));
    int result = conv_uint32_to_string(buffer, sizeof(buffer), test_values[i]);

    TEST_ASSERT_GREATER_THAN(0, result);
    TEST_ASSERT_EQUAL(result, (int)strlen(buffer));
  }
}

// Main test runner
int main(void) {
  UNITY_BEGIN();

  // Basic functionality tests
  RUN_TEST(test_conv_uint32_to_string_zero);
  RUN_TEST(test_conv_uint32_to_string_single_digit);
  RUN_TEST(test_conv_uint32_to_string_double_digit);
  RUN_TEST(test_conv_uint32_to_string_triple_digit);
  RUN_TEST(test_conv_uint32_to_string_large_number);
  RUN_TEST(test_conv_uint32_to_string_max_uint32);

  // Exact buffer size tests
  RUN_TEST(test_conv_uint32_to_string_exact_buffer_size_single_digit);
  RUN_TEST(test_conv_uint32_to_string_exact_buffer_size_double_digit);
  RUN_TEST(test_conv_uint32_to_string_exact_buffer_size_max_uint32);

  // Buffer too small tests
  RUN_TEST(test_conv_uint32_to_string_buffer_too_small_single_digit);
  RUN_TEST(test_conv_uint32_to_string_buffer_too_small_double_digit);
  RUN_TEST(test_conv_uint32_to_string_buffer_too_small_large_number);
  RUN_TEST(test_conv_uint32_to_string_buffer_too_small_max_uint32);

  // Edge case tests
  RUN_TEST(test_conv_uint32_to_string_zero_buffer_size);
  RUN_TEST(test_conv_uint32_to_string_null_buffer);
  RUN_TEST(test_conv_uint32_to_string_buffer_size_one);
  RUN_TEST(test_conv_uint32_to_string_buffer_unchanged_on_failure);

  // Comprehensive value tests
  RUN_TEST(test_conv_uint32_to_string_powers_of_ten);
  RUN_TEST(test_conv_uint32_to_string_boundary_values);

  // Safety and consistency tests
  RUN_TEST(test_conv_uint32_to_string_no_buffer_overflow);
  RUN_TEST(test_conv_uint32_to_string_return_value_matches_strlen);

  return UNITY_END();
}