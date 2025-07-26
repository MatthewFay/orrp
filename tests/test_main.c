#include "unity.h"
#include "db.h" // Include the header for the code we are testing

/**
 * @brief A simple test case to ensure the testing framework is working.
 *
 * This test uses a basic assertion to verify that true is true.
 * Its purpose is to confirm that the test build process and runner
 * are correctly configured.
 */
void test_the_first(void) {
    TEST_ASSERT_EQUAL(1, 1);
}

/**
 * @brief Another simple test case.
 *
 * This test demonstrates a slightly more complex assertion, checking
 * a range. This helps to ensure different assertion types are working.
 */
void test_the_second(void) {
    TEST_ASSERT_INT_WITHIN(10, 50, 55);
}
