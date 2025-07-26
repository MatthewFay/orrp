#include "unity.h"

// Forward declarations for test functions
void test_the_first(void);
void test_the_second(void);

/**
 * @brief setUp function, called before each test case.
 *
 * This function can be used to set up any necessary preconditions
 * for the tests. For now, it is empty.
 */
void setUp(void) {
    // Set up code, if any
}

/**
 * @brief tearDown function, called after each test case.
 *
 * This function can be used to clean up any resources allocated
 * during the tests. For now, it is empty.
 */
void tearDown(void) {
    // Tear down code, if any
}

/**
 * @brief Main function for the test runner.
 *
 * This function initializes the Unity test framework, runs all the
 * test cases, and reports the results.
 * @return int Returns the number of test failures.
 */
int main(void) {
    UNITY_BEGIN();

    // Run the test cases
    RUN_TEST(test_the_first);
    RUN_TEST(test_the_second);

    return UNITY_END();
}
