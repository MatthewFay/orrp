#include "core/stack.h"
#include "unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// A simple struct to test with the stack
typedef struct {
  int id;
  char name[20];
} TestData;

// A global stack pointer that is managed by the setUp/tearDown functions.
static c_stack_t *my_stack = NULL;

// This function is run before each test case.
// It ensures that each test starts with a fresh, empty stack,
// making them independent of one another.
void setUp(void) {
  my_stack = stack_create();
  TEST_ASSERT_NOT_NULL(my_stack);
}

// This function is run after each test case.
// It frees the stack and any memory allocated during the test.
// Since all test cases now push dynamically allocated memory,
// this function can safely free each popped value.
void tearDown(void) {
  if (my_stack) {
    // Pop and free any remaining dynamically allocated memory
    while (!stack_is_empty(my_stack)) {
      void *popped_value = stack_pop(my_stack);
      free(popped_value);
    }
    stack_free(my_stack);
    my_stack = NULL;
  }
}

// Test case 1: A newly created stack should be empty and have a count of 0.
void test_stack_create_is_empty_and_zero_count(void) {
  TEST_ASSERT_TRUE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 2: Pushing a value should make the stack not empty and increment
// count.
void test_stack_push_changes_stack_state(void) {
  int *test_value = (int *)malloc(sizeof(int));
  TEST_ASSERT_NOT_NULL(test_value);
  *test_value = 10;
  bool result = stack_push(my_stack, test_value);
  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_FALSE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(1, my_stack->count);
}

// Test case 3: Pushing and peeking at a pointer to dynamically allocated
// memory.
void test_stack_push_and_peek_dynamically_allocated_pointer(void) {
  int *test_value = (int *)malloc(sizeof(int));
  TEST_ASSERT_NOT_NULL(test_value);
  *test_value = 123;
  stack_push(my_stack, test_value);
  int *peeked_value = (int *)stack_peek(my_stack);
  TEST_ASSERT_EQUAL_PTR(test_value, peeked_value);
  TEST_ASSERT_EQUAL(123, *peeked_value);
}

// Test case 4: Pushing and popping a pointer to dynamically allocated memory.
void test_stack_push_and_pop_dynamically_allocated_pointer(void) {
  int *test_value = (int *)malloc(sizeof(int));
  TEST_ASSERT_NOT_NULL(test_value);
  *test_value = 456;
  stack_push(my_stack, test_value);
  int *popped_value = (int *)stack_pop(my_stack);
  TEST_ASSERT_EQUAL_PTR(test_value, popped_value);
  TEST_ASSERT_EQUAL(456, *popped_value);
  TEST_ASSERT_TRUE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 5: Pushing and popping a dynamically allocated integer pointer.
void test_stack_push_and_pop_malloc_pointer(void) {
  int *ptr = (int *)malloc(sizeof(int));
  TEST_ASSERT_NOT_NULL(ptr);
  *ptr = 789;

  bool result = stack_push(my_stack, ptr);
  TEST_ASSERT_TRUE(result);
  int *popped_ptr = (int *)stack_pop(my_stack);

  TEST_ASSERT_EQUAL(789, *popped_ptr);
  TEST_ASSERT_EQUAL_PTR(ptr, popped_ptr);
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 6: Pushing and popping a pointer to a struct.
void test_stack_push_and_pop_struct_pointer(void) {
  TestData *data_ptr = (TestData *)malloc(sizeof(TestData));
  TEST_ASSERT_NOT_NULL(data_ptr);
  data_ptr->id = 1;
  strcpy(data_ptr->name, "Unity Test");

  bool result = stack_push(my_stack, data_ptr);
  TEST_ASSERT_TRUE(result);
  TestData *popped_data = (TestData *)stack_pop(my_stack);

  TEST_ASSERT_EQUAL_PTR(data_ptr, popped_data);
  TEST_ASSERT_EQUAL(1, popped_data->id);
  TEST_ASSERT_EQUAL_STRING("Unity Test", popped_data->name);
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 7: Popping from an empty stack should return NULL and not change
// the count.
void test_stack_pop_from_empty_stack_returns_null(void) {
  void *popped_value = stack_pop(my_stack);
  TEST_ASSERT_NULL(popped_value);
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 8: Peeking at an empty stack should return NULL and not change the
// count.
void test_stack_peek_from_empty_stack_returns_null(void) {
  void *peeked_value = stack_peek(my_stack);
  TEST_ASSERT_NULL(peeked_value);
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 9: Pushing and popping multiple values should follow LIFO order and
// update the count.
void test_stack_push_and_pop_multiple_values_LIFO(void) {
  int *value1 = (int *)malloc(sizeof(int));
  int *value2 = (int *)malloc(sizeof(int));
  TestData *data_ptr = (TestData *)malloc(sizeof(TestData));
  TEST_ASSERT_NOT_NULL(value1);
  TEST_ASSERT_NOT_NULL(value2);
  TEST_ASSERT_NOT_NULL(data_ptr);

  *value1 = 111;
  *value2 = 222;
  data_ptr->id = 333;

  TEST_ASSERT_TRUE(stack_push(my_stack, value1));
  TEST_ASSERT_EQUAL(1, my_stack->count);
  TEST_ASSERT_TRUE(stack_push(my_stack, value2));
  TEST_ASSERT_EQUAL(2, my_stack->count);
  TEST_ASSERT_TRUE(stack_push(my_stack, data_ptr));
  TEST_ASSERT_EQUAL(3, my_stack->count);

  // First pop should be the last item pushed (data_ptr).
  TestData *popped_data = (TestData *)stack_pop(my_stack);
  TEST_ASSERT_EQUAL_PTR(data_ptr, popped_data);
  TEST_ASSERT_EQUAL(2, my_stack->count);

  // Second pop should be the second item pushed (value2).
  int *popped_value_2 = (int *)stack_pop(my_stack);
  TEST_ASSERT_EQUAL_PTR(value2, popped_value_2);
  TEST_ASSERT_EQUAL(222, *popped_value_2);
  TEST_ASSERT_EQUAL(1, my_stack->count);

  // Third pop should be the first item pushed (value1).
  int *popped_value_1 = (int *)stack_pop(my_stack);
  TEST_ASSERT_EQUAL_PTR(value1, popped_value_1);
  TEST_ASSERT_EQUAL(111, *popped_value_1);
  TEST_ASSERT_EQUAL(0, my_stack->count);

  TEST_ASSERT_TRUE(stack_is_empty(my_stack));
}

// Test case 10: Pushing and popping a NULL pointer.
void test_stack_push_and_pop_null_pointer(void) {
  bool result = stack_push(my_stack, NULL);
  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(1, my_stack->count);

  void *popped_value = stack_pop(my_stack);
  TEST_ASSERT_NULL(popped_value);
  TEST_ASSERT_TRUE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(0, my_stack->count);
}

// Test case 11: Peeking multiple times should not change the stack state.
void test_stack_peek_multiple_times_does_not_change_stack(void) {
  int *test_value = (int *)malloc(sizeof(int));
  TEST_ASSERT_NOT_NULL(test_value);
  *test_value = 100;
  stack_push(my_stack, test_value);
  TEST_ASSERT_EQUAL(1, my_stack->count);

  void *first_peek = stack_peek(my_stack);
  void *second_peek = stack_peek(my_stack);

  TEST_ASSERT_EQUAL_PTR(first_peek, second_peek);
  TEST_ASSERT_FALSE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(1, my_stack->count);
}

// Test case 12: A stress test pushing and popping a large number of items.
void test_stack_stress_test(void) {
  const int num_items = 1000;
  int **pointers = (int **)malloc(sizeof(int *) * num_items);
  TEST_ASSERT_NOT_NULL(pointers);

  // Push a large number of pointers.
  for (int i = 0; i < num_items; i++) {
    pointers[i] = (int *)malloc(sizeof(int));
    TEST_ASSERT_NOT_NULL(pointers[i]);
    *pointers[i] = i;
    TEST_ASSERT_TRUE(stack_push(my_stack, pointers[i]));
  }
  TEST_ASSERT_EQUAL(num_items, my_stack->count);

  // Pop all items and check LIFO order.
  for (int i = num_items - 1; i >= 0; i--) {
    int *popped_ptr = (int *)stack_pop(my_stack);
    TEST_ASSERT_EQUAL(i, *popped_ptr);
    TEST_ASSERT_EQUAL_PTR(pointers[i], popped_ptr);
    free(popped_ptr); // free here since the tearDown loop will not get these
  }

  TEST_ASSERT_TRUE(stack_is_empty(my_stack));
  TEST_ASSERT_EQUAL(0, my_stack->count);
  free(pointers);
}

// This is the main function that runs all the tests.
int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_stack_create_is_empty_and_zero_count);
  RUN_TEST(test_stack_push_changes_stack_state);
  RUN_TEST(test_stack_push_and_peek_dynamically_allocated_pointer);
  RUN_TEST(test_stack_push_and_pop_dynamically_allocated_pointer);
  RUN_TEST(test_stack_push_and_pop_malloc_pointer);
  RUN_TEST(test_stack_push_and_pop_struct_pointer);
  RUN_TEST(test_stack_pop_from_empty_stack_returns_null);
  RUN_TEST(test_stack_peek_from_empty_stack_returns_null);
  RUN_TEST(test_stack_push_and_pop_multiple_values_LIFO);
  RUN_TEST(test_stack_push_and_pop_null_pointer);
  RUN_TEST(test_stack_peek_multiple_times_does_not_change_stack);
  RUN_TEST(test_stack_stress_test);
  return UNITY_END();
}
