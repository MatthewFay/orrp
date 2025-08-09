#include "core/queue.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

void test_create_and_destroy(void) {
  Queue *q = q_create();
  TEST_ASSERT_NOT_NULL(q);
  TEST_ASSERT_TRUE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(0, q_size(q));
  q_destroy(q);
}

void test_enqueue_and_dequeue(void) {
  Queue *q = q_create();
  int a = 42, b = 99, c = -1;
  q_enqueue(q, &a);
  q_enqueue(q, &b);
  q_enqueue(q, &c);

  TEST_ASSERT_FALSE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(3, q_size(q));

  int *out = (int *)q_dequeue(q);
  TEST_ASSERT_EQUAL_INT(a, *out);

  out = (int *)q_dequeue(q);
  TEST_ASSERT_EQUAL_INT(b, *out);

  out = (int *)q_dequeue(q);
  TEST_ASSERT_EQUAL_INT(c, *out);

  TEST_ASSERT_TRUE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(0, q_size(q));

  // Dequeue from empty queue
  out = (int *)q_dequeue(q);
  TEST_ASSERT_NULL(out);

  q_destroy(q);
}

void test_peek(void) {
  Queue *q = q_create();
  int a = 123, b = 456;
  TEST_ASSERT_NULL(q_peek(q));

  q_enqueue(q, &a);
  TEST_ASSERT_EQUAL_PTR(&a, q_peek(q));

  q_enqueue(q, &b);
  TEST_ASSERT_EQUAL_PTR(&a, q_peek(q));

  int *out = (int *)q_dequeue(q);
  TEST_ASSERT_EQUAL_PTR(&a, out);
  TEST_ASSERT_EQUAL_PTR(&b, q_peek(q));

  q_dequeue(q);
  TEST_ASSERT_NULL(q_peek(q));

  q_destroy(q);
}

void test_size_and_empty(void) {
  Queue *q = q_create();
  TEST_ASSERT_TRUE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(0, q_size(q));

  int a = 1;
  q_enqueue(q, &a);
  TEST_ASSERT_FALSE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(1, q_size(q));

  q_dequeue(q);
  TEST_ASSERT_TRUE(q_empty(q));
  TEST_ASSERT_EQUAL_INT(0, q_size(q));

  q_destroy(q);
}

void test_destroy_null(void) {
  // Should not crash
  q_destroy(NULL);
}

void test_enqueue_null_queue(void) {
  // Should not crash
  q_enqueue(NULL, NULL);
}

void test_dequeue_null_queue(void) {
  void *out = q_dequeue(NULL);
  TEST_ASSERT_NULL(out);
}

void test_peek_null_queue(void) {
  void *out = q_peek(NULL);
  TEST_ASSERT_NULL(out);
}

void test_large_number_of_elements(void) {
  Queue *q = q_create();
  int N = 1000;
  int *arr = malloc(sizeof(int) * N);
  TEST_ASSERT_NOT_NULL(arr);

  for (int i = 0; i < N; ++i) {
    arr[i] = i;
    q_enqueue(q, &arr[i]);
  }
  TEST_ASSERT_EQUAL_INT(N, q_size(q));

  for (int i = 0; i < N; ++i) {
    int *out = (int *)q_dequeue(q);
    TEST_ASSERT_EQUAL_INT(i, *out);
  }
  TEST_ASSERT_TRUE(q_empty(q));
  free(arr);
  q_destroy(q);
}

void test_mixed_types(void) {
  Queue *q = q_create();
  int a = 1;
  float b = 3.14;
  char *c = "hello";
  q_enqueue(q, &a);
  q_enqueue(q, &b);
  q_enqueue(q, c);

  TEST_ASSERT_EQUAL_INT(3, q_size(q));
  TEST_ASSERT_EQUAL_INT(a, *(int *)q_dequeue(q));
  TEST_ASSERT_EQUAL_FLOAT(b, *(float *)q_dequeue(q));
  TEST_ASSERT_EQUAL_STRING(c, (char *)q_dequeue(q));

  TEST_ASSERT_TRUE(q_empty(q));
  q_destroy(q);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_create_and_destroy);
  RUN_TEST(test_enqueue_and_dequeue);
  RUN_TEST(test_peek);
  RUN_TEST(test_size_and_empty);
  RUN_TEST(test_destroy_null);
  RUN_TEST(test_enqueue_null_queue);
  RUN_TEST(test_dequeue_null_queue);
  RUN_TEST(test_peek_null_queue);
  RUN_TEST(test_large_number_of_elements);
  RUN_TEST(test_mixed_types);
  return UNITY_END();
}
