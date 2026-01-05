#include "core/queue.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

void test_create_and_destroy(void) {
  queue_t *q = queue_create();
  TEST_ASSERT_NOT_NULL(q);
  TEST_ASSERT_TRUE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(0, queue_size(q));
  queue_destroy(q);
}

void test_enqueue_and_dequeue(void) {
  queue_t *q = queue_create();
  int a = 42, b = 99, c = -1;
  queue_enqueue(q, &a);
  queue_enqueue(q, &b);
  queue_enqueue(q, &c);

  TEST_ASSERT_FALSE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(3, queue_size(q));

  int *out = (int *)queue_dequeue(q);
  TEST_ASSERT_EQUAL_INT(a, *out);

  out = (int *)queue_dequeue(q);
  TEST_ASSERT_EQUAL_INT(b, *out);

  out = (int *)queue_dequeue(q);
  TEST_ASSERT_EQUAL_INT(c, *out);

  TEST_ASSERT_TRUE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(0, queue_size(q));

  // Dequeue from empty queue
  out = (int *)queue_dequeue(q);
  TEST_ASSERT_NULL(out);

  queue_destroy(q);
}

void test_peek(void) {
  queue_t *q = queue_create();
  int a = 123, b = 456;
  TEST_ASSERT_NULL(queue_peek(q));

  queue_enqueue(q, &a);
  TEST_ASSERT_EQUAL_PTR(&a, queue_peek(q));

  queue_enqueue(q, &b);
  TEST_ASSERT_EQUAL_PTR(&a, queue_peek(q));

  int *out = (int *)queue_dequeue(q);
  TEST_ASSERT_EQUAL_PTR(&a, out);
  TEST_ASSERT_EQUAL_PTR(&b, queue_peek(q));

  queue_dequeue(q);
  TEST_ASSERT_NULL(queue_peek(q));

  queue_destroy(q);
}

void test_size_and_empty(void) {
  queue_t *q = queue_create();
  TEST_ASSERT_TRUE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(0, queue_size(q));

  int a = 1;
  queue_enqueue(q, &a);
  TEST_ASSERT_FALSE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(1, queue_size(q));

  queue_dequeue(q);
  TEST_ASSERT_TRUE(queue_empty(q));
  TEST_ASSERT_EQUAL_INT(0, queue_size(q));

  queue_destroy(q);
}

void test_destroy_null(void) {
  // Should not crash
  queue_destroy(NULL);
}

void test_enqueue_null_queue(void) {
  // Should not crash
  queue_enqueue(NULL, NULL);
}

void test_dequeue_null_queue(void) {
  void *out = queue_dequeue(NULL);
  TEST_ASSERT_NULL(out);
}

void test_peek_null_queue(void) {
  void *out = queue_peek(NULL);
  TEST_ASSERT_NULL(out);
}

void test_large_number_of_elements(void) {
  queue_t *q = queue_create();
  int N = 1000;
  int *arr = malloc(sizeof(int) * N);
  TEST_ASSERT_NOT_NULL(arr);

  for (int i = 0; i < N; ++i) {
    arr[i] = i;
    queue_enqueue(q, &arr[i]);
  }
  TEST_ASSERT_EQUAL_INT(N, queue_size(q));

  for (int i = 0; i < N; ++i) {
    int *out = (int *)queue_dequeue(q);
    TEST_ASSERT_EQUAL_INT(i, *out);
  }
  TEST_ASSERT_TRUE(queue_empty(q));
  free(arr);
  queue_destroy(q);
}

void test_mixed_types(void) {
  queue_t *q = queue_create();
  int a = 1;
  float b = 3.14;
  char *c = "hello";
  queue_enqueue(q, &a);
  queue_enqueue(q, &b);
  queue_enqueue(q, c);

  TEST_ASSERT_EQUAL_INT(3, queue_size(q));
  TEST_ASSERT_EQUAL_INT(a, *(int *)queue_dequeue(q));
  TEST_ASSERT_EQUAL_FLOAT(b, *(float *)queue_dequeue(q));
  TEST_ASSERT_EQUAL_STRING(c, (char *)queue_dequeue(q));

  TEST_ASSERT_TRUE(queue_empty(q));
  queue_destroy(q);
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
