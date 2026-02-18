#include "engine/container/container_cache.h"
#include "engine/container/container_types.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Helper function to create a test container
static eng_container_t *create_test_container(const char *name) {
  eng_container_t *c = calloc(1, sizeof(eng_container_t));
  c->name = strdup(name);
  return c;
}

// Helper function to create a test node
static container_cache_node_t *create_test_node(const char *name) {
  container_cache_node_t *node = calloc(1, sizeof(container_cache_node_t));
  node->container = create_test_container(name);
  return node;
}

// Helper to free a node and its container
static void free_test_node(container_cache_node_t *node) {
  if (node && node->container) {
    free(node->container->name);
    free(node->container);
  }
  free(node);
}

void setUp(void) {
  // Called before each test
}

void tearDown(void) {
  // Called after each test
}

// Mocks
void container_close(eng_container_t *c) { (void)c; }

// ============= container_cache_create tests =============

void test_container_cache_create_success(void) {
  container_cache_t *cache = container_cache_create(10);

  TEST_ASSERT_NOT_NULL(cache);
  TEST_ASSERT_EQUAL(10, cache->capacity);
  TEST_ASSERT_EQUAL(0, cache->size);
  TEST_ASSERT_NULL(cache->head);
  TEST_ASSERT_NULL(cache->tail);
  TEST_ASSERT_NULL(cache->nodes);

  container_cache_destroy(cache);
}

void test_container_cache_create_zero_capacity(void) {
  container_cache_t *cache = container_cache_create(0);

  TEST_ASSERT_NOT_NULL(cache);
  TEST_ASSERT_EQUAL(0, cache->capacity);

  container_cache_destroy(cache);
}

// ============= container_cache_destroy tests =============

void test_container_cache_destroy_null(void) {
  // Should not crash
  container_cache_destroy(NULL);
  TEST_ASSERT_TRUE(true);
}

void test_container_cache_destroy_with_nodes(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node = create_test_node("test");

  container_cache_put(cache, node);

  // Should free all nodes without leaking
  container_cache_destroy(cache);
  TEST_ASSERT_TRUE(true);
}

// ============= container_cache_get tests =============

void test_container_cache_get_null_cache(void) {
  container_cache_node_t *result = container_cache_get(NULL, "test");
  TEST_ASSERT_NULL(result);
}

void test_container_cache_get_null_name(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *result = container_cache_get(cache, NULL);

  TEST_ASSERT_NULL(result);
  container_cache_destroy(cache);
}

void test_container_cache_get_existing_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node = create_test_node("mycontainer");

  container_cache_put(cache, node);
  container_cache_node_t *result = container_cache_get(cache, "mycontainer");

  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL_PTR(node, result);
  TEST_ASSERT_EQUAL_STRING("mycontainer", result->container->name);

  container_cache_destroy(cache);
}

void test_container_cache_get_nonexistent_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *result = container_cache_get(cache, "notfound");

  TEST_ASSERT_NULL(result);
  container_cache_destroy(cache);
}

// ============= container_cache_put tests =============

void test_container_cache_put_null_cache(void) {
  container_cache_node_t *node = create_test_node("test");
  bool result = container_cache_put(NULL, node);

  TEST_ASSERT_FALSE(result);
  free_test_node(node);
}

void test_container_cache_put_null_node(void) {
  container_cache_t *cache = container_cache_create(5);
  bool result = container_cache_put(cache, NULL);

  TEST_ASSERT_FALSE(result);
  container_cache_destroy(cache);
}

void test_container_cache_put_node_with_null_container(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node = calloc(1, sizeof(container_cache_node_t));
  node->container = NULL;

  bool result = container_cache_put(cache, node);

  TEST_ASSERT_FALSE(result);
  free(node);
  container_cache_destroy(cache);
}

void test_container_cache_put_single_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node = create_test_node("test1");

  bool result = container_cache_put(cache, node);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(1, cache->size);
  TEST_ASSERT_EQUAL_PTR(node, cache->head);
  TEST_ASSERT_EQUAL_PTR(node, cache->tail);
  TEST_ASSERT_NULL(node->prev);
  TEST_ASSERT_NULL(node->next);

  container_cache_destroy(cache);
}

void test_container_cache_put_multiple_nodes(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  TEST_ASSERT_EQUAL(3, cache->size);
  TEST_ASSERT_EQUAL_PTR(node3, cache->head);
  TEST_ASSERT_EQUAL_PTR(node1, cache->tail);
  TEST_ASSERT_EQUAL_PTR(node2, node3->next);
  TEST_ASSERT_EQUAL_PTR(node3, node2->prev);

  container_cache_destroy(cache);
}

// ============= container_cache_move_to_front tests =============

void test_container_cache_move_to_front_null_cache(void) {
  container_cache_node_t *node = create_test_node("test");
  // Should not crash
  container_cache_move_to_front(NULL, node);
  TEST_ASSERT_TRUE(true);
  free_test_node(node);
}

void test_container_cache_move_to_front_null_node(void) {
  container_cache_t *cache = container_cache_create(5);
  // Should not crash
  container_cache_move_to_front(cache, NULL);
  TEST_ASSERT_TRUE(true);
  container_cache_destroy(cache);
}

void test_container_cache_move_to_front_already_at_front(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);

  container_cache_move_to_front(cache, node2);

  TEST_ASSERT_EQUAL_PTR(node2, cache->head);
  TEST_ASSERT_EQUAL_PTR(node1, cache->tail);

  container_cache_destroy(cache);
}

void test_container_cache_move_to_front_from_middle(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  container_cache_move_to_front(cache, node2);

  TEST_ASSERT_EQUAL_PTR(node2, cache->head);
  TEST_ASSERT_EQUAL_PTR(node1, cache->tail);
  TEST_ASSERT_EQUAL_PTR(node3, node2->next);
  TEST_ASSERT_NULL(node2->prev);

  container_cache_destroy(cache);
}

void test_container_cache_move_to_front_from_tail(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  container_cache_move_to_front(cache, node1);

  TEST_ASSERT_EQUAL_PTR(node1, cache->head);
  TEST_ASSERT_EQUAL_PTR(node2, cache->tail);
  TEST_ASSERT_NULL(node1->prev);
  TEST_ASSERT_EQUAL_PTR(node3, node1->next);

  container_cache_destroy(cache);
}

// ============= container_cache_remove tests =============

void test_container_cache_remove_null_cache(void) {
  container_cache_node_t *node = create_test_node("test");
  bool result = container_cache_remove(NULL, node);

  TEST_ASSERT_FALSE(result);
  free_test_node(node);
}

void test_container_cache_remove_null_node(void) {
  container_cache_t *cache = container_cache_create(5);
  bool result = container_cache_remove(cache, NULL);

  TEST_ASSERT_FALSE(result);
  container_cache_destroy(cache);
}

void test_container_cache_remove_only_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node = create_test_node("test1");

  container_cache_put(cache, node);
  bool result = container_cache_remove(cache, node);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(0, cache->size);
  TEST_ASSERT_NULL(cache->head);
  TEST_ASSERT_NULL(cache->tail);

  container_cache_destroy(cache);
}

void test_container_cache_remove_head_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  bool result = container_cache_remove(cache, node3);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(2, cache->size);
  TEST_ASSERT_EQUAL_PTR(node2, cache->head);
  TEST_ASSERT_EQUAL_PTR(node1, cache->tail);
  TEST_ASSERT_NULL(node2->prev);

  container_cache_destroy(cache);
}

void test_container_cache_remove_tail_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  bool result = container_cache_remove(cache, node1);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(2, cache->size);
  TEST_ASSERT_EQUAL_PTR(node3, cache->head);
  TEST_ASSERT_EQUAL_PTR(node2, cache->tail);
  TEST_ASSERT_NULL(node2->next);

  container_cache_destroy(cache);
}

void test_container_cache_remove_middle_node(void) {
  container_cache_t *cache = container_cache_create(5);
  container_cache_node_t *node1 = create_test_node("test1");
  container_cache_node_t *node2 = create_test_node("test2");
  container_cache_node_t *node3 = create_test_node("test3");

  container_cache_put(cache, node1);
  container_cache_put(cache, node2);
  container_cache_put(cache, node3);

  bool result = container_cache_remove(cache, node2);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_EQUAL(2, cache->size);
  TEST_ASSERT_EQUAL_PTR(node3, cache->head);
  TEST_ASSERT_EQUAL_PTR(node1, cache->tail);
  TEST_ASSERT_EQUAL_PTR(node1, node3->next);
  TEST_ASSERT_EQUAL_PTR(node3, node1->prev);

  container_cache_destroy(cache);
}

// ============= Main test runner =============

int main(void) {
  UNITY_BEGIN();

  // Create tests
  RUN_TEST(test_container_cache_create_success);
  RUN_TEST(test_container_cache_create_zero_capacity);

  // Destroy tests
  RUN_TEST(test_container_cache_destroy_null);
  RUN_TEST(test_container_cache_destroy_with_nodes);

  // Get tests
  RUN_TEST(test_container_cache_get_null_cache);
  RUN_TEST(test_container_cache_get_null_name);
  RUN_TEST(test_container_cache_get_existing_node);
  RUN_TEST(test_container_cache_get_nonexistent_node);

  // Put tests
  RUN_TEST(test_container_cache_put_null_cache);
  RUN_TEST(test_container_cache_put_null_node);
  RUN_TEST(test_container_cache_put_node_with_null_container);
  RUN_TEST(test_container_cache_put_single_node);
  RUN_TEST(test_container_cache_put_multiple_nodes);

  // Move to front tests
  RUN_TEST(test_container_cache_move_to_front_null_cache);
  RUN_TEST(test_container_cache_move_to_front_null_node);
  RUN_TEST(test_container_cache_move_to_front_already_at_front);
  RUN_TEST(test_container_cache_move_to_front_from_middle);
  RUN_TEST(test_container_cache_move_to_front_from_tail);

  // Remove tests
  RUN_TEST(test_container_cache_remove_null_cache);
  RUN_TEST(test_container_cache_remove_null_node);
  RUN_TEST(test_container_cache_remove_only_node);
  RUN_TEST(test_container_cache_remove_head_node);
  RUN_TEST(test_container_cache_remove_tail_node);
  RUN_TEST(test_container_cache_remove_middle_node);

  return UNITY_END();
}