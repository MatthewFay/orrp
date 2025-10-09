#include "engine/dc_cache/dc_cache.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// --- Mocking Framework ---

// A mock implementation of the container struct.
// The cache only cares about the 'name' field for the key.
typedef struct mock_container_s {
  const char *name;
  int id;
} mock_container_t;

// Global counters to track calls to our mock functions.
static int g_create_count = 0;
static int g_close_count = 0;

// Mock function to create a container.
// This will be passed as the 'create_fn' to the cache.
eng_container_t *mock_create_container(const char *name) {
  g_create_count++;
  mock_container_t *c = malloc(sizeof(mock_container_t));
  if (!c)
    return NULL;

  // The cache uses the container's name as its key, so we must duplicate it.
  char *name_dup = strdup(name);
  if (!name_dup) {
    free(c);
    return NULL;
  }
  c->name = name_dup;
  c->id = g_create_count; // Assign a unique ID for potential debugging.
  return (eng_container_t *)c;
}

// Mock function to close/destroy a container.
// This is used by the cache during eviction and destruction.
void eng_container_close(eng_container_t *c) {
  if (c) {
    g_close_count++;
    // Free the duplicated name string and the container itself.
    free((void *)c->name);
    free(c);
  }
}

// --- Test Suite Setup ---

// This function is run before each test.
void setUp(void) {
  // Reset counters and initialize the cache with a known capacity.
  g_create_count = 0;
  g_close_count = 0;
  eng_dc_cache_init(3, mock_create_container);
}

// This function is run after each test.
void tearDown(void) {
  // Clean up the cache, ensuring all created containers are closed.
  eng_dc_cache_destroy();
  // Verify that every created container was eventually closed.
  TEST_ASSERT_EQUAL_INT(g_create_count, g_close_count);
}

// --- Test Cases ---

// Test basic initialization and destruction.
void test_init_and_destroy(void) {
  // The setUp and tearDown functions handle the logic.
  // This test passes if they complete without errors.
  TEST_ASSERT_EQUAL_INT(0, g_create_count);
  TEST_ASSERT_EQUAL_INT(0, g_close_count);
}

// Test getting a single item for the first time (cache miss).
void test_get_single_item_cache_miss(void) {
  eng_container_t *c1 = eng_dc_cache_get("item1");

  TEST_ASSERT_NOT_NULL(c1);
  TEST_ASSERT_EQUAL_STRING("item1", c1->name);
  TEST_ASSERT_EQUAL_INT(1, g_create_count); // Should be created once.
  TEST_ASSERT_EQUAL_INT(0, g_close_count);  // Should not be closed.

  eng_dc_cache_release_container(c1);
}

// Test getting the same item twice (cache miss, then cache hit).
void test_get_same_item_twice_cache_hit(void) {
  eng_container_t *c1 = eng_dc_cache_get("item1");
  eng_container_t *c2 = eng_dc_cache_get("item1");

  TEST_ASSERT_NOT_NULL(c1);
  TEST_ASSERT_NOT_NULL(c2);
  TEST_ASSERT_EQUAL_PTR(c1, c2);            // Pointers should be identical.
  TEST_ASSERT_EQUAL_INT(1, g_create_count); // Still only created once.
  TEST_ASSERT_EQUAL_INT(0, g_close_count);

  eng_dc_cache_release_container(c1);
  eng_dc_cache_release_container(c2);
}

// Test that releasing a container doesn't immediately evict it.
void test_release_does_not_evict(void) {
  eng_container_t *c1 = eng_dc_cache_get("item1");
  eng_dc_cache_release_container(c1);

  // At this point, ref_count is 0, but it's still in the cache.
  TEST_ASSERT_EQUAL_INT(1, g_create_count);
  TEST_ASSERT_EQUAL_INT(0, g_close_count);

  // Get it again to confirm it wasn't re-created.
  eng_container_t *c2 = eng_dc_cache_get("item1");
  TEST_ASSERT_EQUAL_PTR(c1, c2);
  TEST_ASSERT_EQUAL_INT(1, g_create_count); // No new creation.

  eng_dc_cache_release_container(c2);
}

// Test the core LRU eviction policy.
void test_lru_eviction_occurs_on_capacity(void) {
  // Load three items, filling the cache (capacity = 3).
  eng_container_t *c1 = eng_dc_cache_get("item1");
  eng_container_t *c2 = eng_dc_cache_get("item2");
  eng_container_t *c3 = eng_dc_cache_get("item3");

  // Release them so they become eviction candidates.
  // Order of release doesn't matter, order of "get" does.
  // LRU order: item3 (MRU), item2, item1 (LRU)
  eng_dc_cache_release_container(c1);
  eng_dc_cache_release_container(c2);
  eng_dc_cache_release_container(c3);

  TEST_ASSERT_EQUAL_INT(3, g_create_count);
  TEST_ASSERT_EQUAL_INT(0, g_close_count);

  // Request a fourth unique item, which should trigger eviction.
  eng_container_t *c4 = eng_dc_cache_get("item4");
  eng_dc_cache_release_container(c4);

  // A new container was created, and the LRU one ("item1") was closed.
  TEST_ASSERT_EQUAL_INT(4, g_create_count);
  TEST_ASSERT_EQUAL_INT(1, g_close_count);

  // Verify that "item1" is no longer in the cache by requesting it again.
  eng_container_t *c1_new = eng_dc_cache_get("item1");
  TEST_ASSERT_NOT_EQUAL(c1, c1_new);        // Should be a new instance.
  TEST_ASSERT_EQUAL_INT(5, g_create_count); // It was created again.

  eng_dc_cache_release_container(c1_new);
}

// Test that accessing an item moves it to the front of the LRU list.
void test_accessing_item_updates_lru_order(void) {
  // Load three items, filling the cache.
  eng_container_t *c1 = eng_dc_cache_get("item1");
  eng_container_t *c2 = eng_dc_cache_get("item2");
  eng_container_t *c3 = eng_dc_cache_get("item3");
  eng_dc_cache_release_container(c1);
  eng_dc_cache_release_container(c2);
  eng_dc_cache_release_container(c3);
  // Current LRU order: item3 (MRU), item2, item1 (LRU)

  // Access "item1" again, which should make it the most recently used.
  eng_container_t *c1_re_get = eng_dc_cache_get("item1");
  TEST_ASSERT_EQUAL_PTR(c1, c1_re_get); // Ensure it's the same object.
  eng_dc_cache_release_container(c1_re_get);
  // New LRU order: item1 (MRU), item3, item2 (LRU)
  TEST_ASSERT_EQUAL_INT(3, g_create_count); // No new items created.

  // Request a fourth item to trigger eviction.
  eng_container_t *c4 = eng_dc_cache_get("item4");
  eng_dc_cache_release_container(c4);

  // "item2" should have been evicted, not "item1" or "item3".
  TEST_ASSERT_EQUAL_INT(4, g_create_count);
  TEST_ASSERT_EQUAL_INT(1, g_close_count);

  // Verify "item1" is still in the cache (it was protected by being
  // re-accessed).
  eng_container_t *c1_still_present = eng_dc_cache_get("item1");
  TEST_ASSERT_EQUAL_PTR(c1, c1_still_present);
  TEST_ASSERT_EQUAL_INT(4, g_create_count); // Not re-created.
  eng_dc_cache_release_container(c1_still_present);

  // Verify "item2" was evicted by re-requesting it.
  eng_container_t *c2_new = eng_dc_cache_get("item2");
  TEST_ASSERT_NOT_EQUAL(c2, c2_new);        // Should be a new pointer.
  TEST_ASSERT_EQUAL_INT(5, g_create_count); // Must be re-created.
  eng_dc_cache_release_container(c2_new);
}

// Test that an item with a reference count > 0 is protected from eviction.
void test_eviction_skips_item_in_use(void) {
  // Load items to fill the cache.
  eng_container_t *c1 = eng_dc_cache_get("item1"); // LRU candidate
  eng_container_t *c2 = eng_dc_cache_get("item2");
  eng_container_t *c3 = eng_dc_cache_get("item3"); // MRU

  // Keep a reference to c1, but release the others.
  // c1 has ref_count = 1. c2 and c3 have ref_count = 0.
  eng_dc_cache_release_container(c2);
  eng_dc_cache_release_container(c3);

  // Request a new item. Eviction should occur.
  // The eviction logic should walk from the tail (c1), find it's in use,
  // move to the next candidate (c2), and evict c2.
  eng_container_t *c4 = eng_dc_cache_get("item4");

  TEST_ASSERT_EQUAL_INT(4, g_create_count);
  TEST_ASSERT_EQUAL_INT(1, g_close_count); // One item was closed.

  // Verify c1 (the original LRU) is still in the cache.
  eng_container_t *c1_check = eng_dc_cache_get("item1");
  TEST_ASSERT_EQUAL_PTR(c1, c1_check);
  TEST_ASSERT_EQUAL_INT(4, g_create_count); // No new creation for c1.

  // Verify c2 was the one evicted.
  eng_container_t *c2_check = eng_dc_cache_get("item2");
  TEST_ASSERT_NOT_EQUAL(c2, c2_check);
  TEST_ASSERT_EQUAL_INT(5, g_create_count); // c2 was re-created.

  eng_dc_cache_release_container(c1);
  eng_dc_cache_release_container(c1_check);
  eng_dc_cache_release_container(c2_check);
  eng_dc_cache_release_container(c4);
}

// Test the documented behavior that the cache grows if no items can be evicted.
void test_cache_grows_when_all_items_are_in_use(void) {
  // Get 3 items, filling capacity, and DO NOT release them.
  eng_container_t *c1 = eng_dc_cache_get("item1");
  eng_container_t *c2 = eng_dc_cache_get("item2");
  eng_container_t *c3 = eng_dc_cache_get("item3");
  TEST_ASSERT_EQUAL_INT(3, g_create_count);
  TEST_ASSERT_EQUAL_INT(0, g_close_count);

  // Request a 4th item. Since all cached items have ref_count > 0,
  // eviction should fail, and the cache should grow.
  eng_container_t *c4 = eng_dc_cache_get("item4");
  TEST_ASSERT_NOT_NULL(c4);

  TEST_ASSERT_EQUAL_INT(4, g_create_count); // A 4th item was created.
  TEST_ASSERT_EQUAL_INT(0, g_close_count);  // Nothing was evicted.

  eng_dc_cache_release_container(c1);
  eng_dc_cache_release_container(c2);
  eng_dc_cache_release_container(c3);
  eng_dc_cache_release_container(c4);
}

// Test edge case of passing NULL to get/release functions.
void test_null_arguments_are_handled_gracefully(void) {
  eng_container_t *c = eng_dc_cache_get(NULL);
  TEST_ASSERT_NULL(c);
  TEST_ASSERT_EQUAL_INT(0, g_create_count);

  eng_dc_cache_release_container(NULL);
  // No crash is the test.
}

// --- Test Runner ---

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_init_and_destroy);
  RUN_TEST(test_get_single_item_cache_miss);
  RUN_TEST(test_get_same_item_twice_cache_hit);
  RUN_TEST(test_release_does_not_evict);
  RUN_TEST(test_lru_eviction_occurs_on_capacity);
  RUN_TEST(test_accessing_item_updates_lru_order);
  RUN_TEST(test_eviction_skips_item_in_use);
  RUN_TEST(test_cache_grows_when_all_items_are_in_use);
  RUN_TEST(test_null_arguments_are_handled_gracefully);
  return UNITY_END();
}