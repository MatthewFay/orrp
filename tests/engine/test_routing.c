#include "engine/routing/routing.h"
#include "unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Note: We are linking against the real "core/hash.h" / xxhash64 implementation
// as requested, so we cannot predict exact indices without knowing the hash
// algorithm's specific output for a string. Instead, we test properties and
// constraints.

void setUp(void) {}

void tearDown(void) {}

// --- Test Group: Route to Queue ---

void test_route_to_queue_should_be_deterministic(void) {
  int total_queues = 16;
  const char *key = "entity_user_123";

  int result1 = route_key_to_queue(key, total_queues);
  int result2 = route_key_to_queue(key, total_queues);

  TEST_ASSERT_EQUAL_INT(result1, result2);
}

void test_route_to_queue_should_stay_within_bounds(void) {
  int total_queues = 8;
  const char *keys[] = {"a", "b", "c", "long_key_name", "12345"};

  for (int i = 0; i < 5; i++) {
    int idx = route_key_to_queue(keys[i], total_queues);
    TEST_ASSERT_GREATER_OR_EQUAL_INT(0, idx);
    TEST_ASSERT_LESS_THAN_INT(total_queues, idx);
  }
}

void test_route_to_queue_should_distribute_different_keys(void) {
  // This is a sanity check. With enough keys, we shouldn't map everything to
  // bucket 0.
  int total_queues = 16;
  int hit_counts[16] = {0};
  char buffer[32];

  for (int i = 0; i < 100; i++) {
    snprintf(buffer, sizeof(buffer), "key_%d", i);
    int idx = route_key_to_queue(buffer, total_queues);
    hit_counts[idx]++;
  }

  // Assert that we hit at least a few different queues
  int buckets_used = 0;
  for (int i = 0; i < total_queues; i++) {
    if (hit_counts[i] > 0)
      buckets_used++;
  }

  TEST_ASSERT_GREATER_THAN_INT(1, buckets_used);
}

// --- Test Group: Route to Consumer ---

void test_route_to_consumer_should_match_queue_topology(void) {
  // Topology: 16 Queues Total. 4 Queues per Consumer.
  // Therefore: 4 Consumers (Indices 0-3).
  // Mapping should be:
  // Queues 0,1,2,3 -> Consumer 0
  // Queues 4,5,6,7 -> Consumer 1
  // ...
  int total_queues = 16;
  int queues_per_consumer = 4;
  const char *key = "some_random_db_key_abc";

  // 1. Get the raw queue index
  int queue_idx = route_key_to_queue(key, total_queues);

  // 2. Get the calculated consumer index
  int consumer_idx =
      route_key_to_consumer(key, total_queues, queues_per_consumer);

  // 3. Verify the mathematical relationship holds
  int expected_consumer = queue_idx / queues_per_consumer;

  TEST_ASSERT_EQUAL_INT(expected_consumer, consumer_idx);
}

void test_route_to_consumer_single_consumer_topology(void) {
  // Topology: 8 queues, all handled by 1 consumer (Consumer 0)
  int total_queues = 8;
  int queues_per_consumer = 8;

  const char *key1 = "key_a";
  const char *key2 = "key_b";

  TEST_ASSERT_EQUAL_INT(
      0, route_key_to_consumer(key1, total_queues, queues_per_consumer));
  TEST_ASSERT_EQUAL_INT(
      0, route_key_to_consumer(key2, total_queues, queues_per_consumer));
}

void test_route_to_consumer_one_queue_per_consumer(void) {
  // Topology: 8 queues, 1 queue per consumer => 8 Consumers
  int total_queues = 8;
  int queues_per_consumer = 1;

  const char *key = "my_key";

  int queue_idx = route_key_to_queue(key, total_queues);
  int consumer_idx =
      route_key_to_consumer(key, total_queues, queues_per_consumer);

  // If every consumer has 1 queue, consumer ID must match Queue ID
  TEST_ASSERT_EQUAL_INT(queue_idx, consumer_idx);
}

int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_route_to_queue_should_be_deterministic);
  RUN_TEST(test_route_to_queue_should_stay_within_bounds);
  RUN_TEST(test_route_to_queue_should_distribute_different_keys);

  RUN_TEST(test_route_to_consumer_should_match_queue_topology);
  RUN_TEST(test_route_to_consumer_single_consumer_topology);
  RUN_TEST(test_route_to_consumer_one_queue_per_consumer);

  return UNITY_END();
}