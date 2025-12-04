#include "engine/consumer/consumer_batch.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include "unity.h"
#include "uthash.h"
#include <stdlib.h>
#include <string.h>

// Global root for the batch table
static consumer_batch_container_t *batch_table = NULL;

// List of messages created during test to clean up in tearDown
typedef struct msg_tracker_s {
  op_queue_msg_t *msg;
  struct msg_tracker_s *next;
} msg_tracker_t;

static msg_tracker_t *tracker_head = NULL;

void setUp(void) {
  batch_table = NULL;
  tracker_head = NULL;
}

// Helper to track and free the dummy messages we create
void _track_msg(op_queue_msg_t *msg) {
  msg_tracker_t *t = malloc(sizeof(msg_tracker_t));
  t->msg = msg;
  t->next = tracker_head;
  tracker_head = t;
}

void tearDown(void) {
  // 1. Free the batch table (The System Under Test)
  consumer_batch_free_table(batch_table);

  // 2. Free the dummy message payloads we created
  while (tracker_head) {
    msg_tracker_t *next = tracker_head->next;

    if (tracker_head->msg) {
      if (tracker_head->msg->op) {
        // FIX: If container_name is a pointer, we must free it.
        // Note: If your struct defines it as char[32], remove this free.
        // But since you were crashing on strncpy, it is likely a pointer.
        if (tracker_head->msg->op->db_key.container_name) {
          free(tracker_head->msg->op->db_key.container_name);
        }
        free(tracker_head->msg->op);
      }
      free(tracker_head->msg);
    }

    free(tracker_head);
    tracker_head = next;
  }
}

// Helper to create a dummy message structure
op_queue_msg_t *create_msg(char *container, char *ser_key) {
  op_queue_msg_t *msg = calloc(1, sizeof(op_queue_msg_t));
  msg->op = calloc(1, sizeof(op_t));

  // FIX: Use strdup instead of strncpy
  // This handles the case where container_name is a char* initialized to NULL.
  msg->op->db_key.container_name = strdup(container);

  msg->op->db_key.dc_type = CONTAINER_TYPE_USR;
  msg->ser_db_key = ser_key;

  _track_msg(msg);
  return msg;
}

// ============================================================================
// Test Group: Input Validation
// ============================================================================

void test_add_msg_null_inputs(void) {
  op_queue_msg_t *msg = create_msg("c1", "k1");

  // NULL Table Pointer
  TEST_ASSERT_FALSE(consumer_batch_add_msg(NULL, msg));

  // NULL Message
  TEST_ASSERT_FALSE(consumer_batch_add_msg(&batch_table, NULL));

  // Verify table remains NULL
  TEST_ASSERT_NULL(batch_table);
}

// ============================================================================
// Test Group: Single Insertion
// ============================================================================

void test_add_single_msg_creates_structure(void) {
  char *c_name = "container_A";
  char *db_key = "user:123";
  op_queue_msg_t *msg = create_msg(c_name, db_key);

  bool res = consumer_batch_add_msg(&batch_table, msg);
  TEST_ASSERT_TRUE(res);
  TEST_ASSERT_NOT_NULL(batch_table);

  // 1. Verify Container
  consumer_batch_container_t *c_entry = NULL;
  HASH_FIND_STR(batch_table, c_name, c_entry);
  TEST_ASSERT_NOT_NULL(c_entry);
  TEST_ASSERT_EQUAL_STRING(c_name, c_entry->container_name);

  // 2. Verify DB Key Entry
  consumer_batch_db_key_t *k_entry = NULL;
  HASH_FIND_STR(c_entry->db_keys, db_key, k_entry);
  TEST_ASSERT_NOT_NULL(k_entry);
  TEST_ASSERT_EQUAL_STRING(db_key, k_entry->ser_db_key);

  // 3. Verify Linked List
  TEST_ASSERT_NOT_NULL(k_entry->head);
  TEST_ASSERT_EQUAL_PTR(msg, k_entry->head->msg);
  TEST_ASSERT_NULL(k_entry->head->next); // Only one item
  TEST_ASSERT_EQUAL_PTR(k_entry->head, k_entry->tail);
}

// ============================================================================
// Test Group: Aggregation (Batching)
// ============================================================================

void test_add_multiple_msgs_same_key_appends_list(void) {
  char *c_name = "container_A";
  char *db_key = "user:123";

  op_queue_msg_t *msg1 = create_msg(c_name, db_key);
  op_queue_msg_t *msg2 = create_msg(c_name, db_key);
  op_queue_msg_t *msg3 = create_msg(c_name, db_key);

  TEST_ASSERT_TRUE(consumer_batch_add_msg(&batch_table, msg1));
  TEST_ASSERT_TRUE(consumer_batch_add_msg(&batch_table, msg2));
  TEST_ASSERT_TRUE(consumer_batch_add_msg(&batch_table, msg3));

  consumer_batch_container_t *c_entry;
  HASH_FIND_STR(batch_table, c_name, c_entry);

  consumer_batch_db_key_t *k_entry;
  HASH_FIND_STR(c_entry->db_keys, db_key, k_entry);

  // Verify List Topology: msg1 -> msg2 -> msg3 -> NULL
  consumer_batch_msg_node_t *node = k_entry->head;

  TEST_ASSERT_NOT_NULL(node);
  TEST_ASSERT_EQUAL_PTR(msg1, node->msg);

  node = node->next;
  TEST_ASSERT_NOT_NULL(node);
  TEST_ASSERT_EQUAL_PTR(msg2, node->msg);

  node = node->next;
  TEST_ASSERT_NOT_NULL(node);
  TEST_ASSERT_EQUAL_PTR(msg3, node->msg);

  TEST_ASSERT_NULL(node->next);
  TEST_ASSERT_EQUAL_PTR(node, k_entry->tail);
}

void test_add_msgs_different_keys_segregates_entries(void) {
  char *c_name = "container_A";

  op_queue_msg_t *msg_k1 = create_msg(c_name, "key:1");
  op_queue_msg_t *msg_k2 = create_msg(c_name, "key:2");

  consumer_batch_add_msg(&batch_table, msg_k1);
  consumer_batch_add_msg(&batch_table, msg_k2);

  consumer_batch_container_t *c_entry;
  HASH_FIND_STR(batch_table, c_name, c_entry);

  // Verify we have 2 distinct key entries in the hash map
  TEST_ASSERT_EQUAL_INT(2, HASH_COUNT(c_entry->db_keys));

  consumer_batch_db_key_t *k1_entry, *k2_entry;
  HASH_FIND_STR(c_entry->db_keys, "key:1", k1_entry);
  HASH_FIND_STR(c_entry->db_keys, "key:2", k2_entry);

  TEST_ASSERT_NOT_NULL(k1_entry);
  TEST_ASSERT_NOT_NULL(k2_entry);
  TEST_ASSERT_NOT_EQUAL(k1_entry, k2_entry);

  // Verify each list has exactly 1 item
  TEST_ASSERT_EQUAL_PTR(msg_k1, k1_entry->head->msg);
  TEST_ASSERT_EQUAL_PTR(msg_k2, k2_entry->head->msg);
}

void test_add_msgs_different_containers_segregates_batches(void) {
  op_queue_msg_t *msg_c1 = create_msg("container_1", "key:common");
  op_queue_msg_t *msg_c2 = create_msg("container_2", "key:common");

  consumer_batch_add_msg(&batch_table, msg_c1);
  consumer_batch_add_msg(&batch_table, msg_c2);

  // Verify root table has 2 containers
  TEST_ASSERT_EQUAL_INT(2, HASH_COUNT(batch_table));

  consumer_batch_container_t *c1, *c2;
  HASH_FIND_STR(batch_table, "container_1", c1);
  HASH_FIND_STR(batch_table, "container_2", c2);

  TEST_ASSERT_NOT_NULL(c1);
  TEST_ASSERT_NOT_NULL(c2);

  // Verify they are independent (check internal keys)
  consumer_batch_db_key_t *k1, *k2;
  HASH_FIND_STR(c1->db_keys, "key:common", k1);
  HASH_FIND_STR(c2->db_keys, "key:common", k2);

  TEST_ASSERT_NOT_NULL(k1);
  TEST_ASSERT_NOT_NULL(k2);
  TEST_ASSERT_EQUAL_PTR(msg_c1, k1->head->msg);
  TEST_ASSERT_EQUAL_PTR(msg_c2, k2->head->msg);
}

// ============================================================================
// Test Group: Complex Mixed Scenario
// ============================================================================

void test_complex_topology(void) {
  // 2 Containers.
  // Container A has 2 Keys. Key 1 has 2 Msgs. Key 2 has 1 Msg.
  // Container B has 1 Key. Key 1 has 1 Msg.

  op_queue_msg_t *a_k1_1 = create_msg("A", "k1");
  op_queue_msg_t *a_k1_2 = create_msg("A", "k1");
  op_queue_msg_t *a_k2_1 = create_msg("A", "k2");
  op_queue_msg_t *b_k1_1 = create_msg("B", "k1");

  consumer_batch_add_msg(&batch_table, a_k1_1);
  consumer_batch_add_msg(&batch_table, a_k1_2);
  consumer_batch_add_msg(&batch_table, a_k2_1);
  consumer_batch_add_msg(&batch_table, b_k1_1);

  // Assertions
  consumer_batch_container_t *cA, *cB;
  HASH_FIND_STR(batch_table, "A", cA);
  HASH_FIND_STR(batch_table, "B", cB);

  TEST_ASSERT_EQUAL_INT(2, HASH_COUNT(cA->db_keys));
  TEST_ASSERT_EQUAL_INT(1, HASH_COUNT(cB->db_keys));

  consumer_batch_db_key_t *key_node;

  // Check A->k1 (list of 2)
  HASH_FIND_STR(cA->db_keys, "k1", key_node);
  TEST_ASSERT_NOT_NULL(key_node->head->next);
  TEST_ASSERT_NULL(key_node->head->next->next);

  // Check A->k2 (list of 1)
  HASH_FIND_STR(cA->db_keys, "k2", key_node);
  TEST_ASSERT_NULL(key_node->head->next);
}

int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_add_msg_null_inputs);
  RUN_TEST(test_add_single_msg_creates_structure);
  RUN_TEST(test_add_multiple_msgs_same_key_appends_list);
  RUN_TEST(test_add_msgs_different_keys_segregates_entries);
  RUN_TEST(test_add_msgs_different_containers_segregates_batches);
  RUN_TEST(test_complex_topology);

  return UNITY_END();
}