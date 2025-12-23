#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/eng_eval/eng_eval.h"
#include "query/ast.h"
#include "unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Constants ---
#define TEST_CONTAINER_NAME "test_container"

// --- Global State for Tests ---
static MDB_txn *sys_txn = (MDB_txn *)0xDEADBEEF;
static MDB_txn *user_txn = (MDB_txn *)0xCAFEBABE;

static eng_container_t mock_container;
static consumer_t mock_consumers[1];
static consumer_cache_t mock_consumer_cache;
static eval_state_t state;
static eval_config_t config;
static eval_ctx_t ctx;

// Helper to allow tests to inject "Cached" bitmaps
static bitmap_t *injected_cache_bm = NULL;

// --- In-Memory Mock Database ---
typedef struct mock_db_entry_s {
  char *key;
  void *data;
  size_t len;
  struct mock_db_entry_s *next;
} mock_db_entry_t;

static mock_db_entry_t *mock_db_head = NULL;

// Helper to clear the mock DB
void clear_mock_db() {
  mock_db_entry_t *current = mock_db_head;
  while (current) {
    mock_db_entry_t *next = current->next;
    free(current->key);
    free(current->data);
    free(current);
    current = next;
  }
  mock_db_head = NULL;
}

// Helper to add data to mock DB
void add_to_mock_db(const char *key, void *data, size_t len) {
  mock_db_entry_t *entry = calloc(1, sizeof(mock_db_entry_t));
  entry->key = strdup(key);
  entry->data = malloc(len);
  memcpy(entry->data, data, len);
  entry->len = len;
  entry->next = mock_db_head;
  mock_db_head = entry;
}

// --- Mocks / Stubs for External Dependencies ---

// Mock Routing: Always route to consumer 0
int route_key_to_consumer(const char *key, uint32_t total,
                          uint32_t per_consumer) {
  (void)key;
  (void)total;
  (void)per_consumer;
  return 0;
}

// Mock Consumer Access
consumer_cache_t *consumer_get_cache(consumer_t *consumer) {
  (void)consumer;
  return &mock_consumer_cache;
}

// Mock Consumer Cache Lookup: Returns injected global if present
const bitmap_t *consumer_cache_get_bm(consumer_cache_t *cache,
                                      const char *key) {
  (void)cache;
  if (injected_cache_bm && strstr(key, "cached_tag")) {
    return injected_cache_bm;
  }
  return NULL;
}

// Mock Container DB Handle Retrieval
bool container_get_db_handle(eng_container_t *container,
                             eng_container_db_key_t *key, MDB_dbi *dbi) {
  (void)container;
  (void)key;
  *dbi = 1; // Dummy handle
  return true;
}

// --- Mocking DB Layer (core/db.h) ---

// Mock the db_get function to read from our linked list
bool db_get(MDB_dbi dbi, MDB_txn *txn, db_key_t *key, db_get_result_t *result) {
  (void)dbi;
  (void)txn;
  if (key->type != DB_KEY_STRING)
    return false;

  const char *lookup_key = (const char *)key->key.s;
  mock_db_entry_t *curr = mock_db_head;

  while (curr) {
    if (strcmp(curr->key, lookup_key) == 0) {
      result->status = DB_GET_OK;
      result->value_len = curr->len;
      result->value = malloc(curr->len);
      memcpy(result->value, curr->data, curr->len);
      return true;
    }
    curr = curr->next;
  }

  result->status = DB_GET_NOT_FOUND;
  return true;
}

// Mock result cleanup
void db_get_result_clear(db_get_result_t *r) {
  if (r->value) {
    free(r->value);
    r->value = NULL;
  }
}

// --- Helper Functions for Test Setup ---

void setup_db_bitmap(const char *key, bitmap_t *bm) {
  size_t len;
  void *data = bitmap_serialize(bm, &len);
  add_to_mock_db(key, data, len);
  free(data);
}

void setup_db_max_id(uint32_t max_id) {
  add_to_mock_db(USR_NEXT_EVENT_ID_KEY, &max_id, sizeof(uint32_t));
}

// --- AST Helpers (Wrapper around real AST calls) ---

// Helper to make test code less verbose while using real AST API
ast_node_t *make_test_tag(const char *k, const char *v) {
  return ast_create_custom_tag_node(k, ast_create_string_literal_node(v, 1));
}

// --- Setup / Teardown ---

void setUp(void) {
  clear_mock_db();

  // Setup Context
  memset(&mock_container, 0, sizeof(eng_container_t));
  mock_container.name = TEST_CONTAINER_NAME;

  memset(&config, 0, sizeof(eval_config_t));
  config.container = &mock_container;
  config.user_txn = user_txn;
  config.sys_txn = sys_txn;
  config.consumers = mock_consumers;
  config.op_queue_total_count = 1;
  config.op_queues_per_consumer = 1;

  memset(&state, 0, sizeof(eval_state_t));

  ctx.config = &config;
  ctx.state = &state;

  injected_cache_bm = NULL;
}

void tearDown(void) {
  clear_mock_db();

  if (injected_cache_bm)
    bitmap_free(injected_cache_bm);

  // Cleanup Eval State
  eng_eval_cleanup_state(&state);
}

// --- Tests ---

void test_resolve_single_tag_from_db(void) {
  // 1. Seed Mock DB
  bitmap_t *bm = bitmap_create();
  bitmap_add(bm, 1);
  bitmap_add(bm, 100);
  setup_db_bitmap("loc:ca", bm);
  bitmap_free(bm);

  // 2. Execute
  ast_node_t *ast = make_test_tag("loc", "ca");
  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  // 3. Assert
  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_NOT_NULL(r.events);
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 1));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 100));
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 50));

  // Cleanup
  bitmap_free(r.events);
  ast_free(ast);
}

void test_resolve_single_tag_miss(void) {
  // No DB seed
  ast_node_t *ast = make_test_tag("loc", "mars");
  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  TEST_ASSERT_TRUE(r.success); // Success, just empty result
  TEST_ASSERT_NOT_NULL(r.events);
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1));

  bitmap_free(r.events);
  ast_free(ast);
}

void test_resolve_from_consumer_cache(void) {
  // 1. Inject Cache
  injected_cache_bm = bitmap_create();
  bitmap_add(injected_cache_bm, 999);

  // 2. Execute
  ast_node_t *ast = make_test_tag("type", "cached_tag");
  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  // 3. Assert
  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 999));

  bitmap_free(r.events);
  ast_free(ast);
}

void test_logical_and(void) {
  // loc:ca (1, 2, 3)
  bitmap_t *b1 = bitmap_create();
  bitmap_add(b1, 1);
  bitmap_add(b1, 2);
  bitmap_add(b1, 3);
  setup_db_bitmap("loc:ca", b1);
  bitmap_free(b1);

  // type:view (2, 3, 4)
  bitmap_t *b2 = bitmap_create();
  bitmap_add(b2, 2);
  bitmap_add(b2, 3);
  bitmap_add(b2, 4);
  setup_db_bitmap("type:view", b2);
  bitmap_free(b2);

  // loc:ca AND type:view -> (2, 3)
  ast_node_t *ast =
      ast_create_logical_node(AST_LOGIC_NODE_AND, make_test_tag("loc", "ca"),
                              make_test_tag("type", "view"));

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 2));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 3));
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 4));

  bitmap_free(r.events);
  ast_free(ast);
}

void test_logical_or(void) {
  // loc:ca (1)
  bitmap_t *b1 = bitmap_create();
  bitmap_add(b1, 1);
  setup_db_bitmap("loc:ca", b1);
  bitmap_free(b1);

  // loc:ny (5)
  bitmap_t *b2 = bitmap_create();
  bitmap_add(b2, 5);
  setup_db_bitmap("loc:ny", b2);
  bitmap_free(b2);

  // loc:ca OR loc:ny -> (1, 5)
  ast_node_t *ast =
      ast_create_logical_node(AST_LOGIC_NODE_OR, make_test_tag("loc", "ca"),
                              make_test_tag("loc", "ny"));

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 1));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 5));

  bitmap_free(r.events);
  ast_free(ast);
}

void test_logical_not(void) {
  // Universe Max ID = 10
  setup_db_max_id(10);

  // loc:ca (1, 2)
  bitmap_t *b1 = bitmap_create();
  bitmap_add(b1, 1);
  bitmap_add(b1, 2);
  setup_db_bitmap("loc:ca", b1);
  bitmap_free(b1);

  // NOT loc:ca -> (0, 3, 4, 5, 6, 7, 8, 9)
  ast_node_t *ast = ast_create_not_node(make_test_tag("loc", "ca"));

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(ast, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1));
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 2));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 0));
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 5));
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 10));

  bitmap_free(r.events);
  ast_free(ast);
}

void test_complex_nested_logic(void) {
  setup_db_max_id(4);

  bitmap_t *b;
  b = bitmap_create();
  bitmap_add(b, 1);
  setup_db_bitmap("tag:A", b);
  bitmap_free(b);
  b = bitmap_create();
  bitmap_add(b, 2);
  setup_db_bitmap("tag:B", b);
  bitmap_free(b);
  b = bitmap_create();
  bitmap_add(b, 1);
  setup_db_bitmap("tag:C", b);
  bitmap_free(b);

  // (A OR B) AND (NOT C)
  ast_node_t *part1 = ast_create_logical_node(
      AST_LOGIC_NODE_OR, make_test_tag("tag", "A"), make_test_tag("tag", "B"));
  ast_node_t *part2 = ast_create_not_node(make_test_tag("tag", "C"));
  ast_node_t *root = ast_create_logical_node(AST_LOGIC_NODE_AND, part1, part2);

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(root, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1)); // Filtered by NOT C
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 2));  // Matches B and NOT C
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 0)); // Not in A or B

  bitmap_free(r.events);
  ast_free(root);
}

void test_stack_overflow_protection(void) {
  ast_node_t *root = make_test_tag("tag", "0");
  for (int i = 0; i < 150; i++) {
    char buf[10];
    sprintf(buf, "%d", i + 1);
    root = ast_create_logical_node(AST_LOGIC_NODE_AND, root,
                                   make_test_tag("tag", buf));
  }

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(root, &ctx);

  TEST_ASSERT_FALSE(r.success);
  ast_free(root);
}

void test_deeply_nested_mixed_logic(void) {
  // Query: ((A AND B) OR (C AND D)) AND (NOT E)
  // Universe Max ID = 10
  setup_db_max_id(10);

  // Sets
  // A: {1, 2}
  // B: {2, 3} -> A AND B = {2}
  // C: {4, 5}
  // D: {5, 6} -> C AND D = {5}
  // Union -> {2, 5}
  // E: {2}
  // NOT E -> Everything except 2
  // Final -> {5}

  bitmap_t *bm;

  bm = bitmap_create();
  bitmap_add(bm, 1);
  bitmap_add(bm, 2);
  setup_db_bitmap("tag:A", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 2);
  bitmap_add(bm, 3);
  setup_db_bitmap("tag:B", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 4);
  bitmap_add(bm, 5);
  setup_db_bitmap("tag:C", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 5);
  bitmap_add(bm, 6);
  setup_db_bitmap("tag:D", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 2);
  setup_db_bitmap("tag:E", bm);
  bitmap_free(bm);

  // Construct AST
  ast_node_t *and1 = ast_create_logical_node(
      AST_LOGIC_NODE_AND, make_test_tag("tag", "A"), make_test_tag("tag", "B"));
  ast_node_t *and2 = ast_create_logical_node(
      AST_LOGIC_NODE_AND, make_test_tag("tag", "C"), make_test_tag("tag", "D"));
  ast_node_t *or_node = ast_create_logical_node(AST_LOGIC_NODE_OR, and1, and2);
  ast_node_t *not_node = ast_create_not_node(make_test_tag("tag", "E"));
  ast_node_t *root =
      ast_create_logical_node(AST_LOGIC_NODE_AND, or_node, not_node);

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(root, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 5));
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 2)); // Filtered by NOT E
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1)); // Not in Intersection

  bitmap_free(r.events);
  ast_free(root);
}

void test_nested_not_logic(void) {
  // Query: NOT (A OR (B AND (NOT C)))
  // Max ID = 5
  // A: {0}
  // B: {1, 2}
  // C: {2} -> NOT C -> {0, 1, 3, 4} (Assuming universe 5)
  // B AND (NOT C) -> {1, 2} AND {0, 1, 3, 4} -> {1}
  // A OR {1} -> {0, 1}
  // NOT {0, 1} -> {2, 3, 4} (Range [0, 5))

  setup_db_max_id(5);

  bitmap_t *bm;
  bm = bitmap_create();
  bitmap_add(bm, 0);
  setup_db_bitmap("tag:A", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 1);
  bitmap_add(bm, 2);
  setup_db_bitmap("tag:B", bm);
  bitmap_free(bm);
  bm = bitmap_create();
  bitmap_add(bm, 2);
  setup_db_bitmap("tag:C", bm);
  bitmap_free(bm);

  ast_node_t *not_c = ast_create_not_node(make_test_tag("tag", "C"));
  ast_node_t *b_and_not_c = ast_create_logical_node(
      AST_LOGIC_NODE_AND, make_test_tag("tag", "B"), not_c);
  ast_node_t *a_or_inner = ast_create_logical_node(
      AST_LOGIC_NODE_OR, make_test_tag("tag", "A"), b_and_not_c);
  ast_node_t *root = ast_create_not_node(a_or_inner);

  eng_eval_result_t r = eng_eval_resolve_exp_to_events(root, &ctx);

  TEST_ASSERT_TRUE(r.success);
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 0)); // In A
  TEST_ASSERT_FALSE(bitmap_contains(r.events, 1)); // In B AND NOT C
  TEST_ASSERT_TRUE(
      bitmap_contains(r.events, 2)); // In C, so excluded from inner AND, so not
                                     // in OR, so present in final NOT
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 3)); // Not in A or B
  TEST_ASSERT_TRUE(bitmap_contains(r.events, 4)); // Not in A or B

  bitmap_free(r.events);
  ast_free(root);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_resolve_single_tag_from_db);
  RUN_TEST(test_resolve_single_tag_miss);
  RUN_TEST(test_resolve_from_consumer_cache);
  RUN_TEST(test_logical_and);
  RUN_TEST(test_logical_or);
  RUN_TEST(test_logical_not);
  RUN_TEST(test_complex_nested_logic);
  RUN_TEST(test_stack_overflow_protection);
  RUN_TEST(test_deeply_nested_mixed_logic);
  RUN_TEST(test_nested_not_logic);
  return UNITY_END();
}