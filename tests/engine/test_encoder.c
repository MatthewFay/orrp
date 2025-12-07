#include "engine/worker/encoder.h"
#include "mpack.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Helper function to create a cmd_ctx_t for testing
static cmd_ctx_t *create_test_ctx(const char *in_value,
                                  const char *entity_value) {
  cmd_ctx_t *ctx = calloc(1, sizeof(cmd_ctx_t));

  ctx->in_tag_value = ast_create_string_literal_node(in_value);
  ctx->entity_tag_value = ast_create_string_literal_node(entity_value);
  ctx->custom_tags_head = NULL;
  ctx->num_custom_tags = 0;

  return ctx;
}

// Helper to free test context
static void free_test_ctx(cmd_ctx_t *ctx) {
  if (!ctx)
    return;

  ast_free(ctx->in_tag_value);
  ast_free(ctx->entity_tag_value);

  ast_node_t *node = ctx->custom_tags_head;
  while (node) {
    ast_node_t *next = node->next;
    ast_free(node);
    node = next;
  }

  free(ctx);
}

// Helper to add custom tag to context
static void add_custom_tag(cmd_ctx_t *ctx, const char *key, const char *value) {
  ast_node_t *tag =
      ast_create_custom_tag_node(key, ast_create_string_literal_node(value));

  if (ctx->custom_tags_head == NULL) {
    ctx->custom_tags_head = tag;
  } else {
    ast_node_t *current = ctx->custom_tags_head;
    while (current->next) {
      current = current->next;
    }
    current->next = tag;
  }
  ctx->num_custom_tags++;
}

// Helper to decode and verify MessagePack output
static void verify_msgpack_string(const char *data, size_t size,
                                  const char *key, const char *expected_value) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  mpack_node_t root = mpack_tree_root(&tree);

  TEST_ASSERT_EQUAL(mpack_type_map, mpack_node_type(root));

  mpack_node_t value = mpack_node_map_cstr(root, key);
  TEST_ASSERT_FALSE(mpack_node_is_missing(value));
  TEST_ASSERT_EQUAL(mpack_type_str, mpack_node_type(value));

  size_t len = mpack_node_strlen(value);
  char *actual = malloc(len + 1);
  mpack_node_copy_cstr(value, actual, len + 1);

  TEST_ASSERT_EQUAL_STRING(expected_value, actual);

  free(actual);
  mpack_tree_destroy(&tree);
}

static void verify_msgpack_uint(const char *data, size_t size, const char *key,
                                uint32_t expected_value) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  mpack_node_t root = mpack_tree_root(&tree);

  TEST_ASSERT_EQUAL(mpack_type_map, mpack_node_type(root));

  mpack_node_t value = mpack_node_map_cstr(root, key);
  TEST_ASSERT_FALSE(mpack_node_is_missing(value));
  TEST_ASSERT_EQUAL(mpack_type_uint, mpack_node_type(value));

  uint32_t actual = mpack_node_u32(value);
  TEST_ASSERT_EQUAL_UINT32(expected_value, actual);

  mpack_tree_destroy(&tree);
}

static size_t get_msgpack_map_size(const char *data, size_t size) {
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  mpack_node_t root = mpack_tree_root(&tree);

  TEST_ASSERT_EQUAL(mpack_type_map, mpack_node_type(root));

  size_t map_size = mpack_node_map_count(root);

  mpack_tree_destroy(&tree);
  return map_size;
}

void setUp(void) {
  // This is run before each test
}

void tearDown(void) {
  // This is run after each test
}

// Test: Basic encoding with required fields only
void test_encode_event_basic_fields(void) {
  cmd_ctx_t *ctx = create_test_ctx("container1", "entity1");
  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 12345, &data, &size);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_NOT_NULL(data);
  TEST_ASSERT_GREATER_THAN(0, size);

  // Verify the MessagePack contains correct data
  TEST_ASSERT_EQUAL(3, get_msgpack_map_size(data, size));
  verify_msgpack_uint(data, size, "id", 12345);
  verify_msgpack_string(data, size, "in", "container1");
  verify_msgpack_string(data, size, "entity", "entity1");

  free(data);
  free_test_ctx(ctx);
}

// Test: Encoding with one custom tag
void test_encode_event_with_single_custom_tag(void) {
  cmd_ctx_t *ctx = create_test_ctx("inbox", "user123");
  add_custom_tag(ctx, "priority", "high");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 999, &data, &size);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_NOT_NULL(data);

  // Should have 4 keys: id, in, entity, priority
  TEST_ASSERT_EQUAL(4, get_msgpack_map_size(data, size));
  verify_msgpack_uint(data, size, "id", 999);
  verify_msgpack_string(data, size, "in", "inbox");
  verify_msgpack_string(data, size, "entity", "user123");
  verify_msgpack_string(data, size, "priority", "high");

  free(data);
  free_test_ctx(ctx);
}

// Test: Encoding with multiple custom tags
void test_encode_event_with_multiple_custom_tags(void) {
  cmd_ctx_t *ctx = create_test_ctx("events", "order456");
  add_custom_tag(ctx, "status", "pending");
  add_custom_tag(ctx, "category", "payment");
  add_custom_tag(ctx, "source", "api");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 7777, &data, &size);

  TEST_ASSERT_TRUE(result);
  TEST_ASSERT_NOT_NULL(data);

  // Should have 6 keys: id, in, entity, status, category, source
  TEST_ASSERT_EQUAL(6, get_msgpack_map_size(data, size));
  verify_msgpack_uint(data, size, "id", 7777);
  verify_msgpack_string(data, size, "in", "events");
  verify_msgpack_string(data, size, "entity", "order456");
  verify_msgpack_string(data, size, "status", "pending");
  verify_msgpack_string(data, size, "category", "payment");
  verify_msgpack_string(data, size, "source", "api");

  free(data);
  free_test_ctx(ctx);
}

// Test: Event ID of zero
void test_encode_event_with_zero_id(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");
  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 0, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_uint(data, size, "id", 0);

  free(data);
  free_test_ctx(ctx);
}

// Test: Event ID with maximum uint32 value
void test_encode_event_with_max_id(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");
  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, UINT32_MAX, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_uint(data, size, "id", UINT32_MAX);

  free(data);
  free_test_ctx(ctx);
}

// Test: Empty string values
void test_encode_event_with_empty_strings(void) {
  cmd_ctx_t *ctx = create_test_ctx("", "");
  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 100, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "in", "");
  verify_msgpack_string(data, size, "entity", "");

  free(data);
  free_test_ctx(ctx);
}

// Test: String values with special characters
void test_encode_event_with_special_characters(void) {
  cmd_ctx_t *ctx = create_test_ctx("test@container#1", "user!$%^&*()");
  add_custom_tag(ctx, "tag-with-dash", "value/with/slashes");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 555, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "in", "test@container#1");
  verify_msgpack_string(data, size, "entity", "user!$%^&*()");
  verify_msgpack_string(data, size, "tag-with-dash", "value/with/slashes");

  free(data);
  free_test_ctx(ctx);
}

// Test: Unicode/UTF-8 strings
void test_encode_event_with_unicode(void) {
  cmd_ctx_t *ctx = create_test_ctx("测试", "用户");
  add_custom_tag(ctx, "emoji", "🎉🎊");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 888, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "in", "测试");
  verify_msgpack_string(data, size, "entity", "用户");
  verify_msgpack_string(data, size, "emoji", "🎉🎊");

  free(data);
  free_test_ctx(ctx);
}

// Test: Long string values
void test_encode_event_with_long_strings(void) {
  char long_string[1024];
  memset(long_string, 'A', 1023);
  long_string[1023] = '\0';

  cmd_ctx_t *ctx = create_test_ctx(long_string, long_string);

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 321, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "in", long_string);
  verify_msgpack_string(data, size, "entity", long_string);

  free(data);
  free_test_ctx(ctx);
}

// Test: Custom tag with empty key (edge case)
void test_encode_event_with_empty_custom_key(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");
  add_custom_tag(ctx, "", "value");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 111, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "", "value");

  free(data);
  free_test_ctx(ctx);
}

// Test: Custom tag with empty value
void test_encode_event_with_empty_custom_value(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");
  add_custom_tag(ctx, "key", "");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 222, &data, &size);

  TEST_ASSERT_TRUE(result);
  verify_msgpack_string(data, size, "key", "");

  free(data);
  free_test_ctx(ctx);
}

// Test: Verify complete MessagePack structure
void test_encode_event_verify_complete_structure(void) {
  cmd_ctx_t *ctx = create_test_ctx("container", "entity");
  add_custom_tag(ctx, "custom1", "value1");
  add_custom_tag(ctx, "custom2", "value2");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 42, &data, &size);

  TEST_ASSERT_TRUE(result);

  // Parse and verify structure
  mpack_tree_t tree;
  mpack_tree_init_data(&tree, data, size);
  mpack_tree_parse(&tree);
  mpack_node_t root = mpack_tree_root(&tree);

  TEST_ASSERT_EQUAL(mpack_type_map, mpack_node_type(root));
  TEST_ASSERT_EQUAL(5, mpack_node_map_count(root));

  // Verify all keys exist
  TEST_ASSERT_FALSE(mpack_node_is_missing(mpack_node_map_cstr(root, "id")));
  TEST_ASSERT_FALSE(mpack_node_is_missing(mpack_node_map_cstr(root, "in")));
  TEST_ASSERT_FALSE(mpack_node_is_missing(mpack_node_map_cstr(root, "entity")));
  TEST_ASSERT_FALSE(
      mpack_node_is_missing(mpack_node_map_cstr(root, "custom1")));
  TEST_ASSERT_FALSE(
      mpack_node_is_missing(mpack_node_map_cstr(root, "custom2")));

  mpack_tree_destroy(&tree);
  free(data);
  free_test_ctx(ctx);
}

// Test: Multiple calls to encode_event should work independently
void test_encode_event_multiple_independent_calls(void) {
  cmd_ctx_t *ctx1 = create_test_ctx("container1", "entity1");
  cmd_ctx_t *ctx2 = create_test_ctx("container2", "entity2");

  char *data1 = NULL, *data2 = NULL;
  size_t size1 = 0, size2 = 0;

  bool result1 = encode_event(ctx1, 100, &data1, &size1);
  bool result2 = encode_event(ctx2, 200, &data2, &size2);

  TEST_ASSERT_TRUE(result1);
  TEST_ASSERT_TRUE(result2);

  verify_msgpack_uint(data1, size1, "id", 100);
  verify_msgpack_string(data1, size1, "in", "container1");

  verify_msgpack_uint(data2, size2, "id", 200);
  verify_msgpack_string(data2, size2, "in", "container2");

  free(data1);
  free(data2);
  free_test_ctx(ctx1);
  free_test_ctx(ctx2);
}

// Test: Encoding preserves custom tag order
void test_encode_event_custom_tag_order(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");
  add_custom_tag(ctx, "first", "1");
  add_custom_tag(ctx, "second", "2");
  add_custom_tag(ctx, "third", "3");

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 1, &data, &size);

  TEST_ASSERT_TRUE(result);

  // All tags should be present (MessagePack maps are unordered,
  // but we can verify all exist)
  verify_msgpack_string(data, size, "first", "1");
  verify_msgpack_string(data, size, "second", "2");
  verify_msgpack_string(data, size, "third", "3");

  free(data);
  free_test_ctx(ctx);
}

// Test: Large number of custom tags
void test_encode_event_many_custom_tags(void) {
  cmd_ctx_t *ctx = create_test_ctx("test", "test");

  // Add 50 custom tags
  char key[32], value[32];
  for (int i = 0; i < 50; i++) {
    snprintf(key, sizeof(key), "key%d", i);
    snprintf(value, sizeof(value), "value%d", i);
    add_custom_tag(ctx, key, value);
  }

  char *data = NULL;
  size_t size = 0;

  bool result = encode_event(ctx, 9999, &data, &size);

  TEST_ASSERT_TRUE(result);

  // Should have 53 keys: id, in, entity, + 50 custom
  TEST_ASSERT_EQUAL(53, get_msgpack_map_size(data, size));

  // Spot check a few
  verify_msgpack_string(data, size, "key0", "value0");
  verify_msgpack_string(data, size, "key25", "value25");
  verify_msgpack_string(data, size, "key49", "value49");

  free(data);
  free_test_ctx(ctx);
}

// Run all tests
int main(void) {
  UNITY_BEGIN();

  RUN_TEST(test_encode_event_basic_fields);
  RUN_TEST(test_encode_event_with_single_custom_tag);
  RUN_TEST(test_encode_event_with_multiple_custom_tags);
  RUN_TEST(test_encode_event_with_zero_id);
  RUN_TEST(test_encode_event_with_max_id);
  RUN_TEST(test_encode_event_with_empty_strings);
  RUN_TEST(test_encode_event_with_special_characters);
  RUN_TEST(test_encode_event_with_unicode);
  RUN_TEST(test_encode_event_with_long_strings);
  RUN_TEST(test_encode_event_with_empty_custom_key);
  RUN_TEST(test_encode_event_with_empty_custom_value);
  RUN_TEST(test_encode_event_verify_complete_structure);
  RUN_TEST(test_encode_event_multiple_independent_calls);
  RUN_TEST(test_encode_event_custom_tag_order);
  RUN_TEST(test_encode_event_many_custom_tags);

  return UNITY_END();
}