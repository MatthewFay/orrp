#include "query/ast.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

void test_string_literal_node(void) {
  ast_node_t *lit = ast_create_string_literal_node("foo");
  TEST_ASSERT_NOT_NULL(lit);
  TEST_ASSERT_EQUAL(AST_LITERAL_NODE, lit->type);
  TEST_ASSERT_EQUAL(AST_LITERAL_STRING, lit->literal.type);
  TEST_ASSERT_EQUAL_STRING("foo", lit->literal.string_value);
  TEST_ASSERT_NULL(lit->next);
  ast_free(lit);
}

void test_number_literal_node(void) {
  ast_node_t *lit = ast_create_number_literal_node(42);
  TEST_ASSERT_NOT_NULL(lit);
  TEST_ASSERT_EQUAL(AST_LITERAL_NODE, lit->type);
  TEST_ASSERT_EQUAL(AST_LITERAL_NUMBER, lit->literal.type);
  TEST_ASSERT_EQUAL_INT64(42, lit->literal.number_value);
  TEST_ASSERT_NULL(lit->next);
  ast_free(lit);
}

void test_tag_node_reserved(void) {
  ast_node_t *val = ast_create_string_literal_node("events");
  ast_node_t *tag = ast_create_tag_node(AST_KEY_IN, val);

  TEST_ASSERT_NOT_NULL(tag);
  TEST_ASSERT_EQUAL(AST_TAG_NODE, tag->type);
  TEST_ASSERT_EQUAL(AST_TAG_KEY_RESERVED, tag->tag.key_type);
  TEST_ASSERT_EQUAL(AST_KEY_IN, tag->tag.reserved_key);
  TEST_ASSERT_EQUAL(val, tag->tag.value);
  TEST_ASSERT_NULL(tag->next);
  ast_free(tag);
}

void test_tag_node_custom(void) {
  ast_node_t *val = ast_create_string_literal_node("US");
  ast_node_t *tag = ast_create_custom_tag_node("country", val);

  TEST_ASSERT_NOT_NULL(tag);
  TEST_ASSERT_EQUAL(AST_TAG_NODE, tag->type);
  TEST_ASSERT_EQUAL(AST_TAG_KEY_CUSTOM, tag->tag.key_type);
  TEST_ASSERT_EQUAL_STRING("country", tag->tag.custom_key);
  TEST_ASSERT_EQUAL(val, tag->tag.value);
  ast_free(tag);
}

void test_comparison_node(void) {
  ast_node_t *left = ast_create_custom_tag_node("clicks", NULL);
  ast_node_t *right = ast_create_number_literal_node(100);
  ast_node_t *cmp = ast_create_comparison_node(AST_OP_GT, left, right);

  TEST_ASSERT_NOT_NULL(cmp);
  TEST_ASSERT_EQUAL(AST_COMPARISON_NODE, cmp->type);
  TEST_ASSERT_EQUAL(AST_OP_GT, cmp->comparison.op);
  TEST_ASSERT_EQUAL(left, cmp->comparison.left);
  TEST_ASSERT_EQUAL(right, cmp->comparison.right);
  ast_free(cmp);
}

void test_logical_node(void) {
  ast_node_t *left = ast_create_string_literal_node("left");
  ast_node_t *right = ast_create_string_literal_node("right");
  ast_node_t *logical =
      ast_create_logical_node(AST_LOGIC_NODE_AND, left, right);

  TEST_ASSERT_NOT_NULL(logical);
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, logical->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_AND, logical->logical.op);
  TEST_ASSERT_EQUAL(left, logical->logical.left_operand);
  TEST_ASSERT_EQUAL(right, logical->logical.right_operand);
  ast_free(logical); // should free all children
}

void test_not_node(void) {
  ast_node_t *operand = ast_create_string_literal_node("notme");
  ast_node_t *not_node = ast_create_not_node(operand);

  TEST_ASSERT_NOT_NULL(not_node);
  TEST_ASSERT_EQUAL(AST_NOT_NODE, not_node->type);
  TEST_ASSERT_EQUAL(operand, not_node->not_op.operand);
  ast_free(not_node);
}

void test_append_multiple_nodes(void) {
  ast_node_t *list = NULL;
  ast_node_t *item1 = ast_create_string_literal_node("a");
  ast_node_t *item2 = ast_create_string_literal_node("b");
  ast_node_t *item3 = ast_create_string_literal_node("c");

  ast_append_node(&list, item1);
  ast_append_node(&list, item2);
  ast_append_node(&list, item3);

  TEST_ASSERT_NOT_NULL(list);
  ast_node_t *cur = list;
  TEST_ASSERT_EQUAL_STRING("a", cur->literal.string_value);

  cur = cur->next;
  TEST_ASSERT_NOT_NULL(cur);
  TEST_ASSERT_EQUAL_STRING("b", cur->literal.string_value);

  cur = cur->next;
  TEST_ASSERT_NOT_NULL(cur);
  TEST_ASSERT_EQUAL_STRING("c", cur->literal.string_value);
  TEST_ASSERT_NULL(cur->next);

  ast_free(list);
}

void test_command_node(void) {
  // Build a list of tags
  ast_node_t *tags_list = NULL;
  ast_node_t *tag1 =
      ast_create_tag_node(AST_KEY_IN, ast_create_string_literal_node("users"));
  ast_node_t *tag2 = ast_create_custom_tag_node(
      "country", ast_create_string_literal_node("US"));
  ast_append_node(&tags_list, tag1);
  ast_append_node(&tags_list, tag2);

  // Create the command node
  ast_node_t *cmd = ast_create_command_node(AST_CMD_QUERY, tags_list);
  TEST_ASSERT_NOT_NULL(cmd);
  TEST_ASSERT_EQUAL(AST_COMMAND_NODE, cmd->type);
  TEST_ASSERT_EQUAL(AST_CMD_QUERY, cmd->command.type);
  TEST_ASSERT_EQUAL(tags_list, cmd->command.tags);

  // Check the tags list within the command
  ast_node_t *current_tag = cmd->command.tags;
  TEST_ASSERT_EQUAL(tag1, current_tag);
  TEST_ASSERT_EQUAL(AST_TAG_KEY_RESERVED, current_tag->tag.key_type);

  current_tag = current_tag->next;
  TEST_ASSERT_EQUAL(tag2, current_tag);
  TEST_ASSERT_EQUAL_STRING("country", current_tag->tag.custom_key);
  TEST_ASSERT_NULL(current_tag->next);

  ast_free(cmd);
}

void test_free_deep_tree(void) {
  // This test primarily checks that ast_free doesn't crash on a nested
  // structure. Running this with a memory checker (like Valgrind) would confirm
  // no leaks.
  ast_node_t *root = ast_create_logical_node(
      AST_LOGIC_NODE_AND,
      ast_create_not_node(ast_create_logical_node(
          AST_LOGIC_NODE_OR, ast_create_string_literal_node("a"),
          ast_create_string_literal_node("b"))),
      ast_create_string_literal_node("c"));

  TEST_ASSERT_NOT_NULL(root);
  ast_free(root);
}

void test_append_node_to_null_list(void) {
  ast_node_t *list = NULL;
  ast_node_t *item = ast_create_string_literal_node("first");
  ast_append_node(&list, item);

  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL(item, list);
  TEST_ASSERT_EQUAL_STRING("first", list->literal.string_value);
  TEST_ASSERT_NULL(list->next);
  ast_free(list);
}

void test_append_null_node(void) {
  ast_node_t *list = NULL;
  ast_append_node(&list, NULL); // Should not crash
  TEST_ASSERT_NULL(list);

  list = ast_create_number_literal_node(1);
  ast_append_node(&list, NULL); // Should not change the list
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_NULL(list->next);
  ast_free(list);
}

int main(void) {
  UNITY_BEGIN();

  // Literal and Tag node tests
  RUN_TEST(test_string_literal_node);
  RUN_TEST(test_number_literal_node);
  RUN_TEST(test_tag_node_reserved);
  RUN_TEST(test_tag_node_custom);

  // Operator and Expression node tests
  RUN_TEST(test_comparison_node);
  RUN_TEST(test_logical_node);
  RUN_TEST(test_not_node);

  // List and Command structure tests
  RUN_TEST(test_append_multiple_nodes);
  RUN_TEST(test_command_node);

  // Memory and edge case tests
  RUN_TEST(test_free_deep_tree);
  RUN_TEST(test_append_node_to_null_list);
  RUN_TEST(test_append_null_node);

  return UNITY_END();
}