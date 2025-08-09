#include "query/ast.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

void test_identifier_node(void) {
  ast_node_t *id = ast_create_identifier_node("foo");
  TEST_ASSERT_NOT_NULL(id);
  TEST_ASSERT_EQUAL(IDENTIFIER_NODE, id->type);
  TEST_ASSERT_NOT_NULL(id->node.id);
  TEST_ASSERT_EQUAL_STRING("foo", id->node.id->value);
  ast_free(id);
}

void test_logical_node(void) {
  ast_node_t *left = ast_create_identifier_node("left");
  ast_node_t *right = ast_create_identifier_node("right");
  ast_node_t *logical = ast_create_logical_node(AND, left, right);
  TEST_ASSERT_NOT_NULL(logical);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, logical->type);
  TEST_ASSERT_NOT_NULL(logical->node.logical);
  TEST_ASSERT_EQUAL(AND, logical->node.logical->op);
  TEST_ASSERT_EQUAL(left, logical->node.logical->left_operand);
  TEST_ASSERT_EQUAL(right, logical->node.logical->right_operand);
  ast_free(logical); // should free all children
}

void test_not_node(void) {
  ast_node_t *operand = ast_create_identifier_node("notme");
  ast_node_t *not_node = ast_create_not_node(operand);
  TEST_ASSERT_NOT_NULL(not_node);
  TEST_ASSERT_EQUAL(NOT_NODE, not_node->type);
  TEST_ASSERT_NOT_NULL(not_node->node.not_op);
  TEST_ASSERT_EQUAL(operand, not_node->node.not_op->operand);
  ast_free(not_node);
}

void test_list_node_single(void) {
  ast_node_t *item = ast_create_identifier_node("single");
  ast_node_t *list = ast_create_list_node(item, NULL);
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL(LIST_NODE, list->type);
  TEST_ASSERT_NOT_NULL(list->node.list);
  TEST_ASSERT_EQUAL(item, list->node.list->item);
  TEST_ASSERT_NULL(list->node.list->next);
  ast_free(list);
}

void test_list_node_multiple_append(void) {
  ast_node_t *list = NULL;
  ast_node_t *item1 = ast_create_identifier_node("a");
  ast_node_t *item2 = ast_create_identifier_node("b");
  ast_node_t *item3 = ast_create_identifier_node("c");

  ast_list_append(&list, item1);
  ast_list_append(&list, item2);
  ast_list_append(&list, item3);

  TEST_ASSERT_NOT_NULL(list);
  ast_node_t *cur = list;
  TEST_ASSERT_EQUAL_STRING("a", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_EQUAL_STRING("b", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_EQUAL_STRING("c", cur->node.list->item->node.id->value);
  TEST_ASSERT_NULL(cur->node.list->next);

  ast_free(list);
}

void test_command_node(void) {
  ast_node_t *args = NULL;
  ast_list_append(&args, ast_create_identifier_node("arg1"));
  ast_list_append(&args, ast_create_identifier_node("arg2"));
  ast_node_t *exp = ast_create_logical_node(OR, ast_create_identifier_node("x"),
                                            ast_create_identifier_node("y"));
  ast_node_t *cmd = ast_create_command_node(QUERY, args, exp);
  TEST_ASSERT_NOT_NULL(cmd);
  TEST_ASSERT_EQUAL(COMMAND_NODE, cmd->type);
  TEST_ASSERT_NOT_NULL(cmd->node.cmd);
  TEST_ASSERT_EQUAL(QUERY, cmd->node.cmd->cmd_type);
  TEST_ASSERT_NOT_NULL(cmd->node.cmd->args);
  TEST_ASSERT_NOT_NULL(cmd->node.cmd->exp);

  // Check args list
  ast_node_t *cur = cmd->node.cmd->args;
  TEST_ASSERT_EQUAL_STRING("arg1", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_EQUAL_STRING("arg2", cur->node.list->item->node.id->value);

  // Check exp
  ast_node_t *exp_node = cmd->node.cmd->exp;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp_node->type);
  TEST_ASSERT_EQUAL(OR, exp_node->node.logical->op);

  ast_free(cmd);
}

void test_free_deep_tree(void) {
  ast_node_t *root =
      ast_create_logical_node(AND,
                              ast_create_not_node(ast_create_logical_node(
                                  OR, ast_create_identifier_node("a"),
                                  ast_create_identifier_node("b"))),
                              ast_create_identifier_node("c"));
  ast_free(root); // Should not leak
}

void test_list_append_to_null(void) {
  ast_node_t *list = NULL;
  ast_node_t *item = ast_create_identifier_node("first");
  ast_list_append(&list, item);
  TEST_ASSERT_NOT_NULL(list);
  TEST_ASSERT_EQUAL_STRING("first", list->node.list->item->node.id->value);
  ast_free(list);
}

void test_list_append_null_item(void) {
  ast_node_t *list = NULL;
  ast_list_append(&list, NULL); // Should not crash
  TEST_ASSERT_NULL(list);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_identifier_node);
  RUN_TEST(test_logical_node);
  RUN_TEST(test_not_node);
  RUN_TEST(test_list_node_single);
  RUN_TEST(test_list_node_multiple_append);
  RUN_TEST(test_command_node);
  RUN_TEST(test_free_deep_tree);
  RUN_TEST(test_list_append_to_null);
  RUN_TEST(test_list_append_null_item);
  return UNITY_END();
}
