#include "core/queue.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// --- Test Infrastructure ---

// No global state: each test manages its own tokens/result.
void setUp(void) {}
void tearDown(void) {}

// Helper to add a token to a given queue.
static void _add_token(Queue *tokens, token_type type, const char *text) {
  token_t *t = malloc(sizeof(token_t));
  t->type = type;
  t->text_value = text ? strdup(text) : NULL;
  t->number_value = 0;
  q_enqueue(tokens, t);
}

// --- Assertion Helpers ---

// Asserts that the parsing was successful.
static void _assert_success(parse_result_t *result) {
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_NOT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NOT_NULL(result->ast);
  TEST_ASSERT_NULL(result->error_message);
}

// Asserts that the parsing failed with a specific error message.
static void _assert_error(parse_result_t *result, const char *expected_msg) {
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  TEST_ASSERT_NOT_NULL(result->error_message);
  if (expected_msg) {
    TEST_ASSERT_EQUAL_STRING(expected_msg, result->error_message);
  }
}

// Helper to check if a node is the identifier we expect.
static void _assert_identifier_node(ast_node_t *node,
                                    const char *expected_val) {
  TEST_ASSERT_NOT_NULL(node);
  TEST_ASSERT_EQUAL(IDENTIFIER_NODE, node->type);
  TEST_ASSERT_EQUAL_STRING(expected_val, node->node.id->value);
}

// --- ADD Command Tests ---

void test_parse_add_command_happy_path(void) {
  Queue *tokens = q_create();
  _add_token(tokens, ADD_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "foo");
  _add_token(tokens, IDENTIFIER, "bar");
  _add_token(tokens, IDENTIFIER, "baz");

  parse_result_t *result = parse(tokens);
  _assert_success(result);
  TEST_ASSERT_EQUAL(OP_TYPE_WRITE, result->type);

  ast_node_t *ast = result->ast;
  TEST_ASSERT_EQUAL(COMMAND_NODE, ast->type);
  TEST_ASSERT_EQUAL(ADD, ast->node.cmd->cmd_type);

  // Check the arguments list thoroughly
  ast_node_t *arg_node = ast->node.cmd->args;
  TEST_ASSERT_NOT_NULL(arg_node);
  _assert_identifier_node(arg_node->node.list->item, "foo");

  arg_node = arg_node->node.list->next;
  TEST_ASSERT_NOT_NULL(arg_node);
  _assert_identifier_node(arg_node->node.list->item, "bar");

  arg_node = arg_node->node.list->next;
  TEST_ASSERT_NOT_NULL(arg_node);
  _assert_identifier_node(arg_node->node.list->item, "baz");

  arg_node = arg_node->node.list->next;
  TEST_ASSERT_NULL(arg_node); // End of list

  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_add_fails_with_too_few_args(void) {
  Queue *tokens = q_create();
  _add_token(tokens, ADD_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "foo");
  _add_token(tokens, IDENTIFIER, "bar");

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Wrong number of arguments for ADD");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_add_fails_with_too_many_args(void) {
  Queue *tokens = q_create();
  _add_token(tokens, ADD_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "foo");
  _add_token(tokens, IDENTIFIER, "bar");
  _add_token(tokens, IDENTIFIER, "baz");
  _add_token(tokens, IDENTIFIER, "qux"); // The extra argument

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Wrong number of arguments for ADD");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_add_fails_with_wrong_arg_type(void) {
  Queue *tokens = q_create();
  _add_token(tokens, ADD_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "foo");
  _add_token(tokens, NUMBER, NULL); // Should be an IDENTIFIER
  _add_token(tokens, IDENTIFIER, "baz");

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Invalid argument type for ADD");
  parse_free_result(result);
  q_destroy(tokens);
}

// --- QUERY Command Tests ---

void test_parse_query_simple_no_expression(void) {
  Queue *tokens = q_create();
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");

  parse_result_t *result = parse(tokens);
  _assert_success(result);
  TEST_ASSERT_EQUAL(OP_TYPE_READ, result->type);

  ast_node_t *ast = result->ast;
  TEST_ASSERT_EQUAL(COMMAND_NODE, ast->type);
  TEST_ASSERT_EQUAL(QUERY, ast->node.cmd->cmd_type);

  // Check arg and ensure expression is NULL
  _assert_identifier_node(ast->node.cmd->args->node.list->item, "analytics");
  TEST_ASSERT_NULL(ast->node.cmd->exp);

  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_simple_expression(void) {
  Queue *tokens = q_create();
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");
  _add_token(tokens, IDENTIFIER, "login_2025");

  parse_result_t *result = parse(tokens);
  _assert_success(result);

  ast_node_t *ast = result->ast;
  _assert_identifier_node(ast->node.cmd->args->node.list->item, "analytics");
  _assert_identifier_node(ast->node.cmd->exp, "login_2025");

  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_precedence_and_grouping_1(void) {
  Queue *tokens = q_create();
  // Test: QUERY analytics ((A OR A2) AND B) OR NOT (C AND (D OR E))
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");

  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, LPAREN, NULL);

  _add_token(tokens, IDENTIFIER, "A");
  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, IDENTIFIER, "A2");
  _add_token(tokens, RPAREN, NULL);
  _add_token(tokens, AND_OP, NULL);
  _add_token(tokens, IDENTIFIER, "B");
  _add_token(tokens, RPAREN, NULL);

  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, NOT_OP, NULL);

  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, IDENTIFIER, "C");
  _add_token(tokens, AND_OP, NULL);
  _add_token(tokens, LPAREN, NULL);

  _add_token(tokens, IDENTIFIER, "D");
  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, IDENTIFIER, "E");

  _add_token(tokens, RPAREN, NULL);
  _add_token(tokens, RPAREN, NULL);

  parse_result_t *result = parse(tokens);
  _assert_success(result);

  ast_node_t *exp = result->ast->node.cmd->exp;
  TEST_ASSERT_NOT_NULL(exp);

  // Top level: OR
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(OR, exp->node.logical->op);

  // Left: AND node (from ((A OR A2) AND B))
  ast_node_t *and_node = exp->node.logical->left_operand;
  TEST_ASSERT_NOT_NULL(and_node);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and_node->type);
  TEST_ASSERT_EQUAL(AND, and_node->node.logical->op);

  // Left of AND: (A OR A2)
  ast_node_t *or_left = and_node->node.logical->left_operand;
  TEST_ASSERT_NOT_NULL(or_left);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, or_left->type);
  TEST_ASSERT_EQUAL(OR, or_left->node.logical->op);
  _assert_identifier_node(or_left->node.logical->left_operand, "A");
  _assert_identifier_node(or_left->node.logical->right_operand, "A2");

  // Right of AND: B
  _assert_identifier_node(and_node->node.logical->right_operand, "B");

  // Right of top-level OR: NOT node
  ast_node_t *not_node = exp->node.logical->right_operand;
  TEST_ASSERT_NOT_NULL(not_node);
  TEST_ASSERT_EQUAL(NOT_NODE, not_node->type);

  // NOT's operand: AND node (C AND (...))
  ast_node_t *and2 = not_node->node.not_op->operand;
  TEST_ASSERT_NOT_NULL(and2);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and2->type);
  TEST_ASSERT_EQUAL(AND, and2->node.logical->op);

  // Left of AND: C
  _assert_identifier_node(and2->node.logical->left_operand, "C");

  // Right of AND: OR node (D OR E)
  ast_node_t *or2 = and2->node.logical->right_operand;
  TEST_ASSERT_NOT_NULL(or2);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, or2->type);
  TEST_ASSERT_EQUAL(OR, or2->node.logical->op);
  _assert_identifier_node(or2->node.logical->left_operand, "D");
  _assert_identifier_node(or2->node.logical->right_operand, "E");

  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_precedence_and_grouping_2(void) {
  Queue *tokens = q_create();
  // Test: QUERY analytics A OR B AND NOT (C OR D)
  // Expected AST: OR(A, AND(B, NOT(OR(C, D))))
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");
  _add_token(tokens, IDENTIFIER, "A");
  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, IDENTIFIER, "B");
  _add_token(tokens, AND_OP, NULL);
  _add_token(tokens, NOT_OP, NULL);
  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, IDENTIFIER, "C");
  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, IDENTIFIER, "D");
  _add_token(tokens, RPAREN, NULL);

  parse_result_t *result = parse(tokens);
  _assert_success(result);

  ast_node_t *exp = result->ast->node.cmd->exp;
  TEST_ASSERT_NOT_NULL(exp);

  // Top level should be OR because it has lower precedence
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(OR, exp->node.logical->op);

  // Left side of OR should be "A"
  _assert_identifier_node(exp->node.logical->left_operand, "A");

  // Right side of OR should be the AND expression
  ast_node_t *and_node = exp->node.logical->right_operand;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and_node->type);
  TEST_ASSERT_EQUAL(AND, and_node->node.logical->op);

  // Left side of AND should be "B"
  _assert_identifier_node(and_node->node.logical->left_operand, "B");

  // Right side of AND should be the NOT expression
  ast_node_t *not_node = and_node->node.logical->right_operand;
  TEST_ASSERT_EQUAL(NOT_NODE, not_node->type);

  // The operand of NOT should be the (C OR D) expression
  ast_node_t *inner_or_node = not_node->node.not_op->operand;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, inner_or_node->type);
  TEST_ASSERT_EQUAL(OR, inner_or_node->node.logical->op);

  _assert_identifier_node(inner_or_node->node.logical->left_operand, "C");
  _assert_identifier_node(inner_or_node->node.logical->right_operand, "D");

  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_precedence_and_grouping_3(void) {
  Queue *tokens = q_create();
  // Test: QUERY analytics (((A AND B) AND (C AND D)) OR E)
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");

  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, LPAREN, NULL);

  _add_token(tokens, IDENTIFIER, "A");
  _add_token(tokens, AND_OP, NULL);
  _add_token(tokens, IDENTIFIER, "B");
  _add_token(tokens, RPAREN, NULL);

  _add_token(tokens, AND_OP, NULL);

  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, IDENTIFIER, "C");
  _add_token(tokens, AND_OP, NULL);
  _add_token(tokens, IDENTIFIER, "D");
  _add_token(tokens, RPAREN, NULL);

  _add_token(tokens, RPAREN, NULL);

  _add_token(tokens, OR_OP, NULL);
  _add_token(tokens, IDENTIFIER, "E");
  _add_token(tokens, RPAREN, NULL);

  parse_result_t *result = parse(tokens);
  _assert_success(result);

  ast_node_t *exp = result->ast->node.cmd->exp;
  TEST_ASSERT_NOT_NULL(exp);

  // Top level: OR
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(OR, exp->node.logical->op);

  // Left: AND node (from ((A AND B) AND (C AND D)))
  ast_node_t *and_outer = exp->node.logical->left_operand;
  TEST_ASSERT_NOT_NULL(and_outer);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and_outer->type);
  TEST_ASSERT_EQUAL(AND, and_outer->node.logical->op);

  // Left of outer AND: (A AND B)
  ast_node_t *and_left = and_outer->node.logical->left_operand;
  TEST_ASSERT_NOT_NULL(and_left);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and_left->type);
  TEST_ASSERT_EQUAL(AND, and_left->node.logical->op);
  _assert_identifier_node(and_left->node.logical->left_operand, "A");
  _assert_identifier_node(and_left->node.logical->right_operand, "B");

  // Right of outer AND: (C AND D)
  ast_node_t *and_right = and_outer->node.logical->right_operand;
  TEST_ASSERT_NOT_NULL(and_right);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, and_right->type);
  TEST_ASSERT_EQUAL(AND, and_right->node.logical->op);
  _assert_identifier_node(and_right->node.logical->left_operand, "C");
  _assert_identifier_node(and_right->node.logical->right_operand, "D");

  // Right of top-level OR: E
  _assert_identifier_node(exp->node.logical->right_operand, "E");

  parse_free_result(result);
  q_destroy(tokens);
}

// --- General Parser and Edge Case Tests ---

void test_parse_fails_on_null_input(void) {
  parse_result_t *result = parse(NULL);
  _assert_error(result, "Invalid input: token queue is NULL.");
  parse_free_result(result);
}

void test_parse_fails_on_empty_input(void) {
  Queue *tokens = q_create();
  parse_result_t *result = parse(tokens);
  _assert_error(result, "Invalid input: token queue is empty.");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_fails_on_invalid_command(void) {
  Queue *tokens = q_create();
  _add_token(tokens, IDENTIFIER, "JUMP"); // Not a valid command
  _add_token(tokens, IDENTIFIER, "foo");

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Invalid command.");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_fails_on_missing_argument(void) {
  Queue *tokens = q_create();
  _add_token(tokens, QUERY_CMD, NULL);

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Invalid syntax: Expected an argument after QUERY");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_fails_on_trailing_tokens(void) {
  Queue *tokens = q_create();
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");
  _add_token(tokens, IDENTIFIER, "login_ok");
  _add_token(tokens, IDENTIFIER,
             "extra_token_is_error"); // This should not be here

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Syntax error: Unexpected token, expected operator.");
  parse_free_result(result);
  q_destroy(tokens);
}

void test_parse_query_fails_on_mismatched_parenthesis(void) {
  Queue *tokens = q_create();
  _add_token(tokens, QUERY_CMD, NULL);
  _add_token(tokens, IDENTIFIER, "analytics");
  _add_token(tokens, LPAREN, NULL);
  _add_token(tokens, IDENTIFIER, "A");

  parse_result_t *result = parse(tokens);
  _assert_error(result, "Mismatched parentheses");
  parse_free_result(result);
  q_destroy(tokens);
}

int main(void) {
  UNITY_BEGIN();

  // ADD tests
  RUN_TEST(test_parse_add_command_happy_path);
  RUN_TEST(test_parse_add_fails_with_too_few_args);
  RUN_TEST(test_parse_add_fails_with_too_many_args);
  RUN_TEST(test_parse_add_fails_with_wrong_arg_type);

  // QUERY tests
  RUN_TEST(test_parse_query_simple_no_expression);
  RUN_TEST(test_parse_query_simple_expression);

  RUN_TEST(test_parse_query_precedence_and_grouping_1);
  RUN_TEST(test_parse_query_precedence_and_grouping_2);
  RUN_TEST(test_parse_query_precedence_and_grouping_3);

  // General/Edge Case tests
  RUN_TEST(test_parse_fails_on_null_input);
  RUN_TEST(test_parse_fails_on_empty_input);
  RUN_TEST(test_parse_fails_on_invalid_command);
  RUN_TEST(test_parse_query_fails_on_missing_argument);
  RUN_TEST(test_parse_query_fails_on_trailing_tokens);
  RUN_TEST(test_parse_query_fails_on_mismatched_parenthesis);

  return UNITY_END();
}