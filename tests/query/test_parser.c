#include "core/queue.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// --- Test Infrastructure ---

// A struct to hold state for each test, managed by setUp and tearDown.
typedef struct {
  Queue *tokens;
  parse_result_t *result;
} TestState;

static TestState g_test;

// This runs before each test.
void setUp(void) {
  g_test.tokens = q_create();
  g_test.result = NULL;
}

// This runs after each test, ensuring everything is cleaned up.
void tearDown(void) { parse_free_result(g_test.result); }

// A robust helper to add a token of any type to the current test's queue.
static void _add_token(token_type type, const char *text) {
  token_t *t = malloc(sizeof(token_t));
  t->type = type;
  t->text_value = text ? strdup(text) : NULL;
  t->number_value = 0;
  q_enqueue(g_test.tokens, t);
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
  // Build tokens: ADD foo bar baz
  _add_token(ADD_CMD, NULL);
  _add_token(IDENTIFIER, "foo");
  _add_token(IDENTIFIER, "bar");
  _add_token(IDENTIFIER, "baz");

  g_test.result = parse(g_test.tokens);
  _assert_success(g_test.result);
  TEST_ASSERT_EQUAL(OP_TYPE_WRITE, g_test.result->type);

  ast_node_t *ast = g_test.result->ast;
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
}

void test_parse_add_fails_with_too_few_args(void) {
  _add_token(ADD_CMD, NULL);
  _add_token(IDENTIFIER, "foo");
  _add_token(IDENTIFIER, "bar");

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Wrong number of arguments for ADD");
}

void test_parse_add_fails_with_too_many_args(void) {
  _add_token(ADD_CMD, NULL);
  _add_token(IDENTIFIER, "foo");
  _add_token(IDENTIFIER, "bar");
  _add_token(IDENTIFIER, "baz");
  _add_token(IDENTIFIER, "qux"); // The extra argument

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Wrong number of arguments for ADD");
}

void test_parse_add_fails_with_wrong_arg_type(void) {
  _add_token(ADD_CMD, NULL);
  _add_token(IDENTIFIER, "foo");
  _add_token(NUMBER, NULL); // Should be an IDENTIFIER
  _add_token(IDENTIFIER, "baz");

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Invalid argument type for ADD");
}

// --- QUERY Command Tests ---

void test_parse_query_simple_no_expression(void) {
  _add_token(QUERY_CMD, NULL);
  _add_token(IDENTIFIER, "analytics");

  g_test.result = parse(g_test.tokens);
  _assert_success(g_test.result);
  TEST_ASSERT_EQUAL(OP_TYPE_READ, g_test.result->type);

  ast_node_t *ast = g_test.result->ast;
  TEST_ASSERT_EQUAL(COMMAND_NODE, ast->type);
  TEST_ASSERT_EQUAL(QUERY, ast->node.cmd->cmd_type);

  // Check arg and ensure expression is NULL
  _assert_identifier_node(ast->node.cmd->args->node.list->item, "analytics");
  TEST_ASSERT_NULL(ast->node.cmd->exp);
}

void test_parse_query_simple_expression(void) {
  _add_token(QUERY_CMD, NULL);
  _add_token(IDENTIFIER, "analytics");
  _add_token(IDENTIFIER, "login_2025");

  g_test.result = parse(g_test.tokens);
  _assert_success(g_test.result);

  ast_node_t *ast = g_test.result->ast;
  _assert_identifier_node(ast->node.cmd->args->node.list->item, "analytics");
  _assert_identifier_node(ast->node.cmd->exp, "login_2025");
}

void test_parse_query_precedence_and_grouping(void) {
  // Test: QUERY analytics A OR B AND NOT (C OR D)
  // Expected AST: OR(A, AND(B, NOT(OR(C, D))))
  _add_token(QUERY_CMD, NULL);
  _add_token(IDENTIFIER, "analytics");
  _add_token(IDENTIFIER, "A");
  _add_token(OR_OP, NULL);
  _add_token(IDENTIFIER, "B");
  _add_token(AND_OP, NULL);
  _add_token(NOT_OP, NULL);
  _add_token(LPAREN, NULL);
  _add_token(IDENTIFIER, "C");
  _add_token(OR_OP, NULL);
  _add_token(IDENTIFIER, "D");
  _add_token(RPAREN, NULL);

  g_test.result = parse(g_test.tokens);
  _assert_success(g_test.result);

  ast_node_t *exp = g_test.result->ast->node.cmd->exp;
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
}

// --- General Parser and Edge Case Tests ---

void test_parse_fails_on_null_input(void) {
  // Can't use setup here as we are passing NULL directly
  g_test.result = parse(NULL);
  _assert_error(g_test.result, "Invalid input: token queue is NULL.");
}

void test_parse_fails_on_empty_input(void) {
  // setUp provides an empty queue, so we don't need to add tokens
  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Invalid input: token queue is empty.");
}

void test_parse_fails_on_invalid_command(void) {
  _add_token(IDENTIFIER, "JUMP"); // Not a valid command
  _add_token(IDENTIFIER, "foo");

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Invalid command.");
}

void test_parse_query_fails_on_missing_argument(void) {
  _add_token(QUERY_CMD, NULL);

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result,
                "Invalid syntax: Expected an argument after QUERY");
}

void test_parse_query_fails_on_trailing_tokens(void) {
  _add_token(QUERY_CMD, NULL);
  _add_token(IDENTIFIER, "analytics");
  _add_token(IDENTIFIER, "login_ok");
  _add_token(IDENTIFIER, "extra_token_is_error"); // This should not be here

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result,
                "Invalid syntax: Unexpected token after expression");
}

void test_parse_query_fails_on_mismatched_parenthesis(void) {
  _add_token(QUERY_CMD, NULL);
  _add_token(IDENTIFIER, "analytics");
  _add_token(LPAREN, NULL);
  _add_token(IDENTIFIER, "A");

  g_test.result = parse(g_test.tokens);
  _assert_error(g_test.result, "Mismatched parentheses");
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
  RUN_TEST(test_parse_query_precedence_and_grouping);

  // General/Edge Case tests
  RUN_TEST(test_parse_fails_on_null_input);
  RUN_TEST(test_parse_fails_on_empty_input);
  RUN_TEST(test_parse_fails_on_invalid_command);
  RUN_TEST(test_parse_query_fails_on_missing_argument);
  RUN_TEST(test_parse_query_fails_on_trailing_tokens);
  RUN_TEST(test_parse_query_fails_on_mismatched_parenthesis);

  return UNITY_END();
}