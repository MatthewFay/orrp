#include "core/queue.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// --- Test Infrastructure ---

void setUp(void) {}
void tearDown(void) {}

// Helper to run the tokenizer and parser on a raw string.
static parse_result_t *_parse_string(const char *input_str) {
  Queue *tokens = tok_tokenize((char *)input_str);
  // The parser takes ownership of and frees the tokens queue
  return parse(tokens);
}

// --- Assertion Helpers ---

static void _assert_success(parse_result_t *result) {
  TEST_ASSERT_NOT_NULL_MESSAGE(result, "Result should not be NULL");
  if (result->error_message) {
    TEST_FAIL_MESSAGE(result->error_message);
  }
  TEST_ASSERT_NOT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NOT_NULL(result->ast);
}

static void _assert_error(parse_result_t *result) {
  TEST_ASSERT_NOT_NULL_MESSAGE(result, "Result should not be NULL");
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  // We don't check for a specific message, just that an error occurred.
}

// Finds a tag in an AST by its reserved key.
static ast_node_t *_find_tag_by_key(ast_node_t *ast, ast_reserved_key_t key) {
  TEST_ASSERT_NOT_NULL(ast);
  ast_node_t *tag = ast->command.tags;
  while (tag) {
    if (tag->tag.key_type == TAG_KEY_RESERVED && tag->tag.reserved_key == key) {
      return tag;
    }
    tag = tag->next;
  }
  return NULL;
}

// Finds a tag in an AST by its custom key name.
static ast_node_t *_find_tag_by_custom_key(ast_node_t *ast,
                                           const char *key_name) {
  TEST_ASSERT_NOT_NULL(ast);
  ast_node_t *tag = ast->command.tags;
  while (tag) {
    if (tag->tag.key_type == TAG_KEY_CUSTOM &&
        strcmp(tag->tag.custom_key, key_name) == 0) {
      return tag;
    }
    tag = tag->next;
  }
  return NULL;
}

// --- EVENT Command Tests ---

void test_event_success_minimal(void) {
  parse_result_t *result =
      _parse_string("event in:\"metrics\" entity:\"user-123\"");
  _assert_success(result);
  TEST_ASSERT_EQUAL(OP_TYPE_WRITE, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, KEY_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("metrics", in_tag->tag.value->literal.string_value);

  ast_node_t *entity_tag = _find_tag_by_key(result->ast, KEY_ENTITY);
  TEST_ASSERT_NOT_NULL(entity_tag);
  TEST_ASSERT_EQUAL_STRING("user-123",
                           entity_tag->tag.value->literal.string_value);

  parse_free_result(result);
}

void test_event_success_full_different_order(void) {
  parse_result_t *result = _parse_string(
      "event id:\"abc\" clicks:\"one\"+ entity:\"user-123\" in:\"metrics\"");
  _assert_success(result);

  ast_node_t *clicks_tag = _find_tag_by_custom_key(result->ast, "clicks");
  TEST_ASSERT_NOT_NULL(clicks_tag);
  TEST_ASSERT_TRUE(clicks_tag->tag.is_counter);
  TEST_ASSERT_EQUAL_STRING("one", clicks_tag->tag.value->literal.string_value);

  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_IN));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_ID));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_ENTITY));

  parse_free_result(result);
}

void test_event_fails_missing_in(void) {
  parse_result_t *result = _parse_string("event entity:\"user-123\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_event_fails_missing_entity(void) {
  parse_result_t *result = _parse_string("event in:\"metrics\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_event_fails_duplicate_custom_tag(void) {
  parse_result_t *result =
      _parse_string("event in:\"metrics\" entity:\"u1\" loc:\"us\" loc:\"ca\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_event_fails_invalid_container_name(void) {
  parse_result_t *result = _parse_string("event in:\"db\" entity:\"u1\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_event_fails_with_query_only_tag(void) {
  parse_result_t *result = _parse_string("event in:\"m\" entity:\"e\" exp:(a)");
  _assert_error(result);
  parse_free_result(result);
}

// --- QUERY Command Tests ---

void test_query_success_minimal(void) {
  parse_result_t *result = _parse_string("query in:\"logs\" exp:(a and b)");
  _assert_success(result);
  TEST_ASSERT_EQUAL(OP_TYPE_READ, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, KEY_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("logs", in_tag->tag.value->literal.string_value);

  ast_node_t *exp_tag = _find_tag_by_key(result->ast, KEY_EXP);
  TEST_ASSERT_NOT_NULL(exp_tag);
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp_tag->tag.value->type);

  parse_free_result(result);
}

void test_query_success_full_different_order(void) {
  parse_result_t *result =
      _parse_string("query exp:(a) cursor:\"xyz\" in:\"logs\" loc:\"us\"");
  _assert_success(result);

  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_IN));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_EXP));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, KEY_CURSOR));
  TEST_ASSERT_NOT_NULL(_find_tag_by_custom_key(result->ast, "loc"));

  parse_free_result(result);
}

void test_query_fails_missing_exp(void) {
  parse_result_t *result = _parse_string("query in:\"logs\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_query_fails_duplicate_in(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" in:\"b\" exp:(c)");
  _assert_error(result);
  parse_free_result(result);
}

// --- Expression Parsing Tests ---

void test_exp_precedence(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" exp:(a or b and c)");
  _assert_success(result);

  ast_node_t *exp = _find_tag_by_key(result->ast, KEY_EXP)->tag.value;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(OR, exp->logical.op); // OR is at the top level
  TEST_ASSERT_EQUAL(LITERAL_NODE, exp->logical.left_operand->type);
  TEST_ASSERT_EQUAL(LOGICAL_NODE,
                    exp->logical.right_operand->type); // AND is nested
  TEST_ASSERT_EQUAL(AND, exp->logical.right_operand->logical.op);

  parse_free_result(result);
}

void test_exp_parentheses_override(void) {
  parse_result_t *result =
      _parse_string("query in:\"abc\" exp:((a or b) and c)");
  _assert_success(result);

  ast_node_t *exp = _find_tag_by_key(result->ast, KEY_EXP)->tag.value;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(AND, exp->logical.op); // AND is at the top level
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->logical.left_operand->type);
  TEST_ASSERT_EQUAL(OR, exp->logical.left_operand->logical.op);
  TEST_ASSERT_EQUAL(LITERAL_NODE, exp->logical.right_operand->type);

  parse_free_result(result);
}

void test_exp_not_operator(void) {
  parse_result_t *result = _parse_string("query in:abc exp:(not a and not b)");
  _assert_success(result);

  ast_node_t *exp = _find_tag_by_key(result->ast, KEY_EXP)->tag.value;
  TEST_ASSERT_EQUAL(LOGICAL_NODE, exp->type);
  TEST_ASSERT_EQUAL(AND, exp->logical.op);
  TEST_ASSERT_EQUAL(NOT_NODE, exp->logical.left_operand->type);
  TEST_ASSERT_EQUAL(NOT_NODE, exp->logical.right_operand->type);

  parse_free_result(result);
}

void test_exp_fails_mismatched_parens(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" exp:((a or b)");
  _assert_error(result);
  parse_free_result(result);
}

void test_exp_fails_invalid_syntax(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" exp:(a and or b)");
  _assert_error(result);
  parse_free_result(result);
}

// --- General Parser and Edge Case Tests ---

void test_parse_fails_on_empty_input(void) {
  parse_result_t *result = _parse_string("");
  _assert_error(result);
  parse_free_result(result);
}

void test_parse_fails_on_invalid_command(void) {
  parse_result_t *result = _parse_string("update in:\"abc\" entity:\"b\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_parse_fails_on_incomplete_tag(void) {
  parse_result_t *result = _parse_string("query in:");
  _assert_error(result);
  parse_free_result(result);
}

int main(void) {
  UNITY_BEGIN();

  // EVENT command tests
  RUN_TEST(test_event_success_minimal);
  RUN_TEST(test_event_success_full_different_order);
  RUN_TEST(test_event_fails_missing_in);
  RUN_TEST(test_event_fails_missing_entity);
  RUN_TEST(test_event_fails_duplicate_custom_tag);
  RUN_TEST(test_event_fails_invalid_container_name);
  RUN_TEST(test_event_fails_with_query_only_tag);

  // QUERY command tests
  RUN_TEST(test_query_success_minimal);
  RUN_TEST(test_query_success_full_different_order);
  RUN_TEST(test_query_fails_missing_exp);
  RUN_TEST(test_query_fails_duplicate_in);

  // Expression parsing tests
  RUN_TEST(test_exp_precedence);
  RUN_TEST(test_exp_parentheses_override);
  RUN_TEST(test_exp_not_operator);
  RUN_TEST(test_exp_fails_mismatched_parens);
  RUN_TEST(test_exp_fails_invalid_syntax);

  // General/edge case tests
  RUN_TEST(test_parse_fails_on_empty_input);
  RUN_TEST(test_parse_fails_on_invalid_command);
  RUN_TEST(test_parse_fails_on_incomplete_tag);

  return UNITY_END();
}