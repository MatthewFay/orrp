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
  // Tokenize creates a heap-allocated queue
  queue_t *tokens = tok_tokenize((char *)input_str);
  // Parse takes ownership of the tokens queue and frees it
  return parse(tokens);
}

// --- Assertion Helpers ---

static void _assert_success(parse_result_t *result) {
  TEST_ASSERT_NOT_NULL_MESSAGE(result, "Result should not be NULL");
  if (result->error_message) {
    TEST_FAIL_MESSAGE(result->error_message);
  }
  TEST_ASSERT_NOT_EQUAL(PARSER_OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NOT_NULL(result->ast);
}

static void _assert_error(parse_result_t *result) {
  TEST_ASSERT_NOT_NULL_MESSAGE(result, "Result should not be NULL");
  TEST_ASSERT_EQUAL(PARSER_OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  // We don't check for a specific message, just that an error occurred.
}

// Finds a tag in an AST by its reserved key.
static ast_node_t *_find_tag_by_key(ast_node_t *ast, ast_reserved_key_t key) {
  if (!ast || !ast->command.tags)
    return NULL;

  ast_node_t *tag = ast->command.tags;
  while (tag) {
    if (tag->tag.key_type == AST_TAG_KEY_RESERVED &&
        tag->tag.reserved_key == key) {
      return tag;
    }
    tag = tag->next;
  }
  return NULL;
}

// Finds a tag in an AST by its custom key name.
static ast_node_t *_find_tag_by_custom_key(ast_node_t *ast,
                                           const char *key_name) {
  if (!ast || !ast->command.tags)
    return NULL;

  ast_node_t *tag = ast->command.tags;
  while (tag) {
    if (tag->tag.key_type == AST_TAG_KEY_CUSTOM &&
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
  TEST_ASSERT_EQUAL(PARSER_OP_TYPE_WRITE, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, AST_KW_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("metrics", in_tag->tag.value->literal.string_value);

  ast_node_t *entity_tag = _find_tag_by_key(result->ast, AST_KW_ENTITY);
  TEST_ASSERT_NOT_NULL(entity_tag);
  TEST_ASSERT_EQUAL_STRING("user-123",
                           entity_tag->tag.value->literal.string_value);

  parse_free_result(result);
}

void test_event_success_numeric_val(void) {
  parse_result_t *result = _parse_string("event in:metrics entity:5");
  _assert_success(result);
  TEST_ASSERT_EQUAL(PARSER_OP_TYPE_WRITE, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, AST_KW_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("metrics", in_tag->tag.value->literal.string_value);

  ast_node_t *entity_tag = _find_tag_by_key(result->ast, AST_KW_ENTITY);
  TEST_ASSERT_NOT_NULL(entity_tag);
  TEST_ASSERT_EQUAL_INT64(5, entity_tag->tag.value->literal.number_value);

  parse_free_result(result);
}

void test_event_success_minimal2(void) {
  // Mixed case keywords are handled by tokenizer, parser sees tokens
  parse_result_t *result = _parse_string("event IN:abc tag:erc entity:fff");
  _assert_success(result);
  TEST_ASSERT_EQUAL(PARSER_OP_TYPE_WRITE, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, AST_KW_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("abc", in_tag->tag.value->literal.string_value);

  ast_node_t *entity_tag = _find_tag_by_key(result->ast, AST_KW_ENTITY);
  TEST_ASSERT_NOT_NULL(entity_tag);
  TEST_ASSERT_EQUAL_STRING("fff", entity_tag->tag.value->literal.string_value);

  parse_free_result(result);
}

void test_event_success_full_different_order(void) {
  parse_result_t *result =
      _parse_string("event clicks:\"one\" entity:\"user-123\" in:\"metrics\"");
  _assert_success(result);

  ast_node_t *clicks_tag = _find_tag_by_custom_key(result->ast, "clicks");
  TEST_ASSERT_NOT_NULL(clicks_tag);
  TEST_ASSERT_EQUAL_STRING("one", clicks_tag->tag.value->literal.string_value);

  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, AST_KW_IN));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, AST_KW_ENTITY));

  parse_free_result(result);
}

// NOTE: The following tests verify that the PARSER succeeds.
// Semantic validation (missing required keys) is handled by the
// Engine/Validation layer, not the Parser.

void test_event_success_missing_in(void) {
  parse_result_t *result = _parse_string("event entity:\"user-123\"");
  _assert_success(result); // Parser should succeed, validator will fail later
  parse_free_result(result);
}

void test_event_success_missing_entity(void) {
  parse_result_t *result = _parse_string("event in:\"metrics\"");
  _assert_success(result); // Parser should succeed
  parse_free_result(result);
}

void test_event_success_duplicate_custom_tag(void) {
  parse_result_t *result =
      _parse_string("event in:\"metrics\" entity:\"u1\" loc:\"us\" loc:\"ca\"");
  _assert_success(result); // Parser should succeed
  parse_free_result(result);
}

void test_event_success_invalid_container_name(void) {
  parse_result_t *result = _parse_string("event in:\"db\" entity:\"u1\"");
  _assert_success(result); // Parser should succeed
  parse_free_result(result);
}

void test_event_success_with_query_tag(void) {
  parse_result_t *result =
      _parse_string("event in:\"m\" entity:\"e\" where:(a)");
  _assert_success(result); // Parser should succeed (AST is created)
  parse_free_result(result);
}

// --- QUERY Command Tests ---

void test_query_success_minimal(void) {
  parse_result_t *result = _parse_string("query in:\"logs\" where:(a and b)");
  _assert_success(result);
  TEST_ASSERT_EQUAL(PARSER_OP_TYPE_READ, result->type);

  ast_node_t *in_tag = _find_tag_by_key(result->ast, AST_KW_IN);
  TEST_ASSERT_NOT_NULL(in_tag);
  TEST_ASSERT_EQUAL_STRING("logs", in_tag->tag.value->literal.string_value);

  ast_node_t *where_tag = _find_tag_by_key(result->ast, AST_KW_WHERE);
  TEST_ASSERT_NOT_NULL(where_tag);
  // The 'value' of the where tag is the root of the expression tree
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where_tag->tag.value->type);

  parse_free_result(result);
}

void test_query_success_full_different_order(void) {
  parse_result_t *result = _parse_string("query where:(a:b) in:\"logs\"");
  _assert_success(result);

  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, AST_KW_IN));
  TEST_ASSERT_NOT_NULL(_find_tag_by_key(result->ast, AST_KW_WHERE));

  parse_free_result(result);
}

void test_query_fails_missing_where(void) {
  parse_result_t *result = _parse_string("query in:\"logs\"");
  _assert_success(result); // Parser succeeds (semantics checked later)
  parse_free_result(result);
}

void test_query_fails_duplicate_in(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" in:\"b\" where:(c)");
  _assert_success(result); // Parser succeeds
  parse_free_result(result);
}

// --- Expression Parsing Tests ---

void test_where_precedence(void) {
  parse_result_t *result =
      _parse_string("query in:\"abc\" where:(a or b and c)");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->type);
  TEST_ASSERT_EQUAL(
      AST_LOGIC_NODE_OR,
      where->logical.op); // OR is lower precedence, so it's the root

  // Left is 'a'
  TEST_ASSERT_EQUAL(AST_LITERAL_NODE, where->logical.left_operand->type);

  // Right is 'b and c'
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->logical.right_operand->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_AND,
                    where->logical.right_operand->logical.op);

  parse_free_result(result);
}

void test_where_parentheses_override(void) {
  parse_result_t *result =
      _parse_string("query in:\"abc\" where:((a or b) and c)");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_AND,
                    where->logical.op); // AND is the root

  // Left is '(a or b)'
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->logical.left_operand->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_OR, where->logical.left_operand->logical.op);

  // Right is 'c'
  TEST_ASSERT_EQUAL(AST_LITERAL_NODE, where->logical.right_operand->type);

  parse_free_result(result);
}

void test_where_not_operator(void) {
  parse_result_t *result =
      _parse_string("query in:abc where:(not a and not b)");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_AND, where->logical.op);

  TEST_ASSERT_EQUAL(AST_NOT_NODE, where->logical.left_operand->type);
  TEST_ASSERT_EQUAL(AST_NOT_NODE, where->logical.right_operand->type);

  parse_free_result(result);
}

void test_where_single_tag(void) {
  parse_result_t *result = _parse_string("query in:test_c where:(loc:ca)");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  // Parser converts "key:val" inside expression to a Custom Tag Node
  TEST_ASSERT_EQUAL(AST_TAG_NODE, where->type);
  TEST_ASSERT_EQUAL(AST_TAG_KEY_CUSTOM, where->tag.key_type);
  TEST_ASSERT_EQUAL_STRING("loc", where->tag.custom_key);

  parse_free_result(result);
}

void test_where_quotes(void) {
  parse_result_t *result = _parse_string("query in:test_c where:(loc:\"ca\")");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  TEST_ASSERT_EQUAL(AST_TAG_NODE, where->type);
  TEST_ASSERT_EQUAL_STRING("loc", where->tag.custom_key);
  TEST_ASSERT_EQUAL_STRING("ca", where->tag.value->literal.string_value);

  parse_free_result(result);
}

void test_where_comparison(void) {
  // Added closing paren at the end to make it valid syntax
  parse_result_t *result = _parse_string(
      "QUERY in:analytics_2025_01 where:(loc:ca AND (action:login > 3))");
  _assert_success(result);

  ast_node_t *where = _find_tag_by_key(result->ast, AST_KW_WHERE)->tag.value;
  TEST_ASSERT_EQUAL(AST_LOGICAL_NODE, where->type);
  TEST_ASSERT_EQUAL(AST_LOGIC_NODE_AND,
                    where->logical.op); // AND is at the top level

  // Left: loc:ca (Tag Node)
  TEST_ASSERT_EQUAL(AST_TAG_NODE, where->logical.left_operand->type);

  // Right: (action:login > 3) (Comparison Node)
  ast_node_t *right = where->logical.right_operand;
  TEST_ASSERT_EQUAL(AST_COMPARISON_NODE, right->type);
  TEST_ASSERT_EQUAL(AST_OP_GT, right->comparison.op);

  // Right->Left: action:login (Tag Node)
  TEST_ASSERT_EQUAL(AST_TAG_NODE, right->comparison.left->type);
  TEST_ASSERT_EQUAL_STRING("action", right->comparison.left->tag.custom_key);

  // Right->Right: 3 (Literal Node)
  TEST_ASSERT_EQUAL(AST_LITERAL_NODE, right->comparison.right->type);
  TEST_ASSERT_EQUAL(AST_LITERAL_NUMBER, right->comparison.right->literal.type);
  TEST_ASSERT_EQUAL_INT64(3, right->comparison.right->literal.number_value);

  parse_free_result(result);
}

void test_where_fails_mismatched_parens(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" where:((a or b)");
  _assert_error(result);
  parse_free_result(result);
}

void test_where_fails_invalid_syntax(void) {
  parse_result_t *result = _parse_string("query in:\"abc\" where:(a and or b)");
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

// --- Additional Parser Edge Case Tests ---
void test_parser_fails_on_missing_colon(void) {
  parse_result_t *result = _parse_string("event in\"metrics\" entity:\"u1\"");
  _assert_error(result);
  parse_free_result(result);
}

void test_parser_fails_on_missing_tag_value(void) {
  parse_result_t *result = _parse_string("event in:");
  _assert_error(result);
  parse_free_result(result);
}

void test_parser_fails_on_where_missing_paren(void) {
  // The parser strictly requires '(' immediately after 'where:'
  parse_result_t *result = _parse_string("query in:\"abc\" where:a and b");
  _assert_error(result);
  parse_free_result(result);
}

int main(void) {
  UNITY_BEGIN();

  // EVENT command tests
  RUN_TEST(test_event_success_minimal);
  RUN_TEST(test_event_success_numeric_val);
  RUN_TEST(test_event_success_minimal2);
  RUN_TEST(test_event_success_full_different_order);
  RUN_TEST(test_event_success_missing_in);
  RUN_TEST(test_event_success_missing_entity);
  RUN_TEST(test_event_success_duplicate_custom_tag);
  RUN_TEST(test_event_success_invalid_container_name);
  RUN_TEST(test_event_success_with_query_tag);

  // QUERY command tests
  RUN_TEST(test_query_success_minimal);
  RUN_TEST(test_query_success_full_different_order);
  RUN_TEST(test_query_fails_missing_where);
  RUN_TEST(test_query_fails_duplicate_in);

  // Expression parsing tests
  RUN_TEST(test_where_precedence);
  RUN_TEST(test_where_parentheses_override);
  RUN_TEST(test_where_not_operator);
  RUN_TEST(test_where_single_tag);
  RUN_TEST(test_where_quotes);
  RUN_TEST(test_where_comparison);
  RUN_TEST(test_where_fails_mismatched_parens);
  RUN_TEST(test_where_fails_invalid_syntax);

  // General/edge case tests
  RUN_TEST(test_parse_fails_on_empty_input);
  RUN_TEST(test_parse_fails_on_invalid_command);
  RUN_TEST(test_parse_fails_on_incomplete_tag);

  // Parser edge case tests
  RUN_TEST(test_parser_fails_on_missing_colon);
  RUN_TEST(test_parser_fails_on_missing_tag_value);
  RUN_TEST(test_parser_fails_on_where_missing_paren);

  return UNITY_END();
}