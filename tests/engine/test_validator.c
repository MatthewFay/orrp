#include "engine/validator/validator.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// --- Configuration ---

// Fallback constant if not included from headers
#ifndef MAX_ENTITY_STR_LEN
#define MAX_ENTITY_STR_LEN 64
#endif

// --- Test Infrastructure ---

validator_result_t result;

void setUp(void) { memset(&result, 0, sizeof(validator_result_t)); }

void tearDown(void) {
  // Parser result cleanup handles AST freeing in the helper
}

// --- Helper: The "Real World" Check ---
// Parses the string -> Validates AST -> Asserts Result
void check_validity(const char *input, bool expected_valid,
                    const char *expected_err) {
  // 1. Tokenize & Parse (Real implementation)
  queue_t *tokens = tok_tokenize((char *)input);
  parse_result_t *p_res = parse(tokens);

  // Ensure Parser didn't fail on syntax before we could test semantics
  // Note: We expect the parser to SUCCEED for things like "event ... where:..."
  // because that is syntactically valid but semantically invalid.
  if (!p_res->success) {
    char msg[256];
    snprintf(msg, sizeof(msg),
             "Parser failed unexpectedly on input: '%s'. Error: %s", input,
             p_res->error_message);
    TEST_FAIL_MESSAGE(msg);
  }

  // 2. Validate (Real implementation)
  validator_analyze(p_res->ast, &result);

  // 3. Assertions
  if (expected_valid) {
    TEST_ASSERT_TRUE_MESSAGE(result.is_valid,
                             "Expected VALID, but got INVALID.");
  } else {
    TEST_ASSERT_FALSE_MESSAGE(result.is_valid,
                              "Expected INVALID, but got VALID.");
    if (expected_err) {
      TEST_ASSERT_EQUAL_STRING(expected_err, result.err_msg);
    }
  }

  // Cleanup
  parse_free_result(p_res);
}

// --- TEST GROUP 1: EVENT Command ---

void test_event_valid_minimal(void) {
  check_validity("event in:logs_2025 entity:user-123", true, NULL);
}

void test_event_valid_with_custom_tags(void) {
  check_validity("event in:logs entity:u1 price:100 loc:CA", true, NULL);
}

void test_event_fails_missing_in(void) {
  check_validity("event entity:user-1", false, "`in` tag is required");
}

void test_event_fails_missing_entity(void) {
  check_validity("event in:logs", false, "`entity` tag is required");
}

void test_event_fails_duplicate_in(void) {
  // Parser allows multiple 'in', Validator rejects
  check_validity("event in:logs entity:u1 in:metrics", false,
                 "Duplicate `in` tags not yet supported");
}

void test_event_fails_duplicate_entity(void) {
  check_validity("event in:logs entity:u1 entity:u2", false,
                 "Duplicate `entity` tag");
}

void test_event_fails_duplicate_custom_keys(void) {
  check_validity("event in:logs entity:u1 browser:chrome browser:firefox",
                 false, "Duplicate tag");
}

void test_event_fails_with_where_clause(void) {
  // This checks that 'where' is caught by validator, even if parser accepts it
  check_validity("event in:logs entity:u1 where:(custom:1)", false,
                 "`where` tag only supported for queries");
}

void test_event_fails_with_key_clause(void) {
  check_validity("event in:logs entity:u1 key:price", false,
                 "Unexpected `key` tag");
}

void test_fails_on_invalid_container_name_chars(void) {
  check_validity("event in:invalid.name entity:u1", false,
                 "Invalid container name");
}

void test_fails_on_invalid_container_name_dots(void) {
  check_validity("event in:.hidden entity:u1", false, "Invalid container name");
}

// --- TEST GROUP 2: QUERY Command ---

void test_query_valid_minimal(void) {
  check_validity("query in:logs where:(loc:ca)", true, NULL);
}

void test_query_fails_missing_where(void) {
  check_validity("query in:logs", false, "`where` tag is required");
}

void test_query_fails_with_entity_tag(void) {
  check_validity("query in:logs entity:u1 where:(loc:ca)", false,
                 "Unexpected `entity` tag");
}

// --- TEST GROUP 3: WHERE Clause Logic ---

void test_where_valid_comparison_mixed_types(void) {
  // String Key > Number Value
  check_validity("query in:logs where:(price > 50)", true, NULL);
}

void test_where_fails_comparison_same_types_number(void) {
  // Num > Num is structurally valid (parser ok), but semantically useless
  check_validity("query in:logs where:(5 > 10)", false,
                 "Invalid comparison types");
}

void test_where_fails_comparison_same_types_string(void) {
  // String > String
  check_validity("query in:logs where:(loc > ca)", false,
                 "Invalid comparison types");
}

void test_where_valid_recursive_logic(void) {
  check_validity("query in:logs where:((loc:ca) AND (price > 10))", true, NULL);
}

void test_where_valid_not_logic(void) {
  check_validity("query in:logs where:(NOT loc:ca)", true, NULL);
}

// --- TEST GROUP 4: INDEX Command ---

void test_index_valid(void) { check_validity("index key:price", true, NULL); }

void test_index_fails_unexpected_tag(void) {
  check_validity("index a:b", false, "Unexpected tag");
}

void test_index_fails_with_in_tag(void) {
  check_validity("index key:price in:logs", false,
                 "Indexing specific containers is not supported yet. Indexes "
                 "apply globally to new data containers.");
}

// --- TEST GROUP 5: Edge Cases (Manual AST) ---
// We use manual construction here to test defensive coding against inputs
// that the parser would normally block, but we want to ensure Validator handles
// safely.

void test_fails_on_null_root(void) {
  validator_analyze(NULL, &result);
  TEST_ASSERT_FALSE(result.is_valid);
}

// Helper to manually build a really long string literal node
static ast_node_t *_manual_long_string_node(void) {
  ast_node_t *node = calloc(1, sizeof(ast_node_t));
  node->type = AST_TAG_NODE;
  node->tag.key_type = AST_TAG_KEY_RESERVED;
  node->tag.reserved_key = AST_KW_ENTITY;

  // Create a value node > MAX_ENTITY_STR_LEN
  ast_node_t *val = calloc(1, sizeof(ast_node_t));
  val->type = AST_LITERAL_NODE;
  val->literal.type = AST_LITERAL_STRING;
  // 70 chars
  val->literal.string_value = strdup(
      "1234567890123456789012345678901234567890123456789012345678901234567890");
  val->literal.string_value_len = 70;

  node->tag.value = val;
  return node;
}

void test_fails_on_entity_name_too_long(void) {
  // We construct this manually to avoid typing a 70 char string in the parser
  // helper
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);

  // Add "in:logs"
  ast_node_t *in_tag =
      ast_create_tag_node(AST_KW_IN, ast_create_string_literal_node("logs", 4));
  ast_append_node(&cmd->command.tags, in_tag);

  // Add invalid entity tag
  ast_node_t *bad_tag = _manual_long_string_node();
  ast_append_node(&cmd->command.tags, bad_tag);

  validator_analyze(cmd, &result);

  TEST_ASSERT_FALSE(result.is_valid);
  TEST_ASSERT_EQUAL_STRING("`entity` value too long", result.err_msg);

  ast_free(cmd);
}

// --- MAIN RUNNER ---

int main(void) {
  UNITY_BEGIN();

  // Event Tests
  RUN_TEST(test_event_valid_minimal);
  RUN_TEST(test_event_valid_with_custom_tags);
  RUN_TEST(test_event_fails_missing_in);
  RUN_TEST(test_event_fails_missing_entity);
  RUN_TEST(test_event_fails_duplicate_in);
  RUN_TEST(test_event_fails_duplicate_entity);
  RUN_TEST(test_event_fails_duplicate_custom_keys);
  RUN_TEST(test_event_fails_with_where_clause);
  RUN_TEST(test_event_fails_with_key_clause);
  RUN_TEST(test_fails_on_invalid_container_name_chars);
  RUN_TEST(test_fails_on_invalid_container_name_dots);

  // Query Tests
  RUN_TEST(test_query_valid_minimal);
  RUN_TEST(test_query_fails_missing_where);
  RUN_TEST(test_query_fails_with_entity_tag);

  // Where Logic Tests
  RUN_TEST(test_where_valid_comparison_mixed_types);
  RUN_TEST(test_where_fails_comparison_same_types_number);
  RUN_TEST(test_where_fails_comparison_same_types_string);
  RUN_TEST(test_where_valid_recursive_logic);
  RUN_TEST(test_where_valid_not_logic);

  // Index Tests
  RUN_TEST(test_index_valid);
  RUN_TEST(test_index_fails_unexpected_tag);
  RUN_TEST(test_index_fails_with_in_tag);

  // Manual / Defensive Tests
  RUN_TEST(test_fails_on_null_root);
  RUN_TEST(test_fails_on_entity_name_too_long);

  return UNITY_END();
}