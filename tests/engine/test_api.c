#include "engine/api.h"
#include "engine/engine.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

// --- Mocking and Test Infrastructure ---

// Mock engine functions
typedef struct {
  int called;
  ast_node_t *last_ast;
  int64_t last_ts;
} mock_eng_state_t;

static mock_eng_state_t mock_state;
// We hold the last parse result globally so we can free it in tearDown
static parse_result_t *last_parse_res = NULL;

void eng_event(api_response_t *resp, ast_node_t *ast, int64_t arrival_ts) {
  mock_state.called++;
  mock_state.last_ast = ast;
  mock_state.last_ts = arrival_ts;
  resp->is_ok = true;
  resp->err_msg = NULL;
  resp->op_type = API_EVENT;
}

void eng_query(api_response_t *resp, ast_node_t *ast) {
  mock_state.called++;
  mock_state.last_ast = ast;
  resp->is_ok = true;
  resp->err_msg = NULL;
  resp->op_type = API_QUERY;
}

void eng_index(api_response_t *resp, ast_node_t *ast) {
  mock_state.called++;
  mock_state.last_ast = ast;
  resp->is_ok = true;
  resp->err_msg = NULL;
  resp->op_type = API_INDEX;
}

bool eng_init(void) { return true; }

void eng_shutdown(void) {}

// --- Setup / Teardown ---

void setUp(void) {
  memset(&mock_state, 0, sizeof(mock_state));
  last_parse_res = NULL;
}

void tearDown(void) {
  if (last_parse_res) {
    parse_free_result(last_parse_res);
    last_parse_res = NULL;
  }
}

// --- Integration Helper ---

static api_response_t *_exec_from_string(const char *input, int64_t ts) {
  // 1. Tokenize
  queue_t *tokens = tok_tokenize((char *)input);

  // 2. Parse
  last_parse_res = parse(tokens);

  // If parser fails on syntax, we can't pass AST to API.
  // Fail the test immediately if this wasn't expected.
  if (!last_parse_res->success) {
    char msg[256];
    snprintf(msg, sizeof(msg), "Parser failed: %s",
             last_parse_res->error_message);
    TEST_FAIL_MESSAGE(msg);
  }

  // 3. Execute API with real AST
  return api_exec(last_parse_res->ast, ts);
}

// --- Tests ---

void test_api_event_success(void) {
  int64_t ts = 1600000000000L;
  api_response_t *resp =
      _exec_from_string("event in:metrics entity:user-1", ts);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok);
  TEST_ASSERT_EQUAL(API_EVENT, resp->op_type);
  TEST_ASSERT_EQUAL(1, mock_state.called);
  TEST_ASSERT_EQUAL_INT64(ts, mock_state.last_ts);
  TEST_ASSERT_NOT_NULL(mock_state.last_ast);

  free_api_response(resp);
}

void test_api_query_success(void) {
  // Parser handles expressions like "val > 10"
  api_response_t *resp =
      _exec_from_string("query in:metrics where:(val > 10)", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok);
  TEST_ASSERT_EQUAL(API_QUERY, resp->op_type);
  TEST_ASSERT_EQUAL(1, mock_state.called);

  free_api_response(resp);
}

void test_api_index_success(void) {
  api_response_t *resp = _exec_from_string("index key:my_field", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok);
  TEST_ASSERT_EQUAL(API_INDEX, resp->op_type);
  TEST_ASSERT_EQUAL(1, mock_state.called);

  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_in(void) {
  // Grammatically valid (Parser OK), Semantically invalid (Validator Fail)
  api_response_t *resp = _exec_from_string("event entity:user-1", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("`in` tag is required", resp->err_msg);

  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_entity(void) {
  api_response_t *resp = _exec_from_string("event in:metrics", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("`entity` tag is required", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);

  free_api_response(resp);
}

void test_api_query_invalid_missing_where(void) {
  api_response_t *resp = _exec_from_string("query in:metrics", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL(0, mock_state.called);

  free_api_response(resp);
}

void test_api_index_invalid_with_in_tag(void) {
  api_response_t *resp = _exec_from_string("index key:field1 in:metrics", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL(0, mock_state.called);

  free_api_response(resp);
}

void test_api_query_invalid_custom_tag(void) {
  // Custom tags (meta:val) are only allowed in EVENT
  api_response_t *resp =
      _exec_from_string("query in:metrics where:(x > 1) meta:val", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL(0, mock_state.called);

  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_custom_tag(void) {
  api_response_t *resp =
      _exec_from_string("event in:metrics entity:user-1 loc:us loc:ca", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Duplicate tag", resp->err_msg);

  free_api_response(resp);
}

void test_api_event_invalid_ast_invalid_container_name(void) {
  api_response_t *resp = _exec_from_string("event in:.db entity:user-1", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);

  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_reserved_tag(void) {
  api_response_t *resp =
      _exec_from_string("event in:metrics in:metrics2 entity:user-1", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);

  free_api_response(resp);
}

void test_api_event_invalid_ast_where_tag(void) {
  api_response_t *resp =
      _exec_from_string("event in:metrics entity:user-1 where:(bad:1)", 0);

  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);

  free_api_response(resp);
}

// Manual Test: Tests API behavior when passed a NULL AST directly.
// We cannot use the parser for this, as the parser always returns a valid
// result or error.
void test_api_event_invalid_ast_null(void) {
  api_response_t *resp = api_exec(NULL, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_api_event_success);
  RUN_TEST(test_api_query_success);
  RUN_TEST(test_api_index_success);
  RUN_TEST(test_api_event_invalid_ast_missing_in);
  RUN_TEST(test_api_event_invalid_ast_missing_entity);
  RUN_TEST(test_api_query_invalid_missing_where);
  RUN_TEST(test_api_index_invalid_with_in_tag);
  RUN_TEST(test_api_query_invalid_custom_tag);
  RUN_TEST(test_api_event_invalid_ast_duplicate_custom_tag);
  RUN_TEST(test_api_event_invalid_ast_invalid_container_name);
  RUN_TEST(test_api_event_invalid_ast_duplicate_reserved_tag);
  RUN_TEST(test_api_event_invalid_ast_where_tag);
  RUN_TEST(test_api_event_invalid_ast_null);

  return UNITY_END();
}