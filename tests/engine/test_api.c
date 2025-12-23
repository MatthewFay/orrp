#include "engine/api.h"
#include "engine/engine.h"
#include "query/ast.h"
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
} mock_eng_event_state_t;

static mock_eng_event_state_t mock_state;

// Updated Mock Signature to accept arrival_ts
void eng_event(api_response_t *resp, ast_node_t *ast, int64_t arrival_ts) {
  mock_state.called++;
  mock_state.last_ast = ast;
  mock_state.last_ts = arrival_ts;
  // Simulate a successful event
  resp->is_ok = true;
  resp->err_msg = NULL;
  resp->op_type = API_EVENT;
}

void eng_query(api_response_t *r, ast_node_t *ast) {
  (void)r;
  (void)ast;
  // For validation tests, we just check if API made it here
  r->is_ok = true;
  mock_state.called++;
}

bool eng_init(void) { return true; }

void eng_shutdown(void) {}

// Helper to create a minimal valid EVENT AST
static ast_node_t *make_event_ast(const char *container, const char *entity) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node(container, 1));
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node(entity, 1));
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  return cmd;
}

// Helper to create a minimal valid QUERY AST
static ast_node_t *make_query_ast(const char *container,
                                  const char *where_clause) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_QUERY, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node(container, 1));
  ast_node_t *where_tag = ast_create_tag_node(
      AST_KEY_WHERE, ast_create_string_literal_node(where_clause, 1));
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, where_tag);
  return cmd;
}

void setUp(void) { memset(&mock_state, 0, sizeof(mock_state)); }

void tearDown(void) {
  // Clean up any resources if needed
}

// --- Tests ---

void test_api_event_success(void) {
  ast_node_t *ast = make_event_ast("metrics", "user-1");
  int64_t ts = 1600000000000L;
  api_response_t *resp = api_exec(ast, ts);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok);
  TEST_ASSERT_EQUAL(API_EVENT, resp->op_type);
  TEST_ASSERT_EQUAL(1, mock_state.called);
  TEST_ASSERT_EQUAL_INT64(ts, mock_state.last_ts);
  TEST_ASSERT_NOT_NULL(mock_state.last_ast);
  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_in(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1", 1));
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Error: Invalid command", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_entity(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics", 1));
  ast_append_node(&cmd->command.tags, in_tag);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Error: Invalid command", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_custom_tag(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics", 1));
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1, 1", 1));
  ast_node_t *custom1 = ast_create_custom_tag_node(
      "loc", ast_create_string_literal_node("us", 1));
  ast_node_t *custom2 = ast_create_custom_tag_node(
      "loc", ast_create_string_literal_node("ca", 1));
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  ast_append_node(&cmd->command.tags, custom1);
  ast_append_node(&cmd->command.tags, custom2);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Error: Invalid command", resp->err_msg);
  free_api_response(resp);
}

void test_api_event_invalid_ast_invalid_container_name(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("./db", 1));
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1", 1));
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_reserved_tag(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag1 = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics", 1));
  ast_node_t *in_tag2 = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics2", 1));
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1", 1));
  ast_append_node(&cmd->command.tags, in_tag1);
  ast_append_node(&cmd->command.tags, in_tag2);
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

void test_api_event_invalid_ast_where_tag(void) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics", 1));
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1", 1));
  ast_node_t *where_tag = ast_create_tag_node(
      AST_KEY_WHERE, ast_create_string_literal_node("should-not-be-here", 1));
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  ast_append_node(&cmd->command.tags, where_tag);
  api_response_t *resp = api_exec(cmd, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

void test_api_event_invalid_ast_null(void) {
  api_response_t *resp = api_exec(NULL, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

void test_api_query_valid_time_range(void) {
  // Query: IN "logs" WHERE "x=1" FROM 100 TO 200
  ast_node_t *ast = make_query_ast("logs", "x=1");
  ast_node_t *from_tag =
      ast_create_tag_node(AST_KEY_FROM, ast_create_number_literal_node(100));
  ast_node_t *to_tag =
      ast_create_tag_node(AST_KEY_TO, ast_create_number_literal_node(200));

  ast_append_node(&ast->command.tags, from_tag);
  ast_append_node(&ast->command.tags, to_tag);

  api_response_t *resp = api_exec(ast, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok); // Should pass validation
  free_api_response(resp);
}

void test_api_query_invalid_time_range(void) {
  // Query: IN "logs" WHERE "x=1" FROM 300 TO 200 (FROM > TO)
  ast_node_t *ast = make_query_ast("logs", "x=1");
  ast_node_t *from_tag =
      ast_create_tag_node(AST_KEY_FROM, ast_create_number_literal_node(300));
  ast_node_t *to_tag =
      ast_create_tag_node(AST_KEY_TO, ast_create_number_literal_node(200));

  ast_append_node(&ast->command.tags, from_tag);
  ast_append_node(&ast->command.tags, to_tag);

  api_response_t *resp = api_exec(ast, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok); // Should fail validation
  free_api_response(resp);
}

void test_api_event_with_time_range_fail(void) {
  // EVENT cannot have FROM/TO
  ast_node_t *ast = make_event_ast("metrics", "user-1");
  ast_node_t *from_tag =
      ast_create_tag_node(AST_KEY_FROM, ast_create_number_literal_node(100));
  ast_append_node(&ast->command.tags, from_tag);

  api_response_t *resp = api_exec(ast, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok); // Invalid command
  free_api_response(resp);
}

void test_api_query_duplicate_from(void) {
  // Query with two FROM tags
  ast_node_t *ast = make_query_ast("logs", "x=1");
  ast_node_t *from1 =
      ast_create_tag_node(AST_KEY_FROM, ast_create_number_literal_node(100));
  ast_node_t *from2 =
      ast_create_tag_node(AST_KEY_FROM, ast_create_number_literal_node(200));

  ast_append_node(&ast->command.tags, from1);
  ast_append_node(&ast->command.tags, from2);

  api_response_t *resp = api_exec(ast, 0);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  free_api_response(resp);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_api_event_success);
  RUN_TEST(test_api_event_invalid_ast_missing_in);
  RUN_TEST(test_api_event_invalid_ast_missing_entity);
  RUN_TEST(test_api_event_invalid_ast_duplicate_custom_tag);
  RUN_TEST(test_api_event_invalid_ast_invalid_container_name);
  RUN_TEST(test_api_event_invalid_ast_duplicate_reserved_tag);
  RUN_TEST(test_api_event_invalid_ast_where_tag);
  RUN_TEST(test_api_event_invalid_ast_null);

  RUN_TEST(test_api_query_valid_time_range);
  RUN_TEST(test_api_query_invalid_time_range);
  RUN_TEST(test_api_event_with_time_range_fail);
  RUN_TEST(test_api_query_duplicate_from);
  return UNITY_END();
}