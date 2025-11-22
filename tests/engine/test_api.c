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
} mock_eng_event_state_t;

static mock_eng_event_state_t mock_state;

void eng_event(api_response_t *resp, ast_node_t *ast) {
  mock_state.called++;
  mock_state.last_ast = ast;
  // Simulate a successful event
  resp->is_ok = true;
  resp->err_msg = NULL;
  resp->op_type = API_EVENT;
}

void eng_query(api_response_t *r, ast_node_t *ast) {}

bool eng_init(void) { return true; }

void eng_shutdown(void) {}

// Helper to create a minimal valid EVENT AST
static ast_node_t *make_event_ast(const char *container, const char *entity) {
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node(container), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node(entity), false);
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  return cmd;
}

void setUp(void) { memset(&mock_state, 0, sizeof(mock_state)); }

void tearDown(void) {
  // Clean up any resources if needed
}

// --- Tests ---

void test_api_event_success(void) {
  ast_node_t *ast = make_event_ast("metrics", "user-1");
  api_response_t *resp = api_exec(ast);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_TRUE(resp->is_ok);
  TEST_ASSERT_EQUAL(API_EVENT, resp->op_type);
  TEST_ASSERT_EQUAL(1, mock_state.called);
  TEST_ASSERT_NOT_NULL(mock_state.last_ast);
  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_in(void) {
  // Missing 'in' tag
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_missing_entity(void) {
  // Missing 'entity' tag
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_append_node(&cmd->command.tags, in_tag);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_custom_tag(void) {
  // Duplicate custom tag
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_node_t *custom1 = ast_create_custom_tag_node(
      "loc", ast_create_string_literal_node("us"), false);
  ast_node_t *custom2 = ast_create_custom_tag_node(
      "loc", ast_create_string_literal_node("ca"), false);
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  ast_append_node(&cmd->command.tags, custom1);
  ast_append_node(&cmd->command.tags, custom2);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_invalid_container_name(void) {
  // Invalid container name (special char)
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("./db"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_duplicate_reserved_tag(void) {
  // Duplicate reserved tag (e.g., two 'in' tags)
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag1 = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_node_t *in_tag2 = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics2"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_append_node(&cmd->command.tags, in_tag1);
  ast_append_node(&cmd->command.tags, in_tag2);
  ast_append_node(&cmd->command.tags, entity_tag);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_counter_twice(void) {
  // Two counter tags
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_node_t *c1 = ast_create_custom_tag_node(
      "foo", ast_create_string_literal_node("1"), true);
  ast_node_t *c2 = ast_create_custom_tag_node(
      "bar", ast_create_string_literal_node("2"), true);
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  ast_append_node(&cmd->command.tags, c1);
  ast_append_node(&cmd->command.tags, c2);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_where_tag(void) {
  // EVENT should not allow where tag
  ast_node_t *cmd = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user-1"), false);
  ast_node_t *where_tag = ast_create_tag_node(
      AST_KEY_WHERE, ast_create_string_literal_node("should-not-be-here"),
      false);
  ast_append_node(&cmd->command.tags, in_tag);
  ast_append_node(&cmd->command.tags, entity_tag);
  ast_append_node(&cmd->command.tags, where_tag);
  api_response_t *resp = api_exec(cmd);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
  free_api_response(resp);
}

void test_api_event_invalid_ast_null(void) {
  // Null AST
  api_response_t *resp = api_exec(NULL);
  TEST_ASSERT_NOT_NULL(resp);
  TEST_ASSERT_FALSE(resp->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", resp->err_msg);
  TEST_ASSERT_EQUAL(0, mock_state.called);
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
  RUN_TEST(test_api_event_invalid_ast_counter_twice);
  RUN_TEST(test_api_event_invalid_ast_where_tag);
  RUN_TEST(test_api_event_invalid_ast_null);
  return UNITY_END();
}
