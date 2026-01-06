#include "engine/cmd_context/cmd_context.h"
#include "query/ast.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

// Globals to hold AST and command context for cleanup
static ast_node_t *g_cmd_ast = NULL;
static cmd_ctx_t *g_cmd_ctx = NULL;

void setUp(void) {
  // This is run before EACH test
  g_cmd_ast = NULL;
  g_cmd_ctx = NULL;
}

void tearDown(void) {
  // This is run after EACH test.
  if (g_cmd_ctx) {
    cmd_context_free(g_cmd_ctx);
    g_cmd_ctx = NULL;
    g_cmd_ast = NULL; // The AST is freed inside cmd_context_free
  } else if (g_cmd_ast) {
    ast_free(g_cmd_ast);
    g_cmd_ast = NULL;
  }
}

// Helper for string literals in tests
// Pass strlen(s) as the length argument to match AST API requirements
static ast_node_t *make_str(const char *s) {
  return ast_create_string_literal_node(s, strlen(s));
}

// --- Test Cases ---

void test_build_cmd_context_simple_event(void) {
  // AST: EVENT in:"metrics" entity:"user1"
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(AST_KW_IN, make_str("metrics"));
  ast_node_t *entity_tag =
      ast_create_tag_node(AST_KW_ENTITY, make_str("user1"));
  ast_append_node(&g_cmd_ast->command.tags, in_tag);
  ast_append_node(&g_cmd_ast->command.tags, entity_tag);

  g_cmd_ctx = build_cmd_context(g_cmd_ast, 0);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->in_tag_value);
  TEST_ASSERT_EQUAL_STRING("metrics",
                           g_cmd_ctx->in_tag_value->literal.string_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->entity_tag_value);
  TEST_ASSERT_EQUAL_STRING("user1",
                           g_cmd_ctx->entity_tag_value->literal.string_value);
  TEST_ASSERT_NULL(g_cmd_ctx->where_tag_value);
  TEST_ASSERT_NULL(g_cmd_ctx->custom_tags_head);
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_custom_tags);
}

void test_build_cmd_context_with_custom_tags(void) {
  // AST: EVENT in:"logs" entity:"req-abc" region:"us-east" status:"ok"
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_IN, make_str("logs")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_ENTITY, make_str("req-abc")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node("region", make_str("us-east")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node("status", make_str("ok")));

  g_cmd_ctx = build_cmd_context(g_cmd_ast, 0);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->in_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->entity_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->custom_tags_head);
  TEST_ASSERT_EQUAL_UINT32(2, g_cmd_ctx->num_custom_tags);

  // Check custom tags list
  ast_node_t *current_tag = g_cmd_ctx->custom_tags_head;
  TEST_ASSERT_EQUAL_STRING("region", current_tag->tag.custom_key);
  TEST_ASSERT_EQUAL_STRING("us-east",
                           current_tag->tag.value->literal.string_value);

  current_tag = current_tag->next;
  TEST_ASSERT_NOT_NULL(current_tag);
  TEST_ASSERT_EQUAL_STRING("status", current_tag->tag.custom_key);
  TEST_ASSERT_EQUAL_STRING("ok", current_tag->tag.value->literal.string_value);

  TEST_ASSERT_NULL(current_tag->next);
}

void test_build_cmd_context_with_counter(void) {
  // AST: EVENT in:"stats" entity:"page-view" path:"/home" +count:1
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_IN, make_str("stats")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_ENTITY, make_str("page-view")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node("path", make_str("/home")));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_custom_tag_node("count", ast_create_number_literal_node(1)));

  g_cmd_ctx = build_cmd_context(g_cmd_ast, 0);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_EQUAL_UINT32(2, g_cmd_ctx->num_custom_tags);
}

void test_build_cmd_context_query(void) {
  // AST: QUERY in:"errors" where:"type:segfault" take:50 cursor:"abc"
  g_cmd_ast = ast_create_command_node(AST_CMD_QUERY, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_IN, make_str("errors")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_WHERE, make_str("type:segfault")));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_tag_node(AST_KW_TAKE, ast_create_number_literal_node(50)));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_CURSOR, make_str("abc")));

  g_cmd_ctx = build_cmd_context(g_cmd_ast, 0);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->in_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->where_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->take_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->cursor_tag_value);
  TEST_ASSERT_NULL(g_cmd_ctx->entity_tag_value);
  TEST_ASSERT_EQUAL_STRING("type:segfault",
                           g_cmd_ctx->where_tag_value->literal.string_value);
  TEST_ASSERT_EQUAL_UINT64(50, g_cmd_ctx->take_tag_value->literal.number_value);
  TEST_ASSERT_EQUAL_STRING("abc",
                           g_cmd_ctx->cursor_tag_value->literal.string_value);
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_custom_tags);
}

void test_build_cmd_context_mixed_tags(void) {
  // AST: EVENT in:"mixed" entity:"test" +c1:1 t2:"v2" +c3:1
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_IN, make_str("mixed")));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KW_ENTITY, make_str("test")));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_custom_tag_node("c1", ast_create_number_literal_node(1)));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node("t2", make_str("v2")));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_custom_tag_node("c3", ast_create_number_literal_node(1)));

  g_cmd_ctx = build_cmd_context(g_cmd_ast, 0);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_EQUAL_UINT32(3, g_cmd_ctx->num_custom_tags);
}

// --- Test Runner ---

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_build_cmd_context_simple_event);
  RUN_TEST(test_build_cmd_context_with_custom_tags);
  RUN_TEST(test_build_cmd_context_with_counter);
  RUN_TEST(test_build_cmd_context_query);
  RUN_TEST(test_build_cmd_context_mixed_tags);
  return UNITY_END();
}