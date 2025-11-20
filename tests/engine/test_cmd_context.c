#include "engine/cmd_context/cmd_context.h"
#include "query/ast.h"
#include "unity.h"
#include <stdlib.h>

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
  // cmd_context_free is responsible for freeing the g_cmd_ctx struct
  // and the g_cmd_ast that it now owns.
  if (g_cmd_ctx) {
    cmd_context_free(g_cmd_ctx);
    g_cmd_ctx = NULL;
    g_cmd_ast = NULL; // The AST is freed inside cmd_context_free
  } else if (g_cmd_ast) {
    // If context building failed, we still need to clean up the AST.
    ast_free(g_cmd_ast);
    g_cmd_ast = NULL;
  }
}

// --- Test Cases ---

void test_build_cmd_context_simple_event(void) {
  // AST: EVENT in:"metrics" entity:"user1"
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_node_t *in_tag = ast_create_tag_node(
      AST_KEY_IN, ast_create_string_literal_node("metrics"), false);
  ast_node_t *entity_tag = ast_create_tag_node(
      AST_KEY_ENTITY, ast_create_string_literal_node("user1"), false);
  ast_append_node(&g_cmd_ast->command.tags, in_tag);
  ast_append_node(&g_cmd_ast->command.tags, entity_tag);

  g_cmd_ctx = build_cmd_context(g_cmd_ast);

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
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_counter_tags);
}

void test_build_cmd_context_with_custom_tags(void) {
  // AST: EVENT in:"logs" entity:"req-abc" region:"us-east" status:"ok"
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_IN,
                                      ast_create_string_literal_node("logs"),
                                      false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_ENTITY,
                                      ast_create_string_literal_node("req-abc"),
                                      false));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_custom_tag_node(
          "region", ast_create_string_literal_node("us-east"), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "status", ast_create_string_literal_node("ok"), false));

  g_cmd_ctx = build_cmd_context(g_cmd_ast);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->in_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->entity_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->custom_tags_head);
  TEST_ASSERT_EQUAL_UINT32(2, g_cmd_ctx->num_custom_tags);
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_counter_tags);

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
                  ast_create_tag_node(AST_KEY_IN,
                                      ast_create_string_literal_node("stats"),
                                      false));
  ast_append_node(
      &g_cmd_ast->command.tags,
      ast_create_tag_node(AST_KEY_ENTITY,
                          ast_create_string_literal_node("page-view"), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "path", ast_create_string_literal_node("/home"), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "count", ast_create_number_literal_node(1), true));

  g_cmd_ctx = build_cmd_context(g_cmd_ast);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_EQUAL_UINT32(2, g_cmd_ctx->num_custom_tags);
  TEST_ASSERT_EQUAL_UINT32(1, g_cmd_ctx->num_counter_tags);
}

void test_build_cmd_context_query(void) {
  // AST: QUERY in:"errors" where:"type:segfault" take:50 cursor:"abc"
  g_cmd_ast = ast_create_command_node(AST_CMD_QUERY, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_IN,
                                      ast_create_string_literal_node("errors"),
                                      false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(
                      AST_KEY_WHERE,
                      ast_create_string_literal_node("type:segfault"), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(
                      AST_KEY_TAKE, ast_create_number_literal_node(50), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_CURSOR,
                                      ast_create_string_literal_node("abc"),
                                      false));

  g_cmd_ctx = build_cmd_context(g_cmd_ast);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->in_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->where_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->take_tag_value);
  TEST_ASSERT_NOT_NULL(g_cmd_ctx->cursor_tag_value);
  TEST_ASSERT_NULL(g_cmd_ctx->entity_tag_value);
  TEST_ASSERT_EQUAL_STRING("type:segfault",
                           g_cmd_ctx->where_tag_value->literal.string_value);
  TEST_ASSERT_EQUAL_UINT32(50, g_cmd_ctx->take_tag_value->literal.number_value);
  TEST_ASSERT_EQUAL_STRING("abc",
                           g_cmd_ctx->cursor_tag_value->literal.string_value);
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_custom_tags);
  TEST_ASSERT_EQUAL_UINT32(0, g_cmd_ctx->num_counter_tags);
}

void test_build_cmd_context_mixed_tags(void) {
  // AST: EVENT in:"mixed" entity:"test" +c1:1 t2:"v2" +c3:1
  g_cmd_ast = ast_create_command_node(AST_CMD_EVENT, NULL);
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_IN,
                                      ast_create_string_literal_node("mixed"),
                                      false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_tag_node(AST_KEY_ENTITY,
                                      ast_create_string_literal_node("test"),
                                      false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "c1", ast_create_number_literal_node(1), true));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "t2", ast_create_string_literal_node("v2"), false));
  ast_append_node(&g_cmd_ast->command.tags,
                  ast_create_custom_tag_node(
                      "c3", ast_create_number_literal_node(1), true));

  g_cmd_ctx = build_cmd_context(g_cmd_ast);

  TEST_ASSERT_NOT_NULL(g_cmd_ctx);
  TEST_ASSERT_EQUAL_UINT32(3, g_cmd_ctx->num_custom_tags);
  TEST_ASSERT_EQUAL_UINT32(2, g_cmd_ctx->num_counter_tags);
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
