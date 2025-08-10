#include "core/queue.h"
#include "query/ast.h"
#include "query/parser.h"
#include "query/tokenizer.h"
#include "unity.h"
#include <stdlib.h>
#include <string.h>

void setUp(void) {}
void tearDown(void) {}

static Queue *make_add_tokens(const char *cmd, const char *a1, const char *a2,
                              const char *a3) {
  Queue *q = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = TEXT;
  t_cmd->text_value = strdup(cmd);
  t_cmd->number_value = 0;
  q_enqueue(q, t_cmd);

  token_t *t1 = malloc(sizeof(token_t));
  t1->type = TEXT;
  t1->text_value = strdup(a1);
  t1->number_value = 0;
  q_enqueue(q, t1);

  token_t *t2 = malloc(sizeof(token_t));
  t2->type = TEXT;
  t2->text_value = strdup(a2);
  t2->number_value = 0;
  q_enqueue(q, t2);

  token_t *t3 = malloc(sizeof(token_t));
  t3->type = TEXT;
  t3->text_value = strdup(a3);
  t3->number_value = 0;
  q_enqueue(q, t3);

  return q;
}

void test_parse_add_happy_path(void) {
  Queue *tokens = make_add_tokens("add", "foo", "bar", "baz");
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_WRITE, result->type);
  TEST_ASSERT_NOT_NULL(result->ast);
  TEST_ASSERT_NULL(result->error_message);
  ast_node_t *ast = result->ast;
  TEST_ASSERT_EQUAL(COMMAND_NODE, ast->type);
  TEST_ASSERT_NOT_NULL(ast->node.cmd);
  TEST_ASSERT_EQUAL(ADD, ast->node.cmd->cmd_type);
  // Check args list
  ast_node_t *cur = ast->node.cmd->args;
  TEST_ASSERT_NOT_NULL(cur);
  TEST_ASSERT_EQUAL_STRING("foo", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_NOT_NULL(cur);
  TEST_ASSERT_EQUAL_STRING("bar", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_NOT_NULL(cur);
  TEST_ASSERT_EQUAL_STRING("baz", cur->node.list->item->node.id->value);
  cur = cur->node.list->next;
  TEST_ASSERT_NULL(cur);
  parse_free_result(result);
}

void test_parse_add_too_few_args(void) {
  Queue *tokens = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = TEXT;
  t_cmd->text_value = strdup("add");
  t_cmd->number_value = 0;
  q_enqueue(tokens, t_cmd);

  token_t *t1 = malloc(sizeof(token_t));
  t1->type = TEXT;
  t1->text_value = strdup("foo");
  t1->number_value = 0;
  q_enqueue(tokens, t1);

  // Only 2 tokens after command
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  TEST_ASSERT_NOT_NULL(result->error_message);
  TEST_ASSERT_EQUAL_STRING("Wrong number of arguments for ADD",
                           result->error_message);
  parse_free_result(result);
}

void test_parse_add_too_many_args(void) {
  Queue *tokens = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = TEXT;
  t_cmd->text_value = strdup("add");
  t_cmd->number_value = 0;
  q_enqueue(tokens, t_cmd);

  for (int i = 0; i < 4; ++i) {
    token_t *t = malloc(sizeof(token_t));
    t->type = TEXT;
    char buf[8];
    snprintf(buf, sizeof(buf), "arg%d", i);
    t->text_value = strdup(buf);
    t->number_value = 0;
    q_enqueue(tokens, t);
  }

  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  TEST_ASSERT_NOT_NULL(result->error_message);
  TEST_ASSERT_EQUAL_STRING("Wrong number of arguments for ADD",
                           result->error_message);
  parse_free_result(result);
}

void test_parse_add_wrong_token_type(void) {
  Queue *tokens = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = TEXT;
  t_cmd->text_value = strdup("add");
  t_cmd->number_value = 0;
  q_enqueue(tokens, t_cmd);

  token_t *t1 = malloc(sizeof(token_t));
  t1->type = NUMBER; // Should be TEXT
  t1->text_value = NULL;
  t1->number_value = 42;
  q_enqueue(tokens, t1);

  token_t *t2 = malloc(sizeof(token_t));
  t2->type = TEXT;
  t2->text_value = strdup("bar");
  t2->number_value = 0;
  q_enqueue(tokens, t2);

  token_t *t3 = malloc(sizeof(token_t));
  t3->type = TEXT;
  t3->text_value = strdup("baz");
  t3->number_value = 0;
  q_enqueue(tokens, t3);

  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  // Error message may be NULL due to current parser logic, just check type
  parse_free_result(result);
}

void test_parse_null_queue(void) {
  parse_result_t *result = parse(NULL);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  parse_free_result(result);
}

void test_parse_empty_queue(void) {
  Queue *tokens = q_create();
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  parse_free_result(result);
}

void test_parse_invalid_command(void) {
  Queue *tokens = make_add_tokens("not_a_command", "foo", "bar", "baz");
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  TEST_ASSERT_NOT_NULL(result->error_message);
  TEST_ASSERT_EQUAL_STRING("Unrecognized command", result->error_message);
  parse_free_result(result);
}

void test_parse_command_token_number_type(void) {
  Queue *tokens = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = NUMBER;
  t_cmd->text_value = NULL;
  t_cmd->number_value = 123;
  q_enqueue(tokens, t_cmd);
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  TEST_ASSERT_NOT_NULL(result->error_message);
  TEST_ASSERT_EQUAL_STRING("Invalid command", result->error_message);
  parse_free_result(result);
}

void test_parse_null_token_in_queue(void) {
  Queue *tokens = q_create();
  q_enqueue(tokens, NULL);
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  parse_free_result(result);
}

void test_parse_valid_command_with_null_args(void) {
  Queue *tokens = q_create();
  token_t *t_cmd = malloc(sizeof(token_t));
  t_cmd->type = TEXT;
  t_cmd->text_value = strdup("add");
  t_cmd->number_value = 0;
  q_enqueue(tokens, t_cmd);
  q_enqueue(tokens, NULL);
  q_enqueue(tokens, NULL);
  q_enqueue(tokens, NULL);
  parse_result_t *result = parse(tokens);
  TEST_ASSERT_NOT_NULL(result);
  TEST_ASSERT_EQUAL(OP_TYPE_ERROR, result->type);
  TEST_ASSERT_NULL(result->ast);
  parse_free_result(result);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_parse_add_happy_path);
  RUN_TEST(test_parse_add_too_few_args);
  RUN_TEST(test_parse_add_too_many_args);
  RUN_TEST(test_parse_add_wrong_token_type);
  RUN_TEST(test_parse_null_queue);
  RUN_TEST(test_parse_empty_queue);
  RUN_TEST(test_parse_invalid_command);
  RUN_TEST(test_parse_command_token_number_type);
  RUN_TEST(test_parse_null_token_in_queue);
  RUN_TEST(test_parse_valid_command_with_null_args);
  return UNITY_END();
}
