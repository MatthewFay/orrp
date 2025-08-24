#include "unity.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h> // For strpbrk
#include <unistd.h>

#include "engine/api.h"
#include "engine/engine.h"
#include "query/parser.h"
#include "query/tokenizer.h"

// --- Globals & Test Helpers ---

static eng_context_t *test_ctx = NULL;

// This helper function simulates the entire server flow from string to response
static api_response_t *run_command(const char *command_string) {
  // 1. Tokenize
  Queue *tokens = tok_tokenize((char *)command_string);
  if (!tokens)
    return NULL;

  // 2. Parse
  parse_result_t *parse_res = parse(tokens);
  tok_clear_all(tokens); // Clean up tokens
  if (!parse_res || parse_res->type == OP_TYPE_ERROR) {
    // Create a mock api_response for parsing errors
    api_response_t *err_res = calloc(1, sizeof(api_response_t));
    err_res->err_msg =
        parse_res ? parse_res->error_message : "Tokenization failed";
    parse_free_result(parse_res);
    return err_res;
  }

  // 3. Execute
  api_response_t *api_res = api_exec(parse_res->ast, test_ctx);
  parse_free_result(parse_res); // Clean up parse result and AST
  return api_res;
}

/**
 * @brief Safely removes a single database file from the 'data' directory.
 */
static void _safe_remove_db_file(const char *container_name) {
  if (container_name == NULL || container_name[0] == '\0' ||
      strcmp(container_name, ".") == 0 || strcmp(container_name, "..") == 0 ||
      strpbrk(container_name, "/\\") != NULL) {
    // Do not proceed with invalid or potentially malicious names
    return;
  }

  char file_path[256];
  snprintf(file_path, sizeof(file_path), "data/%s.mdb", container_name);
  char file_path2[256];
  snprintf(file_path2, sizeof(file_path2), "data/%s.mdb-lock", container_name);

  remove(file_path);
  remove(file_path2);
}

// --- Test Suite Setup & Teardown ---

void setUp(void) {
  // This runs before each test to ensure a clean slate by removing
  // any files that might have been left over from a previous run.
  _safe_remove_db_file("system");
  _safe_remove_db_file("analytics");
  _safe_remove_db_file("logs");
  _safe_remove_db_file("products");
  _safe_remove_db_file("git");
  _safe_remove_db_file("high_volume_test");

  test_ctx = eng_init();
  TEST_ASSERT_NOT_NULL(test_ctx);
}

void tearDown(void) {
  // This runs after each test to clean up the files created.
  eng_close_ctx(test_ctx);

  _safe_remove_db_file("system");
  _safe_remove_db_file("analytics");
  _safe_remove_db_file("logs");
  _safe_remove_db_file("products");
  _safe_remove_db_file("git");
  _safe_remove_db_file("high_volume_test");
}

// --- Test Cases for EVENT Command ---

void test_EVENT_BasicCommand_ShouldSucceed(void) {
  const char *cmd = "EVENT in:analytics entity:user123 loc:sf";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_TRUE(response->is_ok);
  TEST_ASSERT_NULL(response->err_msg);
  TEST_ASSERT_EQUAL(API_EVENT, response->op_type);

  free_api_response(response);
}

void test_EVENT_WithManyTags_ShouldSucceed(void) {
  const char *cmd = "EVENT in:logs entity:service-abc region:us-west-1 "
                    "env:prod level:error code:503";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_TRUE(response->is_ok);

  free_api_response(response);
}

void test_EVENT_WithQuotedStringValues_ShouldSucceed(void) {
  const char *cmd = "EVENT in:products entity:prod-xyz name:\"Widget A\" "
                    "desc:\"A very fine widget\"";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_TRUE(response->is_ok);

  free_api_response(response);
}

void test_EVENT_CaseSensitiveValues_ShouldSucceed(void) {
  const char *cmd =
      "EVENT in:git entity:commit-123 branch:Feature-A user:Alice";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_TRUE(response->is_ok);

  free_api_response(response);
}

void test_EVENT_MissingRequiredTag_IN_ShouldFail(void) {
  const char *cmd = "EVENT entity:user123 loc:sf"; // Missing 'in' tag
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", response->err_msg);

  free_api_response(response);
}

void test_EVENT_MissingRequiredTag_ENTITY_ShouldFail(void) {
  const char *cmd = "EVENT in:analytics loc:sf"; // Missing 'entity' tag
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", response->err_msg);

  free_api_response(response);
}

void test_EVENT_DuplicateReservedTag_ShouldFail(void) {
  const char *cmd = "EVENT in:one in:two entity:user123";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", response->err_msg);

  free_api_response(response);
}

void test_EVENT_DuplicateCustomTag_ShouldFail(void) {
  const char *cmd = "EVENT in:analytics entity:user123 loc:sf loc:ny";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", response->err_msg);

  free_api_response(response);
}

void test_EVENT_InvalidContainerName_ShouldFail(void) {
  // The API layer's `_validate_ast` should catch these before they hit the
  // engine
  const char *cmd1 = "EVENT in:ab entity:u1";     // Too short
  const char *cmd2 = "EVENT in:1start entity:u1"; // Starts with number

  api_response_t *r1 = run_command(cmd1);
  TEST_ASSERT_NOT_NULL(r1);
  TEST_ASSERT_FALSE(r1->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", r1->err_msg);

  api_response_t *r2 = run_command(cmd2);
  TEST_ASSERT_NOT_NULL(r2);
  TEST_ASSERT_FALSE(r2->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", r2->err_msg);

  free_api_response(r1);
  free_api_response(r2);
}

void test_EVENT_SyntaxError_MalformedTag_ShouldFail(void) {
  // This should fail at the tokenizer or parser level
  const char *cmd = "EVENT in:analytics entity user123"; // Missing colon
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  // The exact error message may come from the parser, "Invalid AST" is a good
  // generic check
  TEST_ASSERT_NOT_NULL(response->err_msg);

  free_api_response(response);
}

void test_EVENT_EmptyCommand_ShouldFail(void) {
  const char *cmd = "";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NULL(response); // Tokenizer should return NULL for empty input
}

void test_EVENT_CommandOnly_ShouldFail(void) {
  const char *cmd = "EVENT";
  api_response_t *response = run_command(cmd);

  TEST_ASSERT_NOT_NULL(response);
  TEST_ASSERT_FALSE(response->is_ok);
  TEST_ASSERT_EQUAL_STRING("Invalid AST", response->err_msg);

  free_api_response(response);
}

void test_EVENT_HighVolumeWrites_ShouldSucceed(void) {
  const char *container = "high_volume_test";
  const char *locations[] = {"sf", "ny", "la", "tx"};
  const char *devices[] = {"mobile", "desktop"};

  for (int i = 0; i < 100; i++) {
    char command_buffer[256];

    // Create a unique entity and cycle through different tag values
    const char *current_loc = locations[i % 4];
    const char *current_dev = devices[i % 2];

    snprintf(command_buffer, sizeof(command_buffer),
             "EVENT in:%s entity:user_%d loc:%s device:%s session:%d",
             container, i, current_loc, current_dev, 1000 + i);

    api_response_t *response = run_command(command_buffer);

    // Assert that every single command succeeds
    TEST_ASSERT_NOT_NULL(response);
    TEST_ASSERT_TRUE(response->is_ok);

    free_api_response(response);
  }
}

// --- Main function to run tests ---

int main(void) {
  UNITY_BEGIN();

  // Happy Path Tests
  RUN_TEST(test_EVENT_BasicCommand_ShouldSucceed);
  RUN_TEST(test_EVENT_WithManyTags_ShouldSucceed);
  RUN_TEST(test_EVENT_WithQuotedStringValues_ShouldSucceed);
  RUN_TEST(test_EVENT_CaseSensitiveValues_ShouldSucceed);

  // Load tests
  RUN_TEST(test_EVENT_HighVolumeWrites_ShouldSucceed);

  // Error and Edge Case Tests
  RUN_TEST(test_EVENT_MissingRequiredTag_IN_ShouldFail);
  RUN_TEST(test_EVENT_MissingRequiredTag_ENTITY_ShouldFail);
  RUN_TEST(test_EVENT_DuplicateReservedTag_ShouldFail);
  RUN_TEST(test_EVENT_DuplicateCustomTag_ShouldFail);
  RUN_TEST(test_EVENT_InvalidContainerName_ShouldFail);
  RUN_TEST(test_EVENT_SyntaxError_MalformedTag_ShouldFail);
  RUN_TEST(test_EVENT_EmptyCommand_ShouldFail);
  RUN_TEST(test_EVENT_CommandOnly_ShouldFail);

  return UNITY_END();
}
