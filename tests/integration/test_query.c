#include "log/log.h"
#include "unity.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "engine/api.h"
#include "query/parser.h"
#include "query/tokenizer.h"

// --- Constants ---

// Polling Configuration for Eventual Consistency
static const int POLL_RETRIES = 50;
static const useconds_t POLL_SLEEP_US = 50000; // 50ms * 50 = 2.5s timeout

// --- Helpers ---

static void _safe_remove_db_file(const char *container_name) {
  if (container_name == NULL || container_name[0] == '\0' ||
      strpbrk(container_name, "/\\") != NULL) {
    return;
  }
  char file_path[256];
  snprintf(file_path, sizeof(file_path), "data/%s.mdb", container_name);
  char file_path2[256];
  snprintf(file_path2, sizeof(file_path2), "data/%s.mdb-lock", container_name);
  remove(file_path);
  remove(file_path2);
}

static api_response_t *run_command(const char *command_string) {
  Queue *tokens = tok_tokenize((char *)command_string);
  if (!tokens)
    return NULL;

  parse_result_t *parse_res = parse(tokens);
  tok_clear_all(tokens);
  if (!parse_res || parse_res->type == OP_TYPE_ERROR) {
    api_response_t *err_res = calloc(1, sizeof(api_response_t));
    err_res->err_msg =
        parse_res ? parse_res->error_message : "Tokenization failed";
    parse_free_result(parse_res);
    return err_res;
  }

  api_response_t *api_res = api_exec(parse_res->ast);
  parse_free_result(parse_res);
  return api_res;
}

static void _write_event(const char *container, const char *tags) {
  static int ent_counter = 0;
  char buf[512];
  snprintf(buf, sizeof(buf), "EVENT in:%s entity:test_ent_%d %s", container,
           ++ent_counter, tags);

  api_response_t *res = run_command(buf);
  TEST_ASSERT_NOT_NULL(res);
  TEST_ASSERT_TRUE_MESSAGE(res->is_ok, res->err_msg);
  free_api_response(res);
}

// Helper to check count directly from a response object
static void _assert_count_val(api_response_t *res, uint32_t expected_count) {
  TEST_ASSERT_NOT_NULL(res);
  TEST_ASSERT_TRUE_MESSAGE(res->is_ok, res->err_msg);
  TEST_ASSERT_EQUAL(API_RESP_TYPE_LIST_U32, res->resp_type);

  char msg[128];
  snprintf(msg, sizeof(msg), "Expected %u results, got %u", expected_count,
           res->payload.list_u32.count);
  TEST_ASSERT_EQUAL_UINT32_MESSAGE(expected_count, res->payload.list_u32.count,
                                   msg);
}

/**
 * @brief Polls the query engine until the result count matches expected_count
 * or timeout. This handles the eventual consistency of the worker threads.
 */
static void _assert_query_count(const char *container, const char *query_clause,
                                uint32_t expected_count) {
  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s %s", container, query_clause);

  api_response_t *res = NULL;
  for (int i = 0; i < POLL_RETRIES; i++) {
    if (res)
      free_api_response(res);

    res = run_command(cmd);

    // If we got a valid response and the count matches, we are done
    // (consistency reached)
    if (res && res->is_ok && res->resp_type == API_RESP_TYPE_LIST_U32 &&
        res->payload.list_u32.count == expected_count) {
      break;
    }

    usleep(POLL_SLEEP_US);
  }

  // Final assertion (will fail with helpful message if timeout reached)
  _assert_count_val(res, expected_count);
  free_api_response(res);
}

static void _assert_ids(api_response_t *res, uint32_t *expected, size_t count) {
  _assert_count_val(res, count);
  for (size_t i = 0; i < count; i++) {
    TEST_ASSERT_EQUAL_UINT32(expected[i], res->payload.list_u32.int32s[i]);
  }
}

// --- Setup/Teardown ---

void suiteSetUp(void) {
  int rc = log_global_init("config/zlog.conf");
  if (rc == -1)
    exit(1);
  if (!api_start_eng())
    exit(1);
}

int suiteTearDown(int num_failures) {
  api_stop_eng();
  _safe_remove_db_file("query_basic");
  _safe_remove_db_file("query_and");
  _safe_remove_db_file("query_or");
  _safe_remove_db_file("query_nested");
  _safe_remove_db_file("query_empty");
  _safe_remove_db_file("query_deep");
  _safe_remove_db_file("query_strict");
  return (num_failures > 0) ? 1 : 0;
}

void setUp(void) {}
void tearDown(void) {}

// --- Integration Tests ---

void test_QUERY_BasicFilter_ShouldReturnMatches(void) {
  const char *c = "query_basic";
  _safe_remove_db_file(c);

  // 1. Write Data
  _write_event(c, "loc:ca type:login");
  _write_event(c, "loc:ny type:login");
  _write_event(c, "loc:ca type:logout");

  // 2. Query with polling
  // Query: where:(loc:ca) -> Expect 2
  _assert_query_count(c, "where:(loc:ca)", 2);
}

void test_QUERY_AndLogic_ShouldReturnIntersection(void) {
  const char *c = "query_and";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca env:prod");
  _write_event(c, "loc:ca env:dev");
  _write_event(c, "loc:ny env:prod");
  _write_event(c, "loc:ca env:prod");

  // Query: (loc:ca AND env:prod) -> Matches 1 and 4 -> Expect 2
  _assert_query_count(c, "where:(loc:ca AND env:prod)", 2);
}

void test_QUERY_OrLogic_ShouldReturnUnion(void) {
  const char *c = "query_or";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca");
  _write_event(c, "loc:ny");
  _write_event(c, "loc:tx");
  _write_event(c, "loc:ca");

  // Query: (loc:ca OR loc:ny) -> Matches 1, 2, 4 -> Expect 3
  _assert_query_count(c, "where:(loc:ca OR loc:ny)", 3);
}

void test_QUERY_NestedLogic_ShouldRespectPrecedence(void) {
  const char *c = "query_nested";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca device:phone wifi:false");  // Match (loc:ca)
  _write_event(c, "loc:tx device:phone wifi:true");   // Match (phone + wifi)
  _write_event(c, "loc:ny device:phone wifi:false");  // No match
  _write_event(c, "loc:tx device:desktop wifi:true"); // No match

  // Query: where:(loc:ca OR (device:phone AND wifi:true)) -> Expect 2
  _assert_query_count(c, "where:(loc:ca OR (device:phone AND wifi:true))", 2);
}

void test_QUERY_NoMatches_ShouldReturnEmptyList(void) {
  const char *c = "query_empty";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca");

  // For expected count 0, polling returns immediately.
  // To ensure we aren't just hitting a race where data isn't ready,
  // we can wait for the data to exist via a "control query" or just force a
  // sleep. Since we just want to verify logic, forcing a sleep is safer for
  // "expect 0".
  usleep(200000);

  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s where:(loc:mars)", c);
  api_response_t *res = run_command(cmd);

  _assert_count_val(res, 0);
  free_api_response(res);
}

void test_QUERY_DeepNesting_ShouldSucceed(void) {
  const char *c = "query_deep";
  _safe_remove_db_file(c);

  // Logic: (A and B) OR (C and D)
  _write_event(c, "a:1 b:1 c:0 d:0"); // Match Left
  _write_event(c, "a:0 b:0 c:1 d:1"); // Match Right
  _write_event(c, "a:1 b:0 c:1 d:0"); // No match

  // Query: where:((a:1 AND b:1) OR (c:1 AND d:1)) -> Expect 2
  _assert_query_count(c, "where:((a:1 AND b:1) OR (c:1 AND d:1))", 2);
}

void test_QUERY_InvalidSyntax_ShouldFail(void) {
  const char *c = "query_fail";
  // No setup needed for syntax check

  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s where:loc:ca", c);

  api_response_t *res = run_command(cmd);

  TEST_ASSERT_NOT_NULL(res);
  TEST_ASSERT_FALSE(res->is_ok);
  TEST_ASSERT_NOT_NULL(res->err_msg);

  free_api_response(res);
}

void test_QUERY_ComplexDeepNesting_ShouldSucceed(void) {
  const char *c = "query_complex";
  _safe_remove_db_file(c);

  // Logic: ((A AND B) OR (C AND (D OR E)))
  // Depth: where -> OR -> AND -> OR

  // 1. Match: (A AND B)
  _write_event(c, "a:1 b:1 c:0 d:0 e:0");

  // 2. Match: (C AND D)
  _write_event(c, "a:0 b:0 c:1 d:1 e:0");

  // 3. Match: (C AND E)
  _write_event(c, "a:0 b:0 c:1 d:0 e:1");

  // 4. No Match: (C only - fails inner AND)
  _write_event(c, "a:0 b:0 c:1 d:0 e:0");

  // 5. No Match: (A only - fails outer OR left side)
  _write_event(c, "a:1 b:0 c:0 d:0 e:0");

  // Query: where:((a:1 AND b:1) OR (c:1 AND (d:1 OR e:1))) -> Expect 3 matches
  // (IDs 1, 2, 3)
  _assert_query_count(c, "where:((a:1 AND b:1) OR (c:1 AND (d:1 OR e:1)))", 3);
}

// Special test where we DO verify strict ordering by forcing serialization
void test_QUERY_StrictOrdering_ManualSerialization(void) {
  const char *c = "query_strict";
  _safe_remove_db_file(c);

  // Write Event 1
  _write_event(c, "aid:one");
  usleep(50000); // Wait for worker to persist ID 1

  // Write Event 2
  _write_event(c, "aid:two");
  usleep(50000); // Wait for worker to persist ID 2

  // Write Event 3
  _write_event(c, "aid:three");
  usleep(200000); // Wait for worker to persist ID 3

  char cmd[256];
  snprintf(cmd, sizeof(cmd),
           "QUERY in:%s where:(aid:one OR aid:two OR aid:three)", c);

  api_response_t *res = run_command(cmd);

  uint32_t expected[] = {1, 2, 3};
  _assert_ids(res, expected, 3);

  free_api_response(res);
}

int main(void) {
  suiteSetUp();

  UNITY_BEGIN();

  RUN_TEST(test_QUERY_BasicFilter_ShouldReturnMatches);
  RUN_TEST(test_QUERY_AndLogic_ShouldReturnIntersection);
  RUN_TEST(test_QUERY_OrLogic_ShouldReturnUnion);
  RUN_TEST(test_QUERY_NestedLogic_ShouldRespectPrecedence);
  RUN_TEST(test_QUERY_NoMatches_ShouldReturnEmptyList);
  RUN_TEST(test_QUERY_DeepNesting_ShouldSucceed);
  RUN_TEST(test_QUERY_InvalidSyntax_ShouldFail);
  RUN_TEST(test_QUERY_ComplexDeepNesting_ShouldSucceed);

  RUN_TEST(test_QUERY_StrictOrdering_ManualSerialization);

  int result = UNITY_END();
  usleep(100000);
  suiteTearDown(result);

  return result;
}