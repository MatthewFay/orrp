#include "log/log.h"
#include "unity.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "engine/api.h"
#include "mpack.h"
#include "query/parser.h"
#include "query/tokenizer.h"

// --- Constants ---

static const int POLL_RETRIES = 50;
static const useconds_t POLL_SLEEP_US = 5000;

// --- Helpers ---

// Structure to define expected key-value pairs for assertions
typedef struct {
  const char *key;
  const char *val;
} kv_pair_t;

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
  queue_t *tokens = tok_tokenize((char *)command_string);
  if (!tokens)
    return NULL;

  parse_result_t *parse_res = parse(tokens);
  tok_clear_all(tokens);
  if (!parse_res || parse_res->type == PARSER_OP_TYPE_ERROR) {
    api_response_t *err_res = calloc(1, sizeof(api_response_t));
    err_res->err_msg =
        parse_res ? parse_res->error_message : "Tokenization failed";
    parse_free_result(parse_res);
    return err_res;
  }

  api_response_t *api_res = api_exec(parse_res->ast, 0);
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
  TEST_ASSERT_EQUAL(API_RESP_TYPE_LIST_OBJ, res->resp_type);

  char msg[128];
  snprintf(msg, sizeof(msg), "Expected %u results, got %u", expected_count,
           res->payload.list_obj.count);
  TEST_ASSERT_EQUAL_UINT32_MESSAGE(expected_count, res->payload.list_obj.count,
                                   msg);
}

static void _assert_query_count(const char *container, const char *query_clause,
                                uint32_t expected_count) {
  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s %s", container, query_clause);

  api_response_t *res = NULL;
  for (int i = 0; i < POLL_RETRIES; i++) {
    if (res)
      free_api_response(res);

    res = run_command(cmd);

    if (res && res->is_ok && res->resp_type == API_RESP_TYPE_LIST_OBJ &&
        res->payload.list_obj.count == expected_count) {
      break;
    }

    usleep(POLL_SLEEP_US);
  }

  _assert_count_val(res, expected_count);
  free_api_response(res);
}

// --- MessagePack Content Verification ---

static void _verify_obj_content(api_obj_t *obj, uint32_t expected_id,
                                kv_pair_t *expected_kvs, size_t kv_count) {
  TEST_ASSERT_NOT_NULL(obj->data);
  TEST_ASSERT_GREATER_THAN(0, obj->data_size);

  mpack_reader_t reader;
  mpack_reader_init_data(&reader, obj->data, obj->data_size);

  uint32_t map_count = mpack_expect_map(&reader);
  bool id_found = false;

  bool *kv_found = calloc(kv_count, sizeof(bool));

  for (uint32_t i = 0; i < map_count; i++) {
    if (mpack_reader_error(&reader) != mpack_ok)
      break;

    char key_buf[32];
    mpack_expect_utf8_cstr(&reader, key_buf, sizeof(key_buf));

    if (mpack_reader_error(&reader) != mpack_ok)
      break;

    if (strcmp(key_buf, "id") == 0) {
      uint32_t val = mpack_expect_u32(&reader);
      TEST_ASSERT_EQUAL_UINT32(expected_id, val);
      id_found = true;
    } else if (strcmp(key_buf, "ts") == 0) {
      int64_t ts_val = mpack_expect_i64(&reader);
      TEST_ASSERT_EQUAL_INT64(0, ts_val);
      if (mpack_reader_error(&reader) != mpack_ok)
        break;
    } else {
      char val_buf[64];
      mpack_expect_utf8_cstr(&reader, val_buf, sizeof(val_buf));

      if (mpack_reader_error(&reader) != mpack_ok)
        break;

      // Loop through expected keys to see if we found a match
      for (size_t k = 0; k < kv_count; k++) {
        if (strcmp(expected_kvs[k].key, key_buf) == 0) {
          TEST_ASSERT_EQUAL_STRING(expected_kvs[k].val, val_buf);
          kv_found[k] = true;
          break;
        }
      }
    }
  }

  TEST_ASSERT_EQUAL(mpack_ok, mpack_reader_error(&reader));
  TEST_ASSERT_TRUE_MESSAGE(id_found, "ID field missing in MessagePack");

  for (size_t k = 0; k < kv_count; k++) {
    char msg[128];
    snprintf(msg, sizeof(msg), "Expected key '%s' was not found in object",
             expected_kvs[k].key);
    TEST_ASSERT_TRUE_MESSAGE(kv_found[k], msg);
  }

  free(kv_found);
}

// Decodes just the ID for simple checks
static void _verify_obj_id(api_obj_t *obj, uint32_t expected_id) {
  // Pass empty kvs
  _verify_obj_content(obj, expected_id, NULL, 0);
}

static void _assert_ids(api_response_t *res, uint32_t *expected, size_t count) {
  _assert_count_val(res, count);
  for (size_t i = 0; i < count; i++) {
    // Check Struct ID
    TEST_ASSERT_EQUAL_UINT32(expected[i], res->payload.list_obj.objects[i].id);
    // Check Encoded ID
    _verify_obj_id(&res->payload.list_obj.objects[i], expected[i]);
  }
}

static void _assert_obj_at_index(const char *cmd, uint32_t index,
                                 uint32_t expected_id, kv_pair_t *expected_kvs,
                                 size_t kv_count) {
  api_response_t *res = NULL;

  // Polling loop: Wait until we have enough results to check the specific index
  for (int i = 0; i < POLL_RETRIES; i++) {
    if (res)
      free_api_response(res);

    res = run_command(cmd);

    // We need success, list type, and count greater than the index we are
    // requesting
    if (res && res->is_ok && res->resp_type == API_RESP_TYPE_LIST_OBJ &&
        res->payload.list_obj.count > index) {
      break;
    }

    usleep(POLL_SLEEP_US);
  }

  TEST_ASSERT_NOT_NULL(res);
  TEST_ASSERT_TRUE(res->is_ok);
  TEST_ASSERT_EQUAL(API_RESP_TYPE_LIST_OBJ, res->resp_type);
  TEST_ASSERT_GREATER_THAN(index, res->payload.list_obj.count);

  api_obj_t *obj = &res->payload.list_obj.objects[index];

  // TEST_ASSERT_EQUAL_UINT32(expected_id, obj->id);
  _verify_obj_content(obj, expected_id, expected_kvs, kv_count);

  free_api_response(res);
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
  _safe_remove_db_file("query_content");
  _safe_remove_db_file("query_complex");
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

void test_QUERY_VerifyContent_ShouldReturnCorrectTags(void) {
  const char *c = "query_content";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca env:prod user:matt");

  _write_event(c, "loc:ny env:dev user:john");

  // Query specific item
  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s where:(loc:ca)", c);

  // Expect ID 1 with specific tags
  kv_pair_t expected[] = {{"loc", "ca"}, {"env", "prod"}, {"user", "matt"}};

  // We expect exactly 1 result (ID 1), at index 0.
  _assert_obj_at_index(cmd, 0, 1, expected, 3);
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
  RUN_TEST(test_QUERY_VerifyContent_ShouldReturnCorrectTags);

  int result = UNITY_END();
  usleep(100000);
  suiteTearDown(result);

  return result;
}