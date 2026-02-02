#include "log/log.h"
#include "unity.h"
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "engine/api.h"
#include "mpack.h"
#include "query/parser.h"
#include "query/tokenizer.h"

// --- Constants ---

static const int POLL_RETRIES = 50;
static const useconds_t POLL_SLEEP_US = 5000;

// --- Helpers ---

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

// Get current timestamp in NANOSECONDS
static int64_t _get_now_ns(void) {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return (int64_t)ts.tv_sec * 1000000000LL + ts.tv_nsec;
}

// Core executor that accepts an explicit timestamp (in Nanoseconds)
static api_response_t *run_command_at(const char *command_string, int64_t ts) {
  queue_t *tokens = tok_tokenize((char *)command_string);
  if (!tokens)
    return NULL;

  parse_result_t *parse_res = parse(tokens);
  tok_clear_all(tokens);
  if (!parse_res || !parse_res->success) {
    api_response_t *err_res = calloc(1, sizeof(api_response_t));
    err_res->err_msg =
        parse_res ? parse_res->error_message : "Tokenization failed";
    parse_free_result(parse_res);
    return err_res;
  }

  // Pass the explicit timestamp (ns) to the engine
  api_response_t *api_res = api_exec(parse_res->ast, ts);
  parse_free_result(parse_res);
  return api_res;
}

// Default helper (passes 0 for timestamp, letting engine/OS decide if needed)
static api_response_t *run_command(const char *command_string) {
  return run_command_at(command_string, 0);
}

// Write event with auto-generated timestamp (0)
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

// Write event with EXPLICIT timestamp (in Nanoseconds)
static void _write_event_at(const char *container, const char *tags,
                            int64_t ts_ns) {
  static int ent_counter = 0;
  char buf[512];
  snprintf(buf, sizeof(buf), "EVENT in:%s entity:test_ent_%d %s", container,
           ++ent_counter, tags);

  api_response_t *res = run_command_at(buf, ts_ns);
  TEST_ASSERT_NOT_NULL(res);
  TEST_ASSERT_TRUE_MESSAGE(res->is_ok, res->err_msg);
  free_api_response(res);
}

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

    char key_buf[64];
    mpack_expect_utf8_cstr(&reader, key_buf, sizeof(key_buf));

    if (mpack_reader_error(&reader) != mpack_ok)
      break;

    if (strcmp(key_buf, "id") == 0) {
      uint32_t val = mpack_expect_u32(&reader);
      if (expected_id != 0) {
        TEST_ASSERT_EQUAL_UINT32(expected_id, val);
      }
      id_found = true;
    } else if (strcmp(key_buf, "ts") == 0) {
      // Use expect_i64 for timestamps (safest given previous issues)
      int64_t ts_val = mpack_expect_i64(&reader);
      TEST_ASSERT_TRUE(ts_val >= 0);
      if (mpack_reader_error(&reader) != mpack_ok)
        break;
    } else {
      char val_buf[64];
      mpack_expect_utf8_cstr(&reader, val_buf, sizeof(val_buf));

      if (mpack_reader_error(&reader) != mpack_ok)
        break;

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

static void _verify_obj_id(api_obj_t *obj, uint32_t expected_id) {
  _verify_obj_content(obj, expected_id, NULL, 0);
}

static void _assert_ids(api_response_t *res, uint32_t *expected, size_t count) {
  _assert_count_val(res, count);
  for (size_t i = 0; i < count; i++) {
    TEST_ASSERT_EQUAL_UINT32(expected[i], res->payload.list_obj.objects[i].id);
    _verify_obj_id(&res->payload.list_obj.objects[i], expected[i]);
  }
}

static void _assert_obj_at_index(const char *cmd, uint32_t index,
                                 uint32_t expected_id, kv_pair_t *expected_kvs,
                                 size_t kv_count) {
  api_response_t *res = NULL;

  for (int i = 0; i < POLL_RETRIES; i++) {
    if (res)
      free_api_response(res);

    res = run_command(cmd);

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
  _safe_remove_db_file("query_take");
  _safe_remove_db_file("query_ts");
  _safe_remove_db_file("query_complex_ts");
  return (num_failures > 0) ? 1 : 0;
}

void setUp(void) {}
void tearDown(void) {}

// --- Integration Tests ---

void test_QUERY_BasicFilter_ShouldReturnMatches(void) {
  const char *c = "query_basic";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca type:login");
  _write_event(c, "loc:ny type:login");
  _write_event(c, "loc:ca type:logout");
  _assert_query_count(c, "where:(loc:ca)", 2);
}

void test_QUERY_AndLogic_ShouldReturnIntersection(void) {
  const char *c = "query_and";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca env:prod");
  _write_event(c, "loc:ca env:dev");
  _write_event(c, "loc:ny env:prod");
  _write_event(c, "loc:ca env:prod");
  _assert_query_count(c, "where:(loc:ca AND env:prod)", 2);
}

void test_QUERY_OrLogic_ShouldReturnUnion(void) {
  const char *c = "query_or";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca");
  _write_event(c, "loc:ny");
  _write_event(c, "loc:tx");
  _write_event(c, "loc:ca");
  _assert_query_count(c, "where:(loc:ca OR loc:ny)", 3);
}

void test_QUERY_NestedLogic_ShouldRespectPrecedence(void) {
  const char *c = "query_nested";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca device:phone wifi:false");
  _write_event(c, "loc:tx device:phone wifi:true");
  _write_event(c, "loc:ny device:phone wifi:false");
  _write_event(c, "loc:tx device:desktop wifi:true");

  _assert_query_count(c, "where:(loc:ca OR (device:phone AND wifi:true))", 2);
}

void test_QUERY_NoMatches_ShouldReturnEmptyList(void) {
  const char *c = "query_empty";
  _safe_remove_db_file(c);

  _write_event(c, "loc:ca");
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

  _write_event(c, "a:1 b:1 c:0 d:0");
  _write_event(c, "a:0 b:0 c:1 d:1");
  _write_event(c, "a:1 b:0 c:1 d:0");

  _assert_query_count(c, "where:((a:1 AND b:1) OR (c:1 AND d:1))", 2);
}

void test_QUERY_InvalidSyntax_ShouldFail(void) {
  const char *c = "query_fail";
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

  _write_event(c, "a:1 b:1 c:0 d:0 e:0");
  _write_event(c, "a:0 b:0 c:1 d:1 e:0");
  _write_event(c, "a:0 b:0 c:1 d:0 e:1");
  _write_event(c, "a:0 b:0 c:1 d:0 e:0");
  _write_event(c, "a:1 b:0 c:0 d:0 e:0");

  _assert_query_count(c, "where:((a:1 AND b:1) OR (c:1 AND (d:1 OR e:1)))", 3);
}

void test_QUERY_StrictOrdering_ManualSerialization(void) {
  const char *c = "query_strict";
  _safe_remove_db_file(c);

  _write_event(c, "aid:one");
  usleep(50000);
  _write_event(c, "aid:two");
  usleep(50000);
  _write_event(c, "aid:three");
  usleep(200000);

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

  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s where:(loc:ca)", c);
  kv_pair_t expected[] = {{"loc", "ca"}, {"env", "prod"}, {"user", "matt"}};
  _assert_obj_at_index(cmd, 0, 0, expected, 3);
}

void test_QUERY_Take_ShouldLimitResults(void) {
  const char *c = "query_take";
  _safe_remove_db_file(c);

  _write_event(c, "data:1 pod:a");
  _write_event(c, "data:2 pod:a");
  _write_event(c, "data:3 pod:a");
  _write_event(c, "data:4 pod:a");

  // Verify all 4 exist
  _assert_query_count(c, "where:(pod:a)", 4);

  // Limit check
  char cmd[256];
  snprintf(cmd, sizeof(cmd), "QUERY in:%s take:2 where:(pod:a)", c);

  api_response_t *res = run_command(cmd);
  _assert_count_val(res, 2);
  free_api_response(res);
}

void test_QUERY_TsRange_ShouldFilterByTime(void) {
  const char *c = "query_ts";
  _safe_remove_db_file(c);

  // 1. Get current time in NANOSECONDS
  int64_t now_ns = _get_now_ns();

  // 2. Define event times in NANOSECONDS relative to now
  // early: now
  // late:  now + 2 seconds (in ns)
  int64_t t_early_ns = now_ns;
  int64_t t_late_ns = now_ns + 2000000000LL;

  // 3. Write events passing NANOSECONDS to engine
  _write_event_at(c, "phase:early", t_early_ns);
  _write_event_at(c, "phase:late", t_late_ns);

  // 4. Convert to MILLISECONDS for querying
  int64_t t_early_ms = t_early_ns / 1000000LL;

  // 5. Test Greater Than (Target: Late)
  // Query: ts > (early_ms + 1000ms)
  char query_gt[128];
  snprintf(query_gt, sizeof(query_gt), "where:(ts > %ld)",
           (long)(t_early_ms + 1000));
  _assert_query_count(c, query_gt, 1);

  // 6. Test Less Than (Target: Early)
  // Query: ts < (early_ms + 1000ms)
  char query_lt[128];
  snprintf(query_lt, sizeof(query_lt), "where:(ts < %ld)",
           (long)(t_early_ms + 1000));
  _assert_query_count(c, query_lt, 1);
}

void test_QUERY_ComplexTsLogic_ShouldFilterCorrectly(void) {
  const char *c = "query_complex_ts";
  _safe_remove_db_file(c);

  int64_t start_ns = _get_now_ns();
  int64_t t1_ns = start_ns;                // T0
  int64_t t2_ns = start_ns + 2000000000LL; // T0 + 2s
  int64_t t3_ns = start_ns + 4000000000LL; // T0 + 4s

  _write_event_at(c, "type:a", t1_ns);
  _write_event_at(c, "type:b", t2_ns);
  _write_event_at(c, "type:a", t3_ns);

  // Convert to MS for queries
  int64_t t_start_ms = start_ns / 1000000LL;

  // 1. Time Window: (ts > T0+1s AND ts < T0+3s) -> Expects Middle (t2)
  char q1[128];
  snprintf(q1, sizeof(q1), "where:(ts > %ld AND ts < %ld)",
           (long)(t_start_ms + 1000), (long)(t_start_ms + 3000));
  _assert_query_count(c, q1, 1);

  // 2. Mixed Attributes: (type:a AND ts > T0+3s) -> Expects Last (t3)
  char q2[128];
  snprintf(q2, sizeof(q2), "where:(type:a AND ts > %ld)",
           (long)(t_start_ms + 3000));
  _assert_query_count(c, q2, 1);

  // 3. Split Range (OR): (ts < T0+1s OR ts > T0+3s) -> Expects First & Last
  // (t1, t3)
  char q3[128];
  snprintf(q3, sizeof(q3), "where:(ts < %ld OR ts > %ld)",
           (long)(t_start_ms + 1000), (long)(t_start_ms + 3000));
  _assert_query_count(c, q3, 2);

  // 4. Nested Complex: (type:b OR (type:a AND ts < T0+1s)) -> Expects Middle &
  // First (t2, t1)
  char q4[128];
  snprintf(q4, sizeof(q4), "where:(type:b OR (type:a AND ts < %ld))",
           (long)(t_start_ms + 1000));
  _assert_query_count(c, q4, 2);
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

  RUN_TEST(test_QUERY_Take_ShouldLimitResults);
  RUN_TEST(test_QUERY_TsRange_ShouldFilterByTime);
  RUN_TEST(test_QUERY_ComplexTsLogic_ShouldFilterCorrectly);

  int result = UNITY_END();
  usleep(100000);
  suiteTearDown(result);

  return result;
}