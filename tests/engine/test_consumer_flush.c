#include "core/bitmaps.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/consumer/consumer_flush.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "unity.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

void container_free_db_key_contents(eng_container_db_key_t db_key) {
  (void)db_key;
}

// ============================================================================
// Test Setup & Teardown Helpers
// ============================================================================

static consumer_cache_entry_t *input_entries_head = NULL;

void setUp(void) { input_entries_head = NULL; }

// Helper to free the INPUT data (Cache Entries)
// We must do this manually because the flush operation does a Deep Copy,
// so the inputs are not owned or freed by the flush result.
void _free_input_entry(consumer_cache_entry_t *entry) {
  if (!entry)
    return;

  // Recurse first
  _free_input_entry(entry->dirty_next);

  // Free DB Key Strings
  if (entry->db_key.container_name)
    free(entry->db_key.container_name);
  if (entry->db_key.db_key.type == DB_KEY_STRING &&
      entry->db_key.db_key.key.s) {
    free(entry->db_key.db_key.key.s);
  }

  // Free Payload
  if (entry->val_type == CONSUMER_CACHE_ENTRY_VAL_STR) {
    consumer_cache_str_t *cc_str = atomic_load(&entry->val.cc_str);
    if (cc_str) {
      if (cc_str->str)
        free(cc_str->str);
      free(cc_str);
    }
  } else if (entry->val_type == CONSUMER_CACHE_ENTRY_VAL_BM) {
    consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->val.cc_bitmap);
    if (cc_bm) {
      if (cc_bm->bitmap)
        bitmap_free(cc_bm->bitmap);
      free(cc_bm);
    }
  }

  free(entry);
}

void tearDown(void) { _free_input_entry(input_entries_head); }

// ============================================================================
// Data Creation Helpers
// ============================================================================

consumer_cache_entry_t *_create_entry(const char *c, const char *k) {
  consumer_cache_entry_t *e = calloc(1, sizeof(consumer_cache_entry_t));
  e->db_key.container_name = strdup(c);
  e->db_key.db_key.type = DB_KEY_STRING;
  e->db_key.db_key.key.s = strdup(k);
  e->version = 10;
  e->flush_version = 5;

  // Link for auto-cleanup
  e->dirty_next = input_entries_head;
  input_entries_head = e;
  return e;
}

consumer_cache_entry_t *create_int(const char *c, const char *k, uint32_t val) {
  consumer_cache_entry_t *e = _create_entry(c, k);
  e->val_type = CONSUMER_CACHE_ENTRY_VAL_INT32;
  atomic_init(&e->val.int32, val);
  return e;
}

consumer_cache_entry_t *create_str(const char *c, const char *k,
                                   const char *val) {
  consumer_cache_entry_t *e = _create_entry(c, k);
  e->val_type = CONSUMER_CACHE_ENTRY_VAL_STR;
  consumer_cache_str_t *s = malloc(sizeof(consumer_cache_str_t));
  s->str = strdup(val);
  atomic_init(&e->val.cc_str, s);
  return e;
}

consumer_cache_entry_t *create_bm(const char *c, const char *k) {
  consumer_cache_entry_t *e = _create_entry(c, k);
  e->val_type = CONSUMER_CACHE_ENTRY_VAL_BM;
  consumer_cache_bitmap_t *b = malloc(sizeof(consumer_cache_bitmap_t));
  b->bitmap = bitmap_create();
  bitmap_add(b->bitmap, 123);
  atomic_init(&e->val.cc_bitmap, b);
  return e;
}

// ============================================================================
// Tests
// ============================================================================

void test_flush_prepare_handles_null_or_empty(void) {
  // 1. NULL Head
  consumer_flush_result_t r1 = consumer_flush_prepare(NULL, 10);
  TEST_ASSERT_FALSE(r1.success);
  TEST_ASSERT_EQUAL_STRING("Invalid dirty head", r1.err_msg);

  // 2. Zero Count (Valid, just empty result)
  consumer_cache_entry_t *e = create_int("c", "k", 1);
  consumer_flush_result_t r2 = consumer_flush_prepare(e, 0);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_NULL(r2.msg); // No message allocated
  TEST_ASSERT_EQUAL_UINT32(0, r2.entries_prepared);
}

void test_flush_prepare_int32(void) {
  consumer_cache_entry_t *e = create_int("users", "u:1", 500);
  e->version = 99;

  consumer_flush_result_t res = consumer_flush_prepare(e, 1);

  TEST_ASSERT_TRUE(res.success);
  TEST_ASSERT_NOT_NULL(res.msg);
  TEST_ASSERT_EQUAL_UINT32(1, res.msg->count);

  eng_writer_entry_t *w = &res.msg->entries[0];

  // Verify Values
  TEST_ASSERT_EQUAL(ENG_WRITER_VAL_INT32, w->val_type);
  TEST_ASSERT_EQUAL_UINT32(500, w->val.int32);
  TEST_ASSERT_EQUAL_UINT64(99, w->version);
  TEST_ASSERT_EQUAL_STRING("users", w->db_key.container_name);
  TEST_ASSERT_EQUAL_STRING("u:1", w->db_key.db_key.key.s);

  // Verify Linkage
  TEST_ASSERT_EQUAL_PTR(&e->flush_version, w->flush_version_ptr);

  // Cleanup Output
  consumer_flush_clear_result(res);
}

void test_flush_prepare_deep_copies_strings(void) {
  consumer_cache_entry_t *e = create_str("logs", "l:1", "original");

  consumer_flush_result_t res = consumer_flush_prepare(e, 1);
  TEST_ASSERT_TRUE(res.success);

  eng_writer_entry_t *w = &res.msg->entries[0];

  // 1. Value matches
  TEST_ASSERT_EQUAL_STRING("original", w->val.str_copy);

  // 2. Memory is distinct (Deep Copy)
  consumer_cache_str_t *src_s = atomic_load(&e->val.cc_str);
  TEST_ASSERT_NOT_EQUAL(src_s->str, w->val.str_copy);

  // 3. Container Name is distinct
  TEST_ASSERT_NOT_EQUAL(e->db_key.container_name, w->db_key.container_name);

  consumer_flush_clear_result(res);
}

void test_flush_prepare_deep_copies_bitmaps(void) {
  consumer_cache_entry_t *e = create_bm("idx", "tag:a");

  consumer_flush_result_t res = consumer_flush_prepare(e, 1);
  TEST_ASSERT_TRUE(res.success);

  eng_writer_entry_t *w = &res.msg->entries[0];
  TEST_ASSERT_EQUAL(ENG_WRITER_VAL_BITMAP, w->val_type);

  // 1. Content matches
  TEST_ASSERT_TRUE(bitmap_contains(w->val.bitmap_copy, 123));

  // 2. Memory is distinct (Modify copy, check source)
  bitmap_remove(w->val.bitmap_copy, 123);

  consumer_cache_bitmap_t *src_bm = atomic_load(&e->val.cc_bitmap);
  TEST_ASSERT_TRUE(bitmap_contains(src_bm->bitmap, 123)); // Source unchanged

  consumer_flush_clear_result(res);
}

void test_flush_prepare_mixed_list_with_skips(void) {
  // Setup Chain: Valid Int -> Invalid Str -> Valid BM
  // Logic: The flush loop follows e->dirty_next

  // 1. Valid Int
  consumer_cache_entry_t *e1 = create_int("c1", "k1", 1);

  // 2. Invalid String (NULL internal pointer)
  consumer_cache_entry_t *e2 = _create_entry("c1", "k2");
  e2->val_type = CONSUMER_CACHE_ENTRY_VAL_STR;
  atomic_init(&e2->val.cc_str, NULL); // Error condition

  // 3. Valid Bitmap
  consumer_cache_entry_t *e3 = create_bm("c1", "k3");

  // Link them sequentially for the iterator
  e1->dirty_next = e2;
  e2->dirty_next = e3;
  e3->dirty_next = NULL;

  // Run
  consumer_flush_result_t res = consumer_flush_prepare(e1, 3);

  // Verify
  TEST_ASSERT_TRUE(res.success);
  TEST_ASSERT_EQUAL_UINT32(2, res.entries_prepared);
  TEST_ASSERT_EQUAL_UINT32(1, res.entries_skipped);

  // Output should contain only the 2 valid entries, packed contiguously
  TEST_ASSERT_EQUAL_UINT32(2, res.msg->count);

  // Entry 0 -> Int
  TEST_ASSERT_EQUAL(ENG_WRITER_VAL_INT32, res.msg->entries[0].val_type);
  TEST_ASSERT_EQUAL_STRING("k1", res.msg->entries[0].db_key.db_key.key.s);

  // Entry 1 -> Bitmap (Skipped the string)
  TEST_ASSERT_EQUAL(ENG_WRITER_VAL_BITMAP, res.msg->entries[1].val_type);
  TEST_ASSERT_EQUAL_STRING("k3", res.msg->entries[1].db_key.db_key.key.s);

  consumer_flush_clear_result(res);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_flush_prepare_handles_null_or_empty);
  RUN_TEST(test_flush_prepare_int32);
  RUN_TEST(test_flush_prepare_deep_copies_strings);
  RUN_TEST(test_flush_prepare_deep_copies_bitmaps);
  RUN_TEST(test_flush_prepare_mixed_list_with_skips);
  return UNITY_END();
}