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
  consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->cc_bitmap);
  if (cc_bm) {
    if (cc_bm->bitmap)
      bitmap_free(cc_bm->bitmap);
    free(cc_bm);
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

consumer_cache_entry_t *create_bm(const char *c, const char *k) {
  consumer_cache_entry_t *e = _create_entry(c, k);
  consumer_cache_bitmap_t *b = malloc(sizeof(consumer_cache_bitmap_t));
  b->bitmap = bitmap_create();
  bitmap_add(b->bitmap, 123);
  atomic_init(&e->cc_bitmap, b);
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
  consumer_cache_entry_t *e = create_bm("c", "k");
  consumer_flush_result_t r2 = consumer_flush_prepare(e, 0);
  TEST_ASSERT_TRUE(r2.success);
  TEST_ASSERT_NULL(r2.msg); // No message allocated
  TEST_ASSERT_EQUAL_UINT32(0, r2.entries_prepared);
}

void test_flush_prepare_deep_copies_bitmaps(void) {
  consumer_cache_entry_t *e = create_bm("idx", "tag:a");

  consumer_flush_result_t res = consumer_flush_prepare(e, 1);
  TEST_ASSERT_TRUE(res.success);

  eng_writer_entry_t *w = &res.msg->entries[0];

  bitmap_t *deser = bitmap_deserialize(w->value, w->value_size);

  // 1. Content matches
  TEST_ASSERT_TRUE(bitmap_contains(deser, 123));

  // 2. Memory is distinct (Modify copy, check source)
  bitmap_remove(deser, 123);

  consumer_cache_bitmap_t *src_bm = atomic_load(&e->cc_bitmap);
  TEST_ASSERT_TRUE(bitmap_contains(src_bm->bitmap, 123)); // Source unchanged

  consumer_flush_clear_result(res);
}

void test_flush_prepare_list_with_skips(void) {
  consumer_cache_entry_t *e1 = create_bm("c1", "k1");

  consumer_cache_entry_t *e2 = _create_entry("c1", "k2");
  e2->cc_bitmap = NULL;

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

  TEST_ASSERT_EQUAL_STRING("k1", res.msg->entries[0].db_key.db_key.key.s);

  TEST_ASSERT_EQUAL_STRING("k3", res.msg->entries[1].db_key.db_key.key.s);

  consumer_flush_clear_result(res);
}

int main(void) {
  UNITY_BEGIN();
  RUN_TEST(test_flush_prepare_handles_null_or_empty);
  RUN_TEST(test_flush_prepare_deep_copies_bitmaps);
  RUN_TEST(test_flush_prepare_list_with_skips);
  return UNITY_END();
}