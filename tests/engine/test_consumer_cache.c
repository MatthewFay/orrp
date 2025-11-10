#include "core/bitmaps.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/consumer/consumer_cache_internal.h"
#include "unity.h"
#include <string.h>

// Test fixture
static consumer_cache_t cache;

// Test helper: create a mock bitmap cache entry
static consumer_cache_entry_t *create_test_bitmap_entry(const char *key,
                                                        uint64_t version) {
  consumer_cache_entry_t *entry = calloc(1, sizeof(consumer_cache_entry_t));
  entry->ser_db_key = strdup(key);
  entry->val_type = CONSUMER_CACHE_ENTRY_VAL_BM;

  consumer_cache_bitmap_t *cc_bm = calloc(1, sizeof(consumer_cache_bitmap_t));
  cc_bm->bitmap = bitmap_create();

  atomic_init(&entry->val.cc_bitmap, cc_bm);
  atomic_init(&entry->flush_version, 0);
  entry->version = version;

  entry->lru_next = NULL;
  entry->lru_prev = NULL;
  entry->dirty_next = NULL;

  return entry;
}

// Test helper: create a mock int32 cache entry
static consumer_cache_entry_t *create_test_int32_entry(const char *key,
                                                       uint32_t value) {
  consumer_cache_entry_t *entry = calloc(1, sizeof(consumer_cache_entry_t));
  entry->ser_db_key = strdup(key);
  entry->val_type = CONSUMER_CACHE_ENTRY_VAL_INT32;

  atomic_init(&entry->val.int32, value);
  atomic_init(&entry->flush_version, 0);
  entry->version = 0;

  entry->lru_next = NULL;
  entry->lru_prev = NULL;
  entry->dirty_next = NULL;

  return entry;
}

// Test helper: create a mock string cache entry
static consumer_cache_entry_t *create_test_str_entry(const char *key,
                                                     const char *str_value) {
  consumer_cache_entry_t *entry = calloc(1, sizeof(consumer_cache_entry_t));
  entry->ser_db_key = strdup(key);
  entry->val_type = CONSUMER_CACHE_ENTRY_VAL_STR;

  consumer_cache_str_t *cc_str = calloc(1, sizeof(consumer_cache_str_t));
  cc_str->str = strdup(str_value);

  atomic_init(&entry->val.cc_str, cc_str);
  atomic_init(&entry->flush_version, 0);
  entry->version = 0;

  entry->lru_next = NULL;
  entry->lru_prev = NULL;
  entry->dirty_next = NULL;

  return entry;
}

static void free_test_entry(consumer_cache_entry_t *entry) {
  if (!entry)
    return;

  switch (entry->val_type) {
  case CONSUMER_CACHE_ENTRY_VAL_BM: {
    consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->val.cc_bitmap);
    if (cc_bm) {
      if (cc_bm->bitmap)
        bitmap_free(cc_bm->bitmap);
      free(cc_bm);
    }
    break;
  }
  case CONSUMER_CACHE_ENTRY_VAL_STR: {
    consumer_cache_str_t *cc_str = atomic_load(&entry->val.cc_str);
    if (cc_str) {
      free(cc_str->str);
      free(cc_str);
    }
    break;
  }
  case CONSUMER_CACHE_ENTRY_VAL_INT32:
    // No cleanup needed for int32
    break;
  default:
    break;
  }

  free(entry->ser_db_key);
  free(entry);
}

void container_free_db_key_contents(eng_container_db_key_t *db_key) {
  (void)db_key;
  return;
}

void setUp(void) {
  consumer_cache_config_t config = {.capacity = 100};
  consumer_cache_init(&cache, &config);
}

void tearDown(void) { consumer_cache_destroy(&cache); }

// =============================================================================
// Initialization Tests
// =============================================================================

void test_cache_init_sets_correct_defaults(void) {
  TEST_ASSERT_EQUAL_UINT32(0, cache.n_entries);
  TEST_ASSERT_NULL(cache.lru_head);
  TEST_ASSERT_NULL(cache.lru_tail);
  TEST_ASSERT_NULL(cache.dirty_head);
  TEST_ASSERT_NULL(cache.dirty_tail);
  TEST_ASSERT_EQUAL_UINT32(0, cache.num_dirty_entries);
}

// =============================================================================
// Add/Get Entry Tests
// =============================================================================

void test_add_single_bitmap_entry_succeeds(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 0);

  TEST_ASSERT_TRUE(consumer_cache_add_entry(&cache, "key1", entry));
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);
  TEST_ASSERT_EQUAL_PTR(entry, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(entry, cache.lru_tail);

  free_test_entry(entry);
}

void test_add_single_int32_entry_succeeds(void) {
  consumer_cache_entry_t *entry = create_test_int32_entry("key1", 42);

  TEST_ASSERT_TRUE(consumer_cache_add_entry(&cache, "key1", entry));
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);
  TEST_ASSERT_EQUAL_UINT32(42, atomic_load(&entry->val.int32));

  free_test_entry(entry);
}

void test_add_single_str_entry_succeeds(void) {
  consumer_cache_entry_t *entry = create_test_str_entry("key1", "test_string");

  TEST_ASSERT_TRUE(consumer_cache_add_entry(&cache, "key1", entry));
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);

  consumer_cache_str_t *cc_str = atomic_load(&entry->val.cc_str);
  TEST_ASSERT_EQUAL_STRING("test_string", cc_str->str);

  free_test_entry(entry);
}

void test_get_existing_entry_returns_true(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 0);
  consumer_cache_add_entry(&cache, "key1", entry);

  consumer_cache_entry_t *retrieved = NULL;
  TEST_ASSERT_TRUE(consumer_cache_get_entry(&cache, "key1", &retrieved, true));
  TEST_ASSERT_EQUAL_PTR(entry, retrieved);

  free_test_entry(entry);
}

void test_get_nonexistent_entry_returns_false(void) {
  consumer_cache_entry_t *retrieved = NULL;
  TEST_ASSERT_FALSE(
      consumer_cache_get_entry(&cache, "nonexistent", &retrieved, true));
  TEST_ASSERT_NULL(retrieved);
}

void test_add_multiple_entries_increments_count(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 100);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "value3");

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);
  consumer_cache_add_entry(&cache, "key3", e3);

  TEST_ASSERT_EQUAL_UINT32(3, cache.n_entries);

  consumer_cache_entry_t *retrieved = NULL;
  TEST_ASSERT_TRUE(consumer_cache_get_entry(&cache, "key2", &retrieved, true));
  TEST_ASSERT_EQUAL_PTR(e2, retrieved);
  TEST_ASSERT_EQUAL_UINT32(CONSUMER_CACHE_ENTRY_VAL_INT32, retrieved->val_type);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

void test_add_null_entry_returns_false(void) {
  TEST_ASSERT_FALSE(consumer_cache_add_entry(&cache, "key1", NULL));
  TEST_ASSERT_EQUAL_UINT32(0, cache.n_entries);
}

void test_add_null_key_returns_false(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 0);
  TEST_ASSERT_FALSE(consumer_cache_add_entry(&cache, NULL, entry));
  free_test_entry(entry);
}

// =============================================================================
// LRU Tests
// =============================================================================

void test_lru_head_is_most_recently_added(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 10);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "val3");

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);
  consumer_cache_add_entry(&cache, "key3", e3);

  TEST_ASSERT_EQUAL_PTR(e3, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(e1, cache.lru_tail);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

void test_get_moves_entry_to_lru_head(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 20);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "val3");

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);
  consumer_cache_add_entry(&cache, "key3", e3);

  // e3 is head, e1 is tail
  TEST_ASSERT_EQUAL_PTR(e3, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(e1, cache.lru_tail);

  // Access e1 (tail)
  consumer_cache_entry_t *retrieved = NULL;
  consumer_cache_get_entry(&cache, "key1", &retrieved, true);

  // e1 should now be head
  TEST_ASSERT_EQUAL_PTR(e1, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_tail);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

void test_get_already_head_entry_stays_head(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 30);

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);

  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_head);

  consumer_cache_entry_t *retrieved = NULL;
  consumer_cache_get_entry(&cache, "key2", &retrieved, true);

  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_head);

  free_test_entry(e1);
  free_test_entry(e2);
}

void test_lru_list_linkage_after_multiple_ops(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 40);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "val3");

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);
  consumer_cache_add_entry(&cache, "key3", e3);

  // Access middle entry
  consumer_cache_entry_t *retrieved = NULL;
  consumer_cache_get_entry(&cache, "key2", &retrieved, true);

  // Verify linkage: e2 -> e3 -> e1
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(e3, e2->lru_next);
  TEST_ASSERT_EQUAL_PTR(e1, e3->lru_next);
  TEST_ASSERT_NULL(e1->lru_next);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

// =============================================================================
// Eviction Tests
// =============================================================================

void test_evict_lru_removes_tail(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 50);

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);

  TEST_ASSERT_EQUAL_PTR(e1, cache.lru_tail);

  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);

  TEST_ASSERT_EQUAL_PTR(e1, evicted);
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_tail);
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_head);

  free_test_entry(e1);
  free_test_entry(e2);
}

void test_evict_lru_on_empty_cache_returns_null(void) {
  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);
  TEST_ASSERT_NULL(evicted);
}

void test_evict_lru_refuses_dirty_bitmap_entry(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 5);
  consumer_cache_add_entry(&cache, "key1", entry);

  // flush_version is 0, entry version is 5 -> dirty
  TEST_ASSERT_EQUAL_UINT64(5, entry->version);
  TEST_ASSERT_EQUAL_UINT64(0, atomic_load(&entry->flush_version));

  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);

  TEST_ASSERT_NULL(evicted);
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);

  free_test_entry(entry);
}

void test_evict_lru_refuses_dirty_int32_entry(void) {
  consumer_cache_entry_t *entry = create_test_int32_entry("key1", 100);
  entry->version = 3;
  consumer_cache_add_entry(&cache, "key1", entry);

  // flush_version is 0, entry version is 3 -> dirty
  TEST_ASSERT_EQUAL_UINT64(3, entry->version);
  TEST_ASSERT_EQUAL_UINT64(0, atomic_load(&entry->flush_version));

  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);

  TEST_ASSERT_NULL(evicted);
  TEST_ASSERT_EQUAL_UINT32(1, cache.n_entries);

  free_test_entry(entry);
}

void test_evict_lru_allows_clean_entry(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 5);
  consumer_cache_add_entry(&cache, "key1", entry);

  // Mark as flushed
  entry->version = 5;
  atomic_store(&entry->flush_version, 5);

  TEST_ASSERT_EQUAL_UINT64(5, entry->version);
  TEST_ASSERT_EQUAL_UINT64(5, atomic_load(&entry->flush_version));

  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);

  TEST_ASSERT_EQUAL_PTR(entry, evicted);
  TEST_ASSERT_EQUAL_UINT32(0, cache.n_entries);

  free_test_entry(entry);
}

void test_evict_lru_updates_lru_pointers(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 60);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "val3");

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);
  consumer_cache_add_entry(&cache, "key3", e3);

  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);
  TEST_ASSERT_EQUAL_PTR(e1, evicted);

  // e2 should now be tail
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_tail);
  TEST_ASSERT_NULL(e2->lru_next);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

// =============================================================================
// Dirty List Tests
// =============================================================================

void test_add_entry_to_dirty_list_succeeds(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 0);

  consumer_cache_add_entry_to_dirty_list(&cache, entry);

  TEST_ASSERT_EQUAL_PTR(entry, cache.dirty_head);
  TEST_ASSERT_EQUAL_PTR(entry, cache.dirty_tail);
  TEST_ASSERT_EQUAL_UINT32(1, cache.num_dirty_entries);
  TEST_ASSERT_NULL(entry->dirty_next);

  free_test_entry(entry);
}

void test_add_multiple_entries_to_dirty_list(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 70);
  consumer_cache_entry_t *e3 = create_test_str_entry("key3", "val3");

  consumer_cache_add_entry_to_dirty_list(&cache, e1);
  consumer_cache_add_entry_to_dirty_list(&cache, e2);
  consumer_cache_add_entry_to_dirty_list(&cache, e3);

  TEST_ASSERT_EQUAL_PTR(e1, cache.dirty_head);
  TEST_ASSERT_EQUAL_PTR(e3, cache.dirty_tail);
  TEST_ASSERT_EQUAL_UINT32(3, cache.num_dirty_entries);

  // Check linkage
  TEST_ASSERT_EQUAL_PTR(e2, e1->dirty_next);
  TEST_ASSERT_EQUAL_PTR(e3, e2->dirty_next);
  TEST_ASSERT_NULL(e3->dirty_next);

  free_test_entry(e1);
  free_test_entry(e2);
  free_test_entry(e3);
}

void test_add_already_dirty_entry_is_idempotent(void) {
  consumer_cache_entry_t *entry = create_test_bitmap_entry("key1", 0);

  consumer_cache_add_entry_to_dirty_list(&cache, entry);
  consumer_cache_add_entry_to_dirty_list(&cache, entry);
  consumer_cache_add_entry_to_dirty_list(&cache, entry);

  TEST_ASSERT_EQUAL_UINT32(1, cache.num_dirty_entries);
  TEST_ASSERT_EQUAL_PTR(entry, cache.dirty_head);
  TEST_ASSERT_EQUAL_PTR(entry, cache.dirty_tail);

  free_test_entry(entry);
}

void test_clear_dirty_list_resets_state(void) {
  consumer_cache_entry_t *e1 = create_test_int32_entry("key1", 80);
  consumer_cache_entry_t *e2 = create_test_str_entry("key2", "val2");

  consumer_cache_add_entry_to_dirty_list(&cache, e1);
  consumer_cache_add_entry_to_dirty_list(&cache, e2);

  TEST_ASSERT_EQUAL_UINT32(2, cache.num_dirty_entries);

  consumer_cache_clear_dirty_list(&cache);

  TEST_ASSERT_NULL(cache.dirty_head);
  TEST_ASSERT_NULL(cache.dirty_tail);
  TEST_ASSERT_EQUAL_UINT32(0, cache.num_dirty_entries);

  free_test_entry(e1);
  free_test_entry(e2);
}

void test_add_to_dirty_list_with_null_entry_is_safe(void) {
  consumer_cache_add_entry_to_dirty_list(&cache, NULL);
  TEST_ASSERT_EQUAL_UINT32(0, cache.num_dirty_entries);
}

// =============================================================================
// Integration Tests
// =============================================================================

void test_add_get_evict_workflow(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 90);

  // Add entries
  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);

  // Get e1 to make it MRU
  consumer_cache_entry_t *retrieved = NULL;
  consumer_cache_get_entry(&cache, "key1", &retrieved, true);

  // e2 is now LRU, should be evicted
  consumer_cache_entry_t *evicted = consumer_cache_evict_lru(&cache);
  TEST_ASSERT_EQUAL_PTR(e2, evicted);

  // e1 should still be retrievable
  TEST_ASSERT_TRUE(consumer_cache_get_entry(&cache, "key1", &retrieved, true));

  free_test_entry(e1);
  free_test_entry(e2);
}

void test_dirty_and_lru_list_independence(void) {
  consumer_cache_entry_t *e1 = create_test_bitmap_entry("key1", 0);
  consumer_cache_entry_t *e2 = create_test_int32_entry("key2", 100);

  consumer_cache_add_entry(&cache, "key1", e1);
  consumer_cache_add_entry(&cache, "key2", e2);

  consumer_cache_add_entry_to_dirty_list(&cache, e1);

  // e2 is LRU head, e1 is tail
  TEST_ASSERT_EQUAL_PTR(e2, cache.lru_head);
  TEST_ASSERT_EQUAL_PTR(e1, cache.lru_tail);

  // e1 is dirty head and tail
  TEST_ASSERT_EQUAL_PTR(e1, cache.dirty_head);
  TEST_ASSERT_EQUAL_PTR(e1, cache.dirty_tail);

  free_test_entry(e1);
  free_test_entry(e2);
}

void test_mixed_value_types_in_cache(void) {
  consumer_cache_entry_t *e_bm = create_test_bitmap_entry("bitmap_key", 0);
  consumer_cache_entry_t *e_int = create_test_int32_entry("int_key", 42);
  consumer_cache_entry_t *e_str = create_test_str_entry("str_key", "hello");

  consumer_cache_add_entry(&cache, "bitmap_key", e_bm);
  consumer_cache_add_entry(&cache, "int_key", e_int);
  consumer_cache_add_entry(&cache, "str_key", e_str);

  TEST_ASSERT_EQUAL_UINT32(3, cache.n_entries);

  // Verify each type
  consumer_cache_entry_t *retrieved = NULL;

  consumer_cache_get_entry(&cache, "bitmap_key", &retrieved, true);
  TEST_ASSERT_EQUAL_UINT32(CONSUMER_CACHE_ENTRY_VAL_BM, retrieved->val_type);

  consumer_cache_get_entry(&cache, "int_key", &retrieved, true);
  TEST_ASSERT_EQUAL_UINT32(CONSUMER_CACHE_ENTRY_VAL_INT32, retrieved->val_type);
  TEST_ASSERT_EQUAL_UINT32(42, atomic_load(&retrieved->val.int32));

  consumer_cache_get_entry(&cache, "str_key", &retrieved, true);
  TEST_ASSERT_EQUAL_UINT32(CONSUMER_CACHE_ENTRY_VAL_STR, retrieved->val_type);
  consumer_cache_str_t *cc_str = atomic_load(&retrieved->val.cc_str);
  TEST_ASSERT_EQUAL_STRING("hello", cc_str->str);

  free_test_entry(e_bm);
  free_test_entry(e_int);
  free_test_entry(e_str);
}

// =============================================================================
// Main Test Runner
// =============================================================================

int main(void) {
  UNITY_BEGIN();

  // Initialization
  RUN_TEST(test_cache_init_sets_correct_defaults);

  // Add/Get
  RUN_TEST(test_add_single_bitmap_entry_succeeds);
  RUN_TEST(test_add_single_int32_entry_succeeds);
  RUN_TEST(test_add_single_str_entry_succeeds);
  RUN_TEST(test_get_existing_entry_returns_true);
  RUN_TEST(test_get_nonexistent_entry_returns_false);
  RUN_TEST(test_add_multiple_entries_increments_count);
  RUN_TEST(test_add_null_entry_returns_false);
  RUN_TEST(test_add_null_key_returns_false);

  // LRU
  RUN_TEST(test_lru_head_is_most_recently_added);
  RUN_TEST(test_get_moves_entry_to_lru_head);
  RUN_TEST(test_get_already_head_entry_stays_head);
  RUN_TEST(test_lru_list_linkage_after_multiple_ops);

  // Eviction
  RUN_TEST(test_evict_lru_removes_tail);
  RUN_TEST(test_evict_lru_on_empty_cache_returns_null);
  RUN_TEST(test_evict_lru_refuses_dirty_bitmap_entry);
  RUN_TEST(test_evict_lru_refuses_dirty_int32_entry);
  RUN_TEST(test_evict_lru_allows_clean_entry);
  RUN_TEST(test_evict_lru_updates_lru_pointers);

  // Dirty List
  RUN_TEST(test_add_entry_to_dirty_list_succeeds);
  RUN_TEST(test_add_multiple_entries_to_dirty_list);
  RUN_TEST(test_add_already_dirty_entry_is_idempotent);
  RUN_TEST(test_clear_dirty_list_resets_state);
  RUN_TEST(test_add_to_dirty_list_with_null_entry_is_safe);

  // Integration
  RUN_TEST(test_add_get_evict_workflow);
  RUN_TEST(test_dirty_and_lru_list_independence);
  RUN_TEST(test_mixed_value_types_in_cache);

  return UNITY_END();
}