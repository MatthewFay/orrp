#include "consumer_cache.h"
#include "ck_epoch.h"
#include "consumer_cache_ebr.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define INIT_TABLE_SIZE 16384
#define TABLE_HASH_SEED 0

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

// --- Public API Implementations ---

bool consumer_cache_init(consumer_cache_t *consumer_cache,
                         consumer_cache_config_t *config) {
  if (!consumer_cache || !config)
    return false;

  consumer_cache->config = *config;
  consumer_cache->is_initialized = false;
  consumer_cache->lru_head = consumer_cache->lru_tail = NULL;
  consumer_cache->dirty_head = NULL;
  consumer_cache->dirty_tail = NULL;
  consumer_cache->n_entries = 0;
  consumer_cache->num_dirty_entries = 0;

  if (!ck_ht_init(&consumer_cache->table, CK_HT_MODE_BYTESTRING,
                  NULL, // Initialize with Murmur64 (default)
                  &_my_allocator, INIT_TABLE_SIZE, TABLE_HASH_SEED)) {
    return false;
  }
  consumer_cache->is_initialized = true;
  return true;
}

void consumer_cache_query_begin(consumer_cache_t *consumer_cache,
                                consumer_cache_handle_t *handle) {
  consumer_cache_ebr_reg();
  // need to fix `consumer_cache_thread_epoch_record`
  ck_epoch_begin(&consumer_cache_thread_epoch_record, &handle->epoch_section);
}

void consumer_cache_query_end(consumer_cache_t *consumer_cache,
                              consumer_cache_handle_t *handle) {
  if (!handle)
    return;
  ck_epoch_end(&consumer_cache_thread_epoch_record, &handle->epoch_section);
}

// TODO
bool consumer_cache_shutdown(consumer_cache_t *consumer_cache) {
  bool success = true;
  consumer_cache->is_initialized = false;
  return success;
}
