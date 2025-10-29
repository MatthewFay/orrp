#include "consumer_cache.h"
#include "ck_epoch.h"
#include "engine/consumer/consumer_cache_entry.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Public API Implementations ---

void consumer_cache_query_begin(ck_epoch_record_t *thread_record,
                                consumer_cache_handle_t *handle) {
  ck_epoch_begin(thread_record, &handle->epoch_section);
}

const bitmap_t *consumer_cache_get_bm(consumer_cache_t *cache,
                                      const char *ser_db_key) {
  consumer_cache_entry_t *entry;
  if (!consumer_cache_get_entry(cache, ser_db_key, &entry)) {
    return NULL;
  }
  consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->val.cc_bitmap);
  return cc_bm->bitmap;
}

void consumer_cache_query_end(ck_epoch_record_t *thread_record,
                              consumer_cache_handle_t *handle) {
  ck_epoch_end(thread_record, &handle->epoch_section);
}
