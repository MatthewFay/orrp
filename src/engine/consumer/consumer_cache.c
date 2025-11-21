#include "consumer_cache.h"
#include "ck_epoch.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/consumer/consumer_ebr.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static _Thread_local struct {
  ck_epoch_t epoch;
  ck_epoch_record_t record;
  ck_epoch_section_t section;
  bool registered;
} thread_ebr = {0};

// --- Public API Implementations ---

void consumer_cache_query_begin() {
  if (__builtin_expect(!thread_ebr.registered, 0)) {
    consumer_ebr_init(&thread_ebr.epoch);
    consumer_ebr_register(&thread_ebr.epoch, &thread_ebr.record);
    thread_ebr.registered = true;
  }
  ck_epoch_begin(&thread_ebr.record, &thread_ebr.section);
}

const bitmap_t *consumer_cache_get_bm(consumer_cache_t *cache,
                                      const char *ser_db_key) {
  consumer_cache_entry_t *entry;
  // To avoid locks, query thread does not move cache entry to front of LRU
  if (!consumer_cache_get_entry(cache, ser_db_key, &entry, false)) {
    return NULL;
  }
  consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->val.cc_bitmap);
  return cc_bm->bitmap;
}

uint32_t consumer_cache_get_u32(consumer_cache_t *cache,
                                const char *ser_db_key) {
  consumer_cache_entry_t *entry;
  // To avoid locks, query thread does not move cache entry to front of LRU
  if (!consumer_cache_get_entry(cache, ser_db_key, &entry, false)) {
    return 0;
  }
  uint32_t int32 = atomic_load(&entry->val.int32);
  return int32;
}

void consumer_cache_query_end() {
  ck_epoch_end(&thread_ebr.record, &thread_ebr.section);
}
