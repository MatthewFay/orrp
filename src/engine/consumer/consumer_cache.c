#include "consumer_cache.h"
#include "engine/consumer/consumer_cache_entry.h"
#include <stdatomic.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// --- Public API Implementations ---

const bitmap_t *consumer_cache_get_bm(consumer_cache_t *cache,
                                      const char *ser_db_key) {
  consumer_cache_entry_t *entry;
  // To avoid locks, query thread does not move cache entry to front of LRU
  if (!consumer_cache_get_entry(cache, ser_db_key, &entry, false)) {
    return NULL;
  }
  consumer_cache_bitmap_t *cc_bm = atomic_load(&entry->cc_bitmap);
  return cc_bm->bitmap;
}
