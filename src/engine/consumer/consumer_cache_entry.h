#ifndef CONSUMER_CACHE_ENTRY_H
#define CONSUMER_CACHE_ENTRY_H

#include "core/bitmaps.h"
#include "engine/container/container.h"
#include <stdatomic.h>
#include <stdint.h>

typedef struct consumer_cache_entry_s {
  struct consumer_cache_entry_s *lru_prev;
  struct consumer_cache_entry_s *lru_next;

  struct consumer_cache_entry_s *dirty_next;

  // TODO: support diff data types
  _Atomic(bitmap_t *) bitmap; // atomic Pointer to the actual bitmap

  _Atomic(uint64_t) flush_version;

  eng_container_db_key_t db_key;

  char *ser_db_key;
} consumer_cache_entry_t;

// Free everything in cache entry EXCEPT bitmap -
// bitmaps are handled by EBR (deferred free)
void consumer_cache_free_entry(consumer_cache_entry_t *entry);

// Create a consumer cache entry. Consumer needs to store bitmap post-creation.
consumer_cache_entry_t *
consumer_cache_create_entry(eng_container_db_key_t *db_key,
                            const char *ser_db_key);
#endif