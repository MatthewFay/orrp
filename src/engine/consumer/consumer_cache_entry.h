#ifndef CONSUMER_CACHE_ENTRY_H
#define CONSUMER_CACHE_ENTRY_H

#include "ck_epoch.h"
#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include <stdatomic.h>
#include <stdint.h>

typedef struct {
  bitmap_t *bitmap;
  ck_epoch_entry_t epoch_entry;
} consumer_cache_bitmap_t;

typedef struct {
  char *str;
  ck_epoch_entry_t epoch_entry;
} consumer_cache_str_t;

typedef enum {
  CONSUMER_CACHE_ENTRY_VAL_BM = 0,
  CONSUMER_CACHE_ENTRY_VAL_INT32 = 1,
  CONSUMER_CACHE_ENTRY_VAL_STR = 2,
  CONSUMER_CACHE_ENTRY_VAL_UNKNOWN = 3
} consumer_cache_entry_val_type_t;

typedef struct consumer_cache_entry_s {
  struct consumer_cache_entry_s *lru_prev;
  struct consumer_cache_entry_s *lru_next;

  struct consumer_cache_entry_s *dirty_next;

  consumer_cache_entry_val_type_t val_type;

  union {
    _Atomic(consumer_cache_bitmap_t *) cc_bitmap;
    _Atomic(consumer_cache_str_t *) cc_str;
    _Atomic(uint32_t) int32;
  } val;

  _Atomic(uint64_t) flush_version;
  uint64_t version;

  eng_container_db_key_t db_key;

  char *ser_db_key;
} consumer_cache_entry_t;

// Free everything in cache entry EXCEPT bitmap/string -
// bitmaps/strings are handled by EBR (deferred free)
void consumer_cache_free_entry(consumer_cache_entry_t *entry);

consumer_cache_entry_t *
consumer_cache_create_entry_bitmap(eng_container_db_key_t *db_key,
                                   const char *ser_db_key,
                                   consumer_cache_bitmap_t *cc_bitmap);

consumer_cache_entry_t *
consumer_cache_create_entry_str(eng_container_db_key_t *db_key,
                                const char *ser_db_key,
                                consumer_cache_str_t *cc_str);

consumer_cache_entry_t *
consumer_cache_create_entry_int32(eng_container_db_key_t *db_key,
                                  const char *ser_db_key, uint32_t value);
#endif