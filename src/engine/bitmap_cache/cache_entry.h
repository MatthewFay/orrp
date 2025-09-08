#ifndef BM_CACHE_ENTRY_H
#define BM_CACHE_ENTRY_H

#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdatomic.h>

// The key entry stored in the hash table.
typedef struct bm_cache_key_entry_s {
  size_t key_len;
  char key[]; // Flexible array member
} bm_cache_key_entry_t;

// The Bitmap cache value entry. It's a member of a hash map, an LRU list,
// and a dirty list all at once.
typedef struct bm_cache_value_entry_s {
  struct bm_cache_value_entry_s *lru_prev;
  struct bm_cache_value_entry_s *lru_next;

  struct bm_cache_value_entry_s *dirty_next;

  _Atomic(bitmap_t *) *bitmap; // atomic Pointer to the actual bitmap

  atomic_bool is_dirty;    // Has `bitmap` been modified?
  atomic_bool is_flushing; // Prevent double-flush
  atomic_bool evict;       // Mark for post-flush eviction

  eng_user_dc_db_type_t db_type;
  db_key_t db_key;

  bm_cache_key_entry_t *key_entry;
  char container_name[]; // Flexible array member
} bm_cache_value_entry_t;

#endif