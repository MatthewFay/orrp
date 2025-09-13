#ifndef BM_CACHE_ENTRY_H
#define BM_CACHE_ENTRY_H

#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdatomic.h>

// The Bitmap cache entry. It's a member of a hash map, an LRU list,
// and a dirty list all at once.
typedef struct bm_cache_entry_s {
  struct bm_cache_entry_s *lru_prev;
  struct bm_cache_entry_s *lru_next;

  struct bm_cache_entry_s *dirty_next;

  _Atomic(bitmap_t *) bitmap; // atomic Pointer to the actual bitmap

  atomic_bool is_dirty;    // Has `bitmap` been modified?
  atomic_bool is_flushing; // Prevent double-flush
  atomic_bool evict;       // Mark for post-flush eviction

  eng_user_dc_db_type_t db_type;
  db_key_t db_key;

  char container_name[]; // Flexible array member
} bm_cache_entry_t;

void bm_cache_free_entry(bm_cache_entry_t *value);

bm_cache_entry_t *bm_cache_create_entry(eng_user_dc_db_type_t db_type,
                                        db_key_t db_key, eng_container_t *dc);
#endif