#ifndef BM_CACHE_ENTRY_H
#define BM_CACHE_ENTRY_H

#include "core/bitmaps.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdatomic.h>
#include <stdint.h>

// The Bitmap cache entry. It's a member of a hash map, an LRU list,
// and a dirty list all at once.
typedef struct bm_cache_entry_s {
  struct bm_cache_entry_s *lru_prev;
  struct bm_cache_entry_s *lru_next;

  struct bm_cache_entry_s *dirty_next;

  _Atomic(bitmap_t *) bitmap; // atomic Pointer to the actual bitmap

  _Atomic(uint64_t) flush_version;

  eng_user_dc_db_type_t db_type;
  db_key_t db_key;

  char *cache_key;

  char container_name[]; // Flexible array member
} bm_cache_entry_t;

// Free everything in cache entry EXCEPT bitmap -
// bitmaps are handled by EBR (deferred free)
void bm_cache_free_entry(bm_cache_entry_t *entry);

bm_cache_entry_t *bm_cache_create_entry(eng_user_dc_db_type_t db_type,
                                        db_key_t db_key, eng_container_t *dc,
                                        const char *cache_key);
#endif