#include "cache_entry.h"
#include <stdatomic.h>

void bm_cache_free_entry(bm_cache_value_entry_t *value) {
  if (!value)
    return;
  bitmap_t *bm = atomic_load(value->bitmap);
  bitmap_free(bm);
  if (value->db_key.type == DB_KEY_STRING) {
    free((char *)value->db_key.key.s);
  }
  free(value->key_entry);
  free(value);
}
