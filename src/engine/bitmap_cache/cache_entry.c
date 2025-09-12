#include "cache_entry.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

void bm_cache_free_entry(bm_cache_value_entry_t *value) {
  if (!value)
    return;
  bitmap_t *bm = atomic_load(&value->bitmap);
  bitmap_free(bm);
  if (value->db_key.type == DB_KEY_STRING) {
    free((char *)value->db_key.key.s);
  }
  free(value->key_entry);
  free(value);
}

bm_cache_value_entry_t *bm_cache_create_val_entry(eng_user_dc_db_type_t db_type,
                                                  db_key_t db_key,
                                                  eng_container_t *dc) {
  bm_cache_value_entry_t *entry =
      calloc(1, sizeof(bm_cache_value_entry_t) + strlen(dc->name) + 1);
  if (!entry)
    return NULL;
  atomic_init(&entry->bitmap, NULL);
  atomic_init(&entry->is_dirty, false);
  atomic_init(&entry->is_flushing, false);
  atomic_init(&entry->evict, false);

  entry->db_type = db_type;
  entry->db_key.type = db_key.type;
  if (db_key.type == DB_KEY_STRING) {
    entry->db_key.key.s = strdup(db_key.key.s);
  }

  strcpy(entry->container_name, dc->name);

  entry->key_entry = NULL;
  return entry;
}