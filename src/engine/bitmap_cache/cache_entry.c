#include "cache_entry.h"
#include "core/db.h"
#include "engine/container.h"
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

void bm_cache_free_entry(bm_cache_entry_t *entry) {
  if (!entry)
    return;
  if (entry->db_key.type == DB_KEY_STRING) {
    free((char *)entry->db_key.key.s);
  }
  free(entry->cache_key);
  free(entry);
}

bm_cache_entry_t *bm_cache_create_entry(eng_user_dc_db_type_t db_type,
                                        db_key_t db_key, eng_container_t *dc,
                                        const char *cache_key) {
  bm_cache_entry_t *entry =
      calloc(1, sizeof(bm_cache_entry_t) + strlen(dc->name) + 1);
  if (!entry)
    return NULL;
  atomic_init(&entry->bitmap, NULL);
  atomic_init(&entry->flush_version, 0);

  entry->db_type = db_type;
  entry->db_key.type = db_key.type;
  if (db_key.type == DB_KEY_STRING) {
    entry->db_key.key.s = strdup(db_key.key.s);
  }
  entry->cache_key = strdup(cache_key);

  strcpy(entry->container_name, dc->name);

  return entry;
}