#include "consumer_cache_entry.h"
#include "core/db.h"
#include "engine/container/container.h"
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

void consumer_cache_free_entry(consumer_cache_entry_t *entry) {
  if (!entry)
    return;
  if (entry->db_key.type == DB_KEY_STRING) {
    free((char *)entry->db_key.key.s);
  }
  free(entry->ser_db_key);
  free(entry);
}

consumer_cache_entry_t *
consumer_cache_create_entry(eng_container_db_key_t *db_key,
                            const char *ser_db_key) {
  consumer_cache_entry_t *entry =
      calloc(1, sizeof(consumer_cache_entry_t) + strlen(dc->name) + 1);
  if (!entry)
    return NULL;
  atomic_init(&entry->bitmap, NULL);
  atomic_init(&entry->flush_version, 0);

  entry->db_type = db_type;
  entry->db_key.type = db_key.type;
  if (db_key.type == DB_KEY_STRING) {
    entry->db_key.key.s = strdup(db_key.key.s);
  }
  entry->ser_db_key = strdup(ser_db_key);

  strcpy(entry->container_name, dc->name);

  return entry;
}