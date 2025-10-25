#include "consumer_cache_entry.h"
#include "core/db.h"
#include "engine/container/container_types.h"
#include <stdatomic.h>
#include <stddef.h>
#include <string.h>

void consumer_cache_free_entry(consumer_cache_entry_t *entry) {
  if (!entry)
    return;
  if (entry->db_key.db_key.type == DB_KEY_STRING) {
    free((char *)entry->db_key.db_key.key.s);
  }
  free(entry->ser_db_key);
  free(entry);
}

consumer_cache_entry_t *
consumer_cache_create_entry(eng_container_db_key_t *db_key,
                            const char *ser_db_key,
                            consumer_cache_entry_val_type_t val_type) {
  consumer_cache_entry_t *entry = calloc(1, sizeof(consumer_cache_entry_t));
  if (!entry)
    return NULL;

  switch (val_type) {
  case CONSUMER_CACHE_ENTRY_VAL_BM:
    atomic_init(&entry->val.bitmap, NULL);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_INT32:
    atomic_init(&entry->val.int32, 0);
    break;
  case CONSUMER_CACHE_ENTRY_VAL_STR:
    atomic_init(&entry->val.str, NULL);
    break;
  default:
    free(entry);
    return NULL;
  }

  entry->val_type = val_type;
  entry->version = 0;
  atomic_init(&entry->flush_version, 0);

  entry->db_key = *db_key;
  entry->db_key.container_name = strdup(db_key->container_name);

  if (db_key->db_key.type == DB_KEY_STRING) {
    entry->db_key.db_key.key.s = strdup(db_key->db_key.key.s);
  }
  entry->ser_db_key = strdup(ser_db_key);

  return entry;
}