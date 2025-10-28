#include "consumer_cache_entry.h"
#include "core/db.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <string.h>

void consumer_cache_free_entry(consumer_cache_entry_t *entry) {
  if (!entry)
    return;
  container_free_db_key_contents(&entry->db_key);
  free(entry->ser_db_key);
  // Don't free cc_bitmap or cc_str, EBR will handle this

  free(entry);
}

static consumer_cache_entry_t *
_create_entry(eng_container_db_key_t *db_key, const char *ser_db_key,
              consumer_cache_entry_val_type_t val_type) {
  if (!db_key || !ser_db_key)
    return NULL;
  consumer_cache_entry_t *entry = calloc(1, sizeof(consumer_cache_entry_t));
  if (!entry)
    return NULL;

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

consumer_cache_entry_t *
consumer_cache_create_entry_bitmap(eng_container_db_key_t *db_key,
                                   const char *ser_db_key,
                                   consumer_cache_bitmap_t *cc_bitmap) {
  if (!cc_bitmap)
    return NULL;
  consumer_cache_entry_t *e =
      _create_entry(db_key, ser_db_key, CONSUMER_CACHE_ENTRY_VAL_BM);
  if (!e)
    return NULL;
  atomic_init(&e->val.cc_bitmap, cc_bitmap);
  return e;
}

consumer_cache_entry_t *
consumer_cache_create_entry_str(eng_container_db_key_t *db_key,
                                const char *ser_db_key,
                                consumer_cache_str_t *cc_str) {
  if (!cc_str)
    return NULL;
  consumer_cache_entry_t *e =
      _create_entry(db_key, ser_db_key, CONSUMER_CACHE_ENTRY_VAL_STR);
  if (!e)
    return NULL;
  atomic_init(&e->val.cc_str, cc_str);
  return e;
}

consumer_cache_entry_t *
consumer_cache_create_entry_int32(eng_container_db_key_t *db_key,
                                  const char *ser_db_key, uint32_t value) {
  consumer_cache_entry_t *e =
      _create_entry(db_key, ser_db_key, CONSUMER_CACHE_ENTRY_VAL_INT32);
  if (!e)
    return NULL;
  atomic_init(&e->val.int32, value);
  return e;
}