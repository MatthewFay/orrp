#include "consumer_cache_internal.h"
#include "ck_ht.h"
#include "consumer_cache_entry.h"
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>

#define HT_SEED 0

// =============================================================================
// --- Dirty List ---
// =============================================================================

void consumer_cache_add_entry_to_dirty_list(consumer_cache_t *cache,
                                            consumer_cache_entry_t *entry) {
  if (!cache || !entry)
    return;
  if (entry->dirty_next || cache->dirty_head == entry ||
      cache->dirty_tail == entry)
    return; // already dirty
  entry->dirty_next = NULL;

  if (cache->dirty_head) {
    cache->dirty_tail->dirty_next = entry;
    cache->dirty_tail = entry;
  } else {
    cache->dirty_head = entry;
    cache->dirty_tail = entry;
  }

  cache->num_dirty_entries++;
}

void consumer_cache_clear_dirty_list(consumer_cache_t *cache) {
  cache->dirty_head = cache->dirty_tail = NULL;
  cache->num_dirty_entries = 0;
}

// =============================================================================
// --- LRU List Helpers ---
// =============================================================================

// Used to move an existing entry (already in LRU) to front of LRU
static void _lru_move_to_front(consumer_cache_t *cache,
                               consumer_cache_entry_t *entry) {
  if (!entry || entry == cache->lru_head)
    return;
  entry->lru_prev->lru_next = entry->lru_next;
  if (entry == cache->lru_tail) {
    cache->lru_tail = entry->lru_prev;
  } else {
    entry->lru_next->lru_prev = entry->lru_prev;
  }
  entry->lru_prev = NULL;
  entry->lru_next = cache->lru_head;
  cache->lru_head->lru_prev = entry;
  cache->lru_head = entry;
}

// Remove entry from LRU
static void _lru_remove_entry(consumer_cache_t *cache,
                              consumer_cache_entry_t *entry) {
  if (entry->lru_prev) {
    entry->lru_prev->lru_next = entry->lru_next;
  } else {
    cache->lru_head = entry->lru_next;
  }

  if (entry->lru_next) {
    entry->lru_next->lru_prev = entry->lru_prev;
  } else {
    cache->lru_tail = entry->lru_prev;
  }

  entry->lru_prev = entry->lru_next = NULL;
}

// Used to add a new entry to the head of LRU
static void _lru_add_to_head(consumer_cache_t *cache,
                             consumer_cache_entry_t *entry) {
  entry->lru_next = cache->lru_head;
  entry->lru_prev = NULL;

  if (cache->lru_head) {
    cache->lru_head->lru_prev = entry;
  } else {
    cache->lru_tail = entry;
  }

  cache->lru_head = entry;
}

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

bool consumer_cache_init(consumer_cache_t *cache,
                         consumer_cache_config_t *config) {
  cache->config = *config;
  cache->lru_head = cache->lru_tail = NULL;
  cache->dirty_head = NULL;
  cache->dirty_tail = NULL;
  cache->n_entries = 0;
  cache->num_dirty_entries = 0;
  if (!ck_ht_init(&cache->table, CK_HT_MODE_BYTESTRING,
                  NULL, // Initialize with Murmur64 (default)
                  &_my_allocator, cache->config.capacity, HT_SEED)) {
    return false;
  }
  return true;
}

bool consumer_cache_destroy(consumer_cache_t *consumer_cache) {
  // TODO
  (void)consumer_cache;
  return true;
}

bool consumer_cache_get_entry(consumer_cache_t *consumer_cache,
                              const char *ser_db_key,
                              consumer_cache_entry_t **entry_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t entry;

  if (!ser_db_key)
    return false;
  unsigned long key_len = strlen(ser_db_key);
  ck_ht_hash(&hash, &consumer_cache->table, ser_db_key, key_len);
  ck_ht_entry_set(&entry, hash, ser_db_key, key_len, NULL);
  if (!ck_ht_get_spmc(&consumer_cache->table, hash, &entry)) {
    return false;
  }
  *entry_out = (consumer_cache_entry_t *)entry.value;
  _lru_move_to_front(consumer_cache, *entry_out);
  return true;
}

static bool _rem_from_cache_table(consumer_cache_t *cache,
                                  consumer_cache_entry_t *entry) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;
  unsigned long key_len = strlen(entry->ser_db_key);
  ck_ht_hash(&hash, &cache->table, entry->ser_db_key, key_len);
  ck_ht_entry_set(&ck_entry, hash, entry->ser_db_key, key_len, entry);
  if (!ck_ht_remove_spmc(&cache->table, hash, &ck_entry)) {
    return false;
  }
  return true;
}

consumer_cache_entry_t *consumer_cache_evict_lru(consumer_cache_t *cache) {
  if (!cache || !cache->lru_tail)
    return NULL;

  consumer_cache_entry_t *victim = cache->lru_tail;
  uint64_t flush_v = atomic_load(&victim->flush_version);

  if (victim->version != flush_v) {
    return NULL; // Safety check: dirty, can't evict
  }

  if (!_rem_from_cache_table(cache, victim))
    return NULL;
  _lru_remove_entry(cache, victim);
  cache->n_entries--;

  return victim; // return to consumer for EBR
}

bool consumer_cache_add_entry(consumer_cache_t *cache, const char *ser_db_key,
                              consumer_cache_entry_t *entry) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry;

  if (!cache || !ser_db_key || !entry)
    return false;

  uint32_t new_size = cache->n_entries + 1;
  _lru_add_to_head(cache, entry);

  unsigned long key_len = strlen(ser_db_key);
  ck_ht_hash(&hash, &cache->table, ser_db_key, key_len);
  char *key = strdup(ser_db_key);
  ck_ht_entry_set(&ck_entry, hash, key, key_len, entry);
  bool put_r = ck_ht_put_spmc(&cache->table, hash, &ck_entry);
  if (put_r) {
    cache->n_entries = new_size;
    return true;
  }
  _lru_remove_entry(cache, entry);
  free(key);
  return false;
}