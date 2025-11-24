#include "core/lock_striped_ht.h"
#include "ck_ht.h"
#include "core/hash.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

void lock_striped_ht_iterate(lock_striped_ht_t *ht, ls_ht_iterator_cb cb,
                             void *ctx) {
  if (!ht || !ht->init || !cb)
    return;

  for (int i = 0; i < NUM_STRIPES; i++) {
    ck_ht_t *stripe_map = ht->ht[i];
    if (!stripe_map)
      continue;

    ck_ht_iterator_t iterator;
    ck_ht_entry_t *entry;

    // Initialize iterator for this stripe
    ck_ht_iterator_init(&iterator);

    // Walk the stripe
    while (ck_ht_next(stripe_map, &iterator, &entry)) {
      // Extract key/value based on the mode (Direct vs ByteString)
      // CK_HT stores keys differently depending on mode.

      void *key_ptr = NULL;
      void *val_ptr = NULL;

      if (stripe_map->mode & CK_HT_MODE_DIRECT) {
        // Int32 keys are stored directly in the key field
        // We pass the address of the key field if you need it,
        // or cast the key itself depending on how you use it.
        // Since your put_int32 passes 'key' by value, let's treat it as such.
        uintptr_t raw_key = ck_ht_entry_key_direct(entry);
        key_ptr = (void *)raw_key; // Cast back to void* for callback
        val_ptr = (void *)ck_ht_entry_value_direct(entry);
      } else {
        // String keys: entry->key points to the actual string (your strdup'd
        // pointer)
        key_ptr = ck_ht_entry_key(entry);
        val_ptr = ck_ht_entry_value(entry);
      }

      cb(key_ptr, val_ptr, ctx);
    }
  }
}

void lock_striped_ht_destroy(lock_striped_ht_t *ht) {
  if (!ht || !ht->init)
    return;
  for (int i = 0; i < NUM_STRIPES; i++) {
    free(ht->lock[i]);
    if (ht->ht[i]) {
      ck_ht_destroy(ht->ht[i]);
    }
  }
  ht->init = false;
}

static void *_hs_malloc(size_t r) { return malloc(r); }
static void _hs_free(void *p, size_t b, bool r) {
  (void)b;
  (void)r;
  free(p);
}
static struct ck_malloc _my_allocator = {.malloc = _hs_malloc,
                                         .free = _hs_free};

static bool _lock_striped_ht_init(lock_striped_ht_t *ht, unsigned int mode) {
  if (!ht)
    return false;
  memset(ht, 0, sizeof(*ht));

  for (int i = 0; i < NUM_STRIPES; i++) {
    ht->lock[i] = malloc(sizeof(ck_spinlock_t));
    if (!ht->lock[i]) {
      lock_striped_ht_destroy(ht);
      return false;
    }
    ck_spinlock_init(ht->lock[i]);
    ht->ht[i] = malloc(sizeof(ck_ht_t));
    if (!ht->ht[i]) {
      lock_striped_ht_destroy(ht);
      return false;
    }
    if (!ck_ht_init(ht->ht[i], mode,
                    NULL, // Initialize with Murmur64 (default)
                    &_my_allocator, INIT_CAPACITY, LS_HT_SEED)) {
      lock_striped_ht_destroy(ht);
      return false;
    }
  }
  ht->init = true;
  return true;
}

bool lock_striped_ht_init_string(lock_striped_ht_t *ht) {
  return _lock_striped_ht_init(ht, CK_HT_MODE_BYTESTRING);
}
bool lock_striped_ht_init_int32(lock_striped_ht_t *ht) {
  return _lock_striped_ht_init(ht, CK_HT_MODE_DIRECT);
}

static int _get_stripe_index(void *key, size_t len) {
  return xxhash64(key, len, 0) & STRIPE_MASK;
}

bool lock_striped_ht_put_string(lock_striped_ht_t *ht, const char *key,
                                void *value) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry = {0};

  if (!ht || !key || !value) {
    return false;
  }

  size_t len = strlen(key);
  int stripe_index = _get_stripe_index((void *)key, len);
  ck_ht_hash(&hash, ht->ht[stripe_index], key, len);
  ck_ht_entry_set(&ck_entry, hash, key, len, value);
  ck_spinlock_lock(ht->lock[stripe_index]);
  bool put_r = ck_ht_put_spmc(ht->ht[stripe_index], hash, &ck_entry);
  ck_spinlock_unlock(ht->lock[stripe_index]);
  return put_r;
}
bool lock_striped_ht_get_string(lock_striped_ht_t *ht, const char *key,
                                void **val_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry = {0};
  ;

  if (!ht || !key || !val_out) {
    return false;
  }
  size_t len = strlen(key);
  int stripe_index = _get_stripe_index((void *)key, len);
  ck_ht_hash(&hash, ht->ht[stripe_index], key, len);
  ck_ht_entry_set(&ck_entry, hash, key, len, NULL);

  if (!ck_ht_get_spmc(ht->ht[stripe_index], hash, &ck_entry)) {
    return false;
  }
  *val_out = (void *)ck_entry.value;

  return true;
}

bool lock_striped_ht_put_int32(lock_striped_ht_t *ht, uint32_t key,
                               void *value) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry = {0};

  if (!ht || !key || !value) {
    return false;
  }

  size_t len = sizeof(uint32_t);
  int stripe_index = _get_stripe_index(&key, len);

  ck_ht_hash_direct(&hash, ht->ht[stripe_index], key);
  ck_ht_entry_set_direct(&ck_entry, hash, key, (uintptr_t)value);

  ck_spinlock_lock(ht->lock[stripe_index]);
  bool put_r = ck_ht_put_spmc(ht->ht[stripe_index], hash, &ck_entry);
  ck_spinlock_unlock(ht->lock[stripe_index]);
  return put_r;
}
bool lock_striped_ht_get_int32(lock_striped_ht_t *ht, uint32_t key,
                               void **val_out) {
  ck_ht_hash_t hash;
  ck_ht_entry_t ck_entry = {0};
  ;

  if (!ht || !key || !val_out) {
    return false;
  }
  size_t len = sizeof(uint32_t);
  int stripe_index = _get_stripe_index(&key, len);

  ck_ht_hash_direct(&hash, ht->ht[stripe_index], key);
  ck_ht_entry_set_direct(&ck_entry, hash, key, 0);

  if (!ck_ht_get_spmc(ht->ht[stripe_index], hash, &ck_entry)) {
    return false;
  }

  *val_out = (void *)ck_ht_entry_value_direct(&ck_entry);

  return true;
}
