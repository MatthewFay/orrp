#ifndef LOCK_STRIPED_HT_H
#define LOCK_STRIPED_HT_H

#include "ck_ht.h"
#include "ck_spinlock.h"
#include <stdbool.h>
#include <stdint.h>

#define NUM_STRIPES 128
#define STRIPE_MASK (NUM_STRIPES - 1)
#define LS_HT_SEED 0
#define INIT_CAPACITY 16384

typedef struct lock_striped_ht_s {
  ck_spinlock_t *lock[NUM_STRIPES];
  ck_ht_t *ht[NUM_STRIPES];
  bool init;
} lock_striped_ht_t;

/**
Lock striped hash table - MPMC */

// Initialization specifies the mode

// Initialize with string keys
bool lock_striped_ht_init_string(lock_striped_ht_t *ht);
// Initialize with uint32_t keys
bool lock_striped_ht_init_int32(lock_striped_ht_t *ht);

// Type-specific operations
bool lock_striped_ht_put_string(lock_striped_ht_t *ht, const char *key,
                                void *value);
bool lock_striped_ht_get_string(lock_striped_ht_t *ht, const char *key,
                                void **val_out);

bool lock_striped_ht_put_int32(lock_striped_ht_t *ht, uint32_t key,
                               void *value);
bool lock_striped_ht_get_int32(lock_striped_ht_t *ht, uint32_t key,
                               void **val_out);

void lock_striped_ht_destroy(lock_striped_ht_t *lock_striped_ht);

#endif