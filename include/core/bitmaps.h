#ifndef CORE_BITMAPS_H
#define CORE_BITMAPS_H

#include "ck_epoch.h"
#include "roaring.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct bitmap_s {
  roaring_bitmap_t *rb;
  ck_epoch_entry_t epoch_entry; // Required for epoch reclamation
  uint64_t version;
} bitmap_t;

// Function to create a new bitmap
bitmap_t *bitmap_create(void);

// Function to add a value to the bitmap
void bitmap_add(bitmap_t *bm, uint32_t value);

// Function to remove a value from the bitmap
void bitmap_remove(bitmap_t *bm, uint32_t value);

// Function to check if a value exists in the bitmap
bool bitmap_contains(bitmap_t *bm, uint32_t value);

// Function to free the bitmap
void bitmap_free(bitmap_t *bm);

void *bitmap_serialize(bitmap_t *bm, size_t *out_size);

bitmap_t *bitmap_deserialize(void *buffer, size_t buffer_size);

bitmap_t *bitmap_copy(bitmap_t *bm);

#endif // CORE_BITMAPS_H
