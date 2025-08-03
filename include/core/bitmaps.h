#ifndef CORE_BITMAPS_H
#define CORE_BITMAPS_H

#include "roaring.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct bitmap_s {
  roaring_bitmap_t *rb;
} bitmap_t;

// Function to create a new bitmap
bitmap_t *bitmap_create();

// Function to add a value to the bitmap
void bitmap_add(bitmap_t *bm, uint32_t value);

// Function to check if a value exists in the bitmap
bool bitmap_contains(bitmap_t *bm, uint32_t value);

// Function to free the bitmap
void bitmap_free(bitmap_t *bm);

void *bitmap_serialize(bitmap_t *bm, size_t *out_size);

bitmap_t *bitmap_deserialize(void *buffer, size_t buffer_size);

#endif // CORE_BITMAPS_H
