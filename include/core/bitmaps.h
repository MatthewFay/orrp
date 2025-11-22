#ifndef CORE_BITMAPS_H
#define CORE_BITMAPS_H

#include "roaring.h"
#include <stdbool.h>
#include <stdint.h>

typedef struct bitmap_s {
  roaring_bitmap_t *rb;
} bitmap_t;

// Function to create a new bitmap
bitmap_t *bitmap_create(void);

// Function to add a value to the bitmap
void bitmap_add(bitmap_t *bm, uint32_t value);

// Function to remove a value from the bitmap
void bitmap_remove(bitmap_t *bm, uint32_t value);

// Function to check if a value exists in the bitmap
bool bitmap_contains(bitmap_t *bm, uint32_t value);

// returns NULL on error
bitmap_t *bitmap_and(const bitmap_t *bm1, const bitmap_t *bm2);

// returns NULL on error
bitmap_t *bitmap_or(const bitmap_t *bm1, const bitmap_t *bm2);

// returns NULL on error
bitmap_t *bitmap_xor(const bitmap_t *bm1, const bitmap_t *bm2);

// returns NULL on error
bitmap_t *bitmap_not(const bitmap_t *bm1, const bitmap_t *bm2);

void bitmap_and_inplace(bitmap_t *bm1, const bitmap_t *bm2);

void bitmap_or_inplace(bitmap_t *bm1, const bitmap_t *bm2);

void bitmap_xor_inplace(bitmap_t *bm1, const bitmap_t *bm2);

void bitmap_not_inplace(bitmap_t *bm1, const bitmap_t *bm2);

bitmap_t *bitmap_flip(const bitmap_t *bm1, uint64_t range_start,
                      uint64_t range_end);

uint32_t bitmap_get_cardinality(const bitmap_t *bm);

void bitmap_to_uint32_array(const bitmap_t *bm, uint32_t *array);

// Function to free the bitmap
void bitmap_free(bitmap_t *bm);

void *bitmap_serialize(bitmap_t *bm, size_t *out_size);

bitmap_t *bitmap_deserialize(void *buffer, size_t buffer_size);

bitmap_t *bitmap_copy(bitmap_t *bm);

#endif // CORE_BITMAPS_H
