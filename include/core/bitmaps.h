#ifndef CORE_BITMAPS_H
#define CORE_BITMAPS_H

#include <stdbool.h>
#include <stdint.h>

// Forward declaration for your opaque bitmap type
typedef struct bitmap_s bitmap_t;

// Function to create a new bitmap
bitmap_t *bitmap_create();

// Function to add a value to the bitmap
void bitmap_add(bitmap_t *bm, uint32_t value);

// Function to check if a value exists in the bitmap
bool bitmap_contains(bitmap_t *bm, uint32_t value);

// Function to free the bitmap
void bitmap_free(bitmap_t *bm);

#endif // CORE_BITMAPS_H
