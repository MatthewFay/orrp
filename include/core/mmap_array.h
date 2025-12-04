#ifndef MMAP_ARRAY_H
#define MMAP_ARRAY_H

#include <pthread.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/**
 * mmap_array_t
 * * A generic, disk-backed, memory-mapped array that supports automatic
 * resizing. Designed for "Fat Indexes" (e.g., EventID -> EntityID).
 */
typedef struct {
  int fd;                // File descriptor
  char *path;            // File path (needed for persistence/debugging)
  void *data;            // The raw memory pointer (base address)
  size_t item_size;      // Size of a single element (stride)
  size_t capacity;       // Current max number of items allocated in file
  pthread_rwlock_t lock; // Reader-Writer lock for multi-thread safety
} mmap_array_t;

/**
 * Configuration options for opening an array
 */
typedef struct {
  const char *path; // Path to .bin file
  size_t
      item_size; // Size of each element in bytes (e.g., sizeof(uint32_t) or 64)
  size_t initial_cap; // Initial capacity (e.g., 10000)
} mmap_array_config_t;

/**
 * Open or create a memory mapped array.
 * @return 0 on success, -1 on failure
 */
int mmap_array_open(mmap_array_t *arr, mmap_array_config_t *config);

/**
 * Close the array, sync to disk, and free resources.
 */
void mmap_array_close(mmap_array_t *arr);

/**
 * Force synchronization of memory map to disk.
 * The OS does this automatically, but this ensures it happens now.
 */
int mmap_array_sync(mmap_array_t *arr);

/**
 * Ensure the array can hold 'index'. If index >= capacity, the array is
 * resized.
 * * THREAD SAFETY: Acquires WRITE lock internally if resize is needed.
 * Returns 0 on success, -1 on failure.
 */
int mmap_array_ensure_capacity(mmap_array_t *arr, size_t index);

/**
 * Get a pointer to the item at index.
 * * PERFORMANCE: This is inlined for speed.
 * WARNING: The returned pointer is strictly temporary. Do not store it.
 * If a resize happens, this pointer becomes invalid.
 * * THREAD SAFETY: Caller must hold Read Lock (mmap_array_read_lock).
 */
static inline void *mmap_array_get(mmap_array_t *arr, size_t index) {
  if (index >= arr->capacity)
    return NULL;
  return (char *)arr->data + (index * arr->item_size);
}

/**
 * Helper to get a value as a specific type.
 * Usage: uint32_t val = *MMAP_ARRAY_GET_AS(arr, 5, uint32_t);
 */
#define MMAP_ARRAY_GET_AS(arr, idx, type) ((type *)mmap_array_get(arr, idx))

/**
 * Thread Locking Helpers
 * * Usage pattern for READING:
 * mmap_array_read_lock(arr);
 * void *ptr = mmap_array_get(arr, i);
 * // copy data from ptr
 * mmap_array_unlock(arr);
 * * Usage pattern for WRITING (Update existing):
 * mmap_array_read_lock(arr); // Read lock is sufficient if not resizing
 * void *ptr = mmap_array_get(arr, i);
 * // memcpy into ptr
 * mmap_array_unlock(arr);
 * * Usage pattern for WRITING (New Append):
 * // Use mmap_array_set_by_val helper or manually handle write lock
 */
int mmap_array_read_lock(mmap_array_t *arr);
int mmap_array_write_lock(mmap_array_t *arr);
int mmap_array_unlock(mmap_array_t *arr);

/**
 * High-level helper: Set a value at an index.
 * Handles locking and resizing automatically.
 * * @param value_ptr Pointer to the data to copy into the array
 */
int mmap_array_set(mmap_array_t *arr, size_t index, const void *value_ptr);

#endif // MMAP_ARRAY_H