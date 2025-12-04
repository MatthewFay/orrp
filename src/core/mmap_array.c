#include "core/mmap_array.h"
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

// Helper to align size to page boundaries (usually 4KB)
static size_t _align_page(size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  return (size + page_size - 1) & ~(page_size - 1);
}

int mmap_array_open(mmap_array_t *arr, mmap_array_config_t *config) {
  if (!arr || !config || config->item_size == 0 || !config->path)
    return -1;

  memset(arr, 0, sizeof(mmap_array_t));
  arr->item_size = config->item_size;
  arr->path = strdup(config->path);

  // Init lock
  if (pthread_rwlock_init(&arr->lock, NULL) != 0) {
    free(arr->path);
    return -1;
  }

  // Open file
  arr->fd = open(arr->path, O_RDWR | O_CREAT, 0644);
  if (arr->fd == -1) {
    pthread_rwlock_destroy(&arr->lock);
    free(arr->path);
    return -1;
  }

  // Check existing size
  struct stat st;
  if (fstat(arr->fd, &st) == -1) {
    close(arr->fd);
    pthread_rwlock_destroy(&arr->lock);
    free(arr->path);
    return -1;
  }

  size_t file_size = st.st_size;
  size_t initial_bytes = config->initial_cap * arr->item_size;

  // If new file or smaller than initial cap, grow it
  if (file_size < initial_bytes) {
    file_size = _align_page(initial_bytes);
    if (ftruncate(arr->fd, file_size) == -1) {
      close(arr->fd);
      pthread_rwlock_destroy(&arr->lock);
      free(arr->path);
      return -1;
    }
  }

  arr->capacity = file_size / arr->item_size;

  // Map memory
  arr->data =
      mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, arr->fd, 0);
  if (arr->data == MAP_FAILED) {
    close(arr->fd);
    pthread_rwlock_destroy(&arr->lock);
    free(arr->path);
    return -1;
  }

  return 0;
}

void mmap_array_close(mmap_array_t *arr) {
  if (!arr)
    return;

  // Acquire write lock to ensure no one is reading during teardown
  pthread_rwlock_wrlock(&arr->lock);

  if (arr->data && arr->data != MAP_FAILED) {
    msync(arr->data, arr->capacity * arr->item_size, MS_SYNC);
    munmap(arr->data, arr->capacity * arr->item_size);
  }

  if (arr->fd != -1) {
    close(arr->fd);
  }

  if (arr->path) {
    free(arr->path);
  }

  pthread_rwlock_unlock(&arr->lock);
  pthread_rwlock_destroy(&arr->lock);
}

int mmap_array_sync(mmap_array_t *arr) {
  if (!arr || !arr->data)
    return -1;
  return msync(arr->data, arr->capacity * arr->item_size, MS_ASYNC);
}

// Internal resize function. Expects Write Lock to be held!
static int _resize_unsafe(mmap_array_t *arr, size_t needed_index) {
  size_t old_size_bytes = arr->capacity * arr->item_size;

  // Growth Strategy: Double size, or match needed size if larger
  size_t new_cap = arr->capacity * 2;
  if (new_cap <= needed_index)
    new_cap = needed_index + 1024; // buffer

  size_t new_size_bytes = _align_page(new_cap * arr->item_size);

  // 1. Sync old data just in case
  msync(arr->data, old_size_bytes, MS_SYNC);

  // 2. Unmap (CRITICAL: Pointers become invalid here)
  munmap(arr->data, old_size_bytes);

  // 3. Resize file
  if (ftruncate(arr->fd, new_size_bytes) == -1) {
    // Emergency recovery: try to remap old size?
    // For now, return error. State is dangerous.
    return -1;
  }

  // 4. Remap
  void *new_ptr = mmap(NULL, new_size_bytes, PROT_READ | PROT_WRITE, MAP_SHARED,
                       arr->fd, 0);
  if (new_ptr == MAP_FAILED) {
    return -1;
  }

  arr->data = new_ptr;
  arr->capacity = new_size_bytes / arr->item_size;

  return 0;
}

int mmap_array_ensure_capacity(mmap_array_t *arr, size_t index) {
  // 1. Optimistic check with Read Lock
  pthread_rwlock_rdlock(&arr->lock);
  if (index < arr->capacity) {
    pthread_rwlock_unlock(&arr->lock);
    return 0;
  }
  pthread_rwlock_unlock(&arr->lock);

  // 2. Needs resize: Acquire Write Lock
  pthread_rwlock_wrlock(&arr->lock);

  // Double check (another thread might have resized while we waited)
  if (index < arr->capacity) {
    pthread_rwlock_unlock(&arr->lock);
    return 0;
  }

  int result = _resize_unsafe(arr, index);

  pthread_rwlock_unlock(&arr->lock);
  return result;
}

int mmap_array_set(mmap_array_t *arr, size_t index, const void *value_ptr) {
  // Ensure capacity handles the locking for the resize check
  if (mmap_array_ensure_capacity(arr, index) != 0) {
    return -1;
  }

  // Now we know index is valid.
  // We only need a READ lock to write to memory (because the memory map
  // exists). The Write lock is only needed if we are changing the map pointer
  // (resizing).
  pthread_rwlock_rdlock(&arr->lock);

  void *dest = mmap_array_get(arr, index);
  if (dest) {
    memcpy(dest, value_ptr, arr->item_size);
  }

  pthread_rwlock_unlock(&arr->lock);
  return (dest != NULL) ? 0 : -1;
}

int mmap_array_read_lock(mmap_array_t *arr) {
  return pthread_rwlock_rdlock(&arr->lock);
}

int mmap_array_write_lock(mmap_array_t *arr) {
  return pthread_rwlock_wrlock(&arr->lock);
}

int mmap_array_unlock(mmap_array_t *arr) {
  return pthread_rwlock_unlock(&arr->lock);
}