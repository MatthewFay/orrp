#include "core/bitmaps.h"
#include "roaring.h"
#include <stdint.h>

bitmap_t *bitmap_create() {
  bitmap_t *bm = malloc(sizeof(bitmap_t));
  if (bm == NULL)
    return NULL;
  bm->rb = roaring_bitmap_create();
  if (bm->rb == NULL) {
    free(bm);
    return NULL;
  }
  return bm;
}

void bitmap_add(bitmap_t *bm, uint32_t value) {
  if (bm && bm->rb) {
    roaring_bitmap_add(bm->rb, value);
  }
}

void bitmap_remove(bitmap_t *bm, uint32_t value) {
  if (bm && bm->rb) {
    roaring_bitmap_remove(bm->rb, value);
  }
}

bool bitmap_contains(bitmap_t *bm, uint32_t value) {
  if (bm && bm->rb) {
    return roaring_bitmap_contains(bm->rb, value);
  }
  // TODO: Should not return false on error condition
  return false;
}

void bitmap_free(bitmap_t *bm) {
  if (bm) {
    if (bm->rb) {
      roaring_bitmap_free(bm->rb);
    }
    free(bm);
  }
}

bitmap_t *bitmap_copy(bitmap_t *bm) {
  if (!bm || !bm->rb)
    return NULL;
  bitmap_t *copy = malloc(sizeof(bitmap_t));
  if (copy == NULL)
    return NULL;
  roaring_bitmap_t *copy_rb = roaring_bitmap_copy(bm->rb);
  if (!copy_rb) {
    free(copy);
    return NULL;
  }
  copy->rb = copy_rb;
  return copy;
}

typedef struct {
  size_t roaring_bitmap_size;
  uint64_t version;
} bitmap_serialization_header_t;

void *bitmap_serialize(bitmap_t *bm, size_t *out_size) {

  if (!bm || !out_size || !bm->rb)
    return NULL;

  *out_size = 0;
  size_t roaring_bitmap_size = roaring_bitmap_portable_size_in_bytes(bm->rb);
  size_t total_size =
      sizeof(bitmap_serialization_header_t) + roaring_bitmap_size;

  void *buffer = malloc(total_size);
  if (!buffer)
    return NULL;

  char *p = (char *)buffer;
  bitmap_serialization_header_t header = {.roaring_bitmap_size =
                                              roaring_bitmap_size};
  memcpy(p, &header, sizeof(header));
  p += sizeof(header);

  if (roaring_bitmap_size > 0) {
    roaring_bitmap_portable_serialize(bm->rb, p);
  }

  *out_size = total_size;
  return buffer;
}

bitmap_t *bitmap_deserialize(void *buffer, size_t buffer_size) {
  if (!buffer)
    return NULL;

  bitmap_t *b = malloc(sizeof(bitmap_t));
  if (!b) {
    return NULL;
  }

  const char *p = (const char *)buffer;

  // use header to get sizes
  const bitmap_serialization_header_t *header =
      (const bitmap_serialization_header_t *)p;
  p += sizeof(bitmap_serialization_header_t);

  // Basic sanity check to prevent reading beyond the buffer
  if (sizeof(bitmap_serialization_header_t) + header->roaring_bitmap_size >
      buffer_size) {
    bitmap_free(b);
    return NULL;
  }

  if (header->roaring_bitmap_size > 0) {
    b->rb = roaring_bitmap_portable_deserialize_safe(
        p, header->roaring_bitmap_size);
    if (!b->rb) {
      bitmap_free(b);
      return NULL;
    }
  } else {
    b->rb = NULL;
  }

  return b;
}