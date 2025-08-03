#include "core/bitmaps.h"
#include "roaring.h"

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

typedef struct {
  size_t roaring_bitmap_size;
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