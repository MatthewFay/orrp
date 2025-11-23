#include "core/bitmaps.h"
#include "roaring.h"
#include <stdbool.h>
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

typedef roaring_bitmap_t *(*bitmap_op_fn)(const roaring_bitmap_t *,
                                          const roaring_bitmap_t *);

static bitmap_t *_apply_bitmap_op(const bitmap_t *bm1, const bitmap_t *bm2,
                                  bitmap_op_fn bm_op_fn) {
  if (!(bm1 && bm1->rb && bm2 && bm2->rb)) {
    return NULL;
  }
  bitmap_t *r = malloc(sizeof(bitmap_t));
  if (!r) {
    return NULL;
  }
  r->rb = bm_op_fn(bm1->rb, bm2->rb);
  if (!r->rb) {
    free(r);
    return NULL;
  }
  return r;
}

bitmap_t *bitmap_and(const bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_op(bm1, bm2, roaring_bitmap_and);
}

bitmap_t *bitmap_or(const bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_op(bm1, bm2, roaring_bitmap_or);
}

bitmap_t *bitmap_xor(const bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_op(bm1, bm2, roaring_bitmap_xor);
}

bitmap_t *bitmap_not(const bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_op(bm1, bm2, roaring_bitmap_andnot);
}

typedef void (*bitmap_inplace_op_fn)(roaring_bitmap_t *,
                                     const roaring_bitmap_t *);

static void _apply_bitmap_inplace_op(bitmap_t *bm1, const bitmap_t *bm2,
                                     bitmap_inplace_op_fn bm_inplace_op_fn) {
  if (!(bm1 && bm1->rb && bm2 && bm2->rb)) {
    return;
  }
  bm_inplace_op_fn(bm1->rb, bm2->rb);
}

void bitmap_and_inplace(bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_inplace_op(bm1, bm2, roaring_bitmap_and_inplace);
}

void bitmap_or_inplace(bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_inplace_op(bm1, bm2, roaring_bitmap_or_inplace);
}

void bitmap_xor_inplace(bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_inplace_op(bm1, bm2, roaring_bitmap_xor_inplace);
}

void bitmap_not_inplace(bitmap_t *bm1, const bitmap_t *bm2) {
  return _apply_bitmap_inplace_op(bm1, bm2, roaring_bitmap_andnot_inplace);
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
  uint64_t roaring_bitmap_size;
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

  // Initialize the pointer to NULL immediately so bitmap_free is safe
  b->rb = NULL;

  const char *p = (const char *)buffer;

  if (buffer_size < sizeof(bitmap_serialization_header_t)) {
    bitmap_free(b);
    return NULL;
  }

  // Use memcpy to read header (avoids alignment/padding issues)
  bitmap_serialization_header_t header;
  memcpy(&header, p, sizeof(header));

  p += sizeof(bitmap_serialization_header_t);

  size_t expected_total_size =
      sizeof(bitmap_serialization_header_t) + header.roaring_bitmap_size;

  if (expected_total_size > buffer_size) {
    bitmap_free(b); // safe because b->rb is NULL
    return NULL;
  }

  if (header.roaring_bitmap_size > 0) {
    b->rb =
        roaring_bitmap_portable_deserialize_safe(p, header.roaring_bitmap_size);

    if (!b->rb) {
      bitmap_free(b);
      return NULL;
    }
  } else {
    b->rb = roaring_bitmap_create();
    if (!b->rb) {
      bitmap_free(b);
      return NULL;
    }
  }

  return b;
}

bitmap_t *bitmap_flip(const bitmap_t *bm1, uint64_t range_start,
                      uint64_t range_end) {
  if (!bm1 || !bm1->rb)
    return NULL;
  bitmap_t *r = malloc(sizeof(bitmap_t));
  if (!r)
    return NULL;
  roaring_bitmap_t *rb = roaring_bitmap_flip(bm1->rb, range_start, range_end);
  if (!rb) {
    free(r);
    return NULL;
  }
  r->rb = rb;
  return r;
}

uint32_t bitmap_get_cardinality(const bitmap_t *bm) {
  if (!bm || !bm->rb)
    return 0;
  return roaring_bitmap_get_cardinality(bm->rb);
}

void bitmap_to_uint32_array(const bitmap_t *bm, uint32_t *array) {
  if (!bm || !bm->rb)
    return;
  return roaring_bitmap_to_uint32_array(bm->rb, array);
}