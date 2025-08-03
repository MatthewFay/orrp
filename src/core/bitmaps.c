#include "core/bitmaps.h"
#include "roaring.h"

bitmap_t *bitmap_create()
{
  bitmap_t *bm = malloc(sizeof(bitmap_t));
  if (bm == NULL)
    return NULL;
  bm->rb = roaring_bitmap_create();
  if (bm->rb == NULL)
  {
    free(bm);
    return NULL;
  }
  return bm;
}

void bitmap_add(bitmap_t *bm, uint32_t value)
{
  if (bm && bm->rb)
  {
    roaring_bitmap_add(bm->rb, value);
  }
}

bool bitmap_contains(bitmap_t *bm, uint32_t value)
{
  if (bm && bm->rb)
  {
    return roaring_bitmap_contains(bm->rb, value);
  }
  // TODO: Should not return false on error condition
  return false;
}

void bitmap_free(bitmap_t *bm)
{
  if (bm)
  {
    if (bm->rb)
    {
      roaring_bitmap_free(bm->rb);
    }
    free(bm);
  }
}