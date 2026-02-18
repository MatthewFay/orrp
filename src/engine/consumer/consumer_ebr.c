#include "consumer_ebr.h"
#include "core/bitmaps.h"
#include "core/ebr.h"
#include "engine/consumer/consumer_cache_entry.h"

CK_EPOCH_CONTAINER(consumer_cache_bitmap_t, epoch_entry,
                   get_bitmap_from_epoch_entry)

static void _dispose_cc_bitmap(ck_epoch_entry_t *entry) {
  consumer_cache_bitmap_t *cc_bm = get_bitmap_from_epoch_entry(entry);
  bitmap_free(cc_bm->bitmap);
  free(cc_bm);
}

void consumer_ebr_retire_bitmap(ck_epoch_entry_t *entry) {
  ebr_call(entry, _dispose_cc_bitmap);
}
