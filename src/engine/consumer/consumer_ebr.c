// consumer_ebr.c
#include "consumer_ebr.h"
#include "core/bitmaps.h"

CK_EPOCH_CONTAINER(bitmap_t, epoch_entry, get_bitmap_from_epoch)

static void _dispose_bitmap(ck_epoch_entry_t *entry) {
  bitmap_t *bm = get_bitmap_from_epoch(entry);
  bitmap_free(bm);
}

void consumer_ebr_init(ck_epoch_t *epoch) { ck_epoch_init(epoch); }

void consumer_ebr_register(ck_epoch_t *epoch, ck_epoch_record_t *record) {
  ck_epoch_register(epoch, record, NULL);
}

void consumer_ebr_retire(ck_epoch_record_t *record, ck_epoch_entry_t *entry) {
  ck_epoch_call(record, entry, _dispose_bitmap);
}

void consumer_ebr_reclaim(ck_epoch_record_t *record) {
  ck_epoch_synchronize(record);
  ck_epoch_reclaim(record);
}