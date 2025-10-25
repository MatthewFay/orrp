#include "consumer_ebr.h"
#include "core/bitmaps.h"
#include "engine/consumer/consumer_cache_entry.h"

CK_EPOCH_CONTAINER(consumer_cache_bitmap_t, epoch_entry, get_bitmap_from_epoch)

static void _dispose_cc_bitmap(ck_epoch_entry_t *entry) {
  consumer_cache_bitmap_t *cc_bm = get_bitmap_from_epoch(entry);
  bitmap_free(cc_bm->bitmap);
  free(cc_bm);
}

CK_EPOCH_CONTAINER(consumer_cache_str_t, epoch_entry, get_str_from_epoch)

static void _dispose_cc_str(ck_epoch_entry_t *entry) {
  consumer_cache_str_t *cc_str = get_str_from_epoch(entry);
  free(cc_str->str);
  free(cc_str);
}

void consumer_ebr_init(ck_epoch_t *epoch) { ck_epoch_init(epoch); }

void consumer_ebr_register(ck_epoch_t *epoch, ck_epoch_record_t *record) {
  ck_epoch_register(epoch, record, NULL);
}

void consumer_ebr_unregister(ck_epoch_record_t *record) {
  ck_epoch_unregister(record);
}

void consumer_ebr_retire_bitmap(ck_epoch_record_t *record,
                                ck_epoch_entry_t *entry) {
  ck_epoch_call(record, entry, _dispose_cc_bitmap);
}

void consumer_ebr_retire_str(ck_epoch_record_t *record,
                             ck_epoch_entry_t *entry) {
  ck_epoch_call(record, entry, _dispose_cc_str);
}

void consumer_ebr_reclaim(ck_epoch_record_t *record) {
  ck_epoch_synchronize(record);
  ck_epoch_reclaim(record);
}