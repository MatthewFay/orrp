#include "consumer_flush.h"
#include "core/bitmaps.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include <stdatomic.h>
#include <stdint.h>

void consumer_flush_clear_result(consumer_flush_result_t fr) {
  eng_writer_queue_free_msg(fr.msg);
}

static bool _consumer_flush_prepare_entry(consumer_cache_entry_t *cache_entry,
                                          eng_writer_entry_t *writer_entry) {
  bitmap_t *bm;
  uint32_t int32;
  char *str;
  if (!cache_entry || !writer_entry)
    return false;
  switch (cache_entry->val_type) {
  case CONSUMER_CACHE_ENTRY_VAL_BM:
    bm = atomic_load(&cache_entry->val.bitmap);
    if (!bm)
      return false;
    writer_entry->val_type = ENG_WRITER_VAL_BITMAP;
    writer_entry->val.bitmap_copy = bitmap_copy(bm);
    if (!writer_entry->val.bitmap_copy)
      return false;
    break;
  case CONSUMER_CACHE_ENTRY_VAL_INT32:
    int32 = atomic_load(&cache_entry->val.int32);
    writer_entry->val_type = ENG_WRITER_VAL_INT32;
    writer_entry->val.int32 = int32;
    break;
  case CONSUMER_CACHE_ENTRY_VAL_STR:
    str = atomic_load(&cache_entry->val.str);
    if (!str || strlen(str) == 0)
      return false;
    writer_entry->val_type = ENG_WRITER_VAL_STR;
    writer_entry->val.str_copy = str;
    break;
  default:
    return false;
  }

  writer_entry->flush_version_ptr = &cache_entry->flush_version;
  writer_entry->version = cache_entry->version;
  writer_entry->db_key = cache_entry->db_key;

  writer_entry->db_key.container_name =
      strdup(cache_entry->db_key.container_name);
  if (!writer_entry->db_key.container_name) {
    eng_writer_queue_free_msg_entry(writer_entry);
    return false;
  }

  if (cache_entry->db_key.db_key.type == DB_KEY_STRING) {
    writer_entry->db_key.db_key.key.s =
        strdup(cache_entry->db_key.db_key.key.s);
    if (!writer_entry->db_key.db_key.key.s) {
      eng_writer_queue_free_msg_entry(writer_entry);
      return false;
    }
  }

  return true;
}

consumer_flush_result_t
consumer_flush_prepare(consumer_cache_entry_t *dirty_head,
                       uint32_t num_dirty_entries) {
  if (!dirty_head) {
    return (consumer_flush_result_t){.success = false,
                                     .err_msg = "Invalid dirty head"};
  }

  if (num_dirty_entries < 1) {
    return (consumer_flush_result_t){.success = true,
                                     .msg = NULL,
                                     .entries_prepared = 0,
                                     .entries_skipped = 0};
  }

  eng_writer_msg_t *msg = malloc(sizeof(eng_writer_msg_t));
  if (!msg) {
    return (consumer_flush_result_t){
        .success = false, .err_msg = "Failed to allocate writer message"};
  }

  msg->entries = malloc(sizeof(eng_writer_entry_t) * num_dirty_entries);
  if (!msg->entries) {
    free(msg);
    return (consumer_flush_result_t){
        .success = false, .err_msg = "Failed to allocate entries array"};
  }

  msg->count = 0;
  uint32_t skipped = 0;

  for (consumer_cache_entry_t *entry = dirty_head; entry;
       entry = entry->dirty_next) {

    if (!_consumer_flush_prepare_entry(entry, &msg->entries[msg->count])) {
      skipped++;
      continue;
    }
    msg->count++;
  }

  return (consumer_flush_result_t){.success = true,
                                   .msg = msg,
                                   .entries_prepared = msg->count,
                                   .entries_skipped = skipped};
}
