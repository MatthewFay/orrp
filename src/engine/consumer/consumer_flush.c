#include "consumer_flush.h"
#include "core/bitmaps.h"
#include "engine/consumer/consumer_cache_entry.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include <stdatomic.h>
#include <stdint.h>

void consumer_flush_clear_result(consumer_flush_result_t fr) {
  eng_writer_queue_free_msg(fr.msg);
}

static bool _ser_bitmap(consumer_cache_bitmap_t *cc_bm, void **val_out,
                        size_t *val_size_out) {
  *val_out = bitmap_serialize(cc_bm->bitmap, val_size_out);
  if (!*val_out) {
    return false;
  }
  return true;
}

static bool _consumer_flush_prepare_entry(consumer_cache_entry_t *cache_entry,
                                          eng_writer_entry_t *writer_entry) {
  if (!cache_entry || !writer_entry)
    return false;
  consumer_cache_bitmap_t *cc_bm = atomic_load(&cache_entry->cc_bitmap);
  if (!cc_bm || !cc_bm->bitmap)
    return false;
  if (!_ser_bitmap(cc_bm, &writer_entry->value, &writer_entry->value_size)) {
    return false;
  }

  writer_entry->bump_flush_version = true;
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

  msg->entries = calloc(num_dirty_entries, sizeof(eng_writer_entry_t));
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
