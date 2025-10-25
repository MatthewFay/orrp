#ifndef eng_writer_queue_MSG_H
#define eng_writer_queue_MSG_H

#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include <stdatomic.h>
#include <stdint.h>

typedef enum {
  ENG_WRITER_VAL_BITMAP = 0,
  ENG_WRITER_VAL_INT32,
  ENG_WRITER_VAL_STR
} eng_writer_val_type_t;

typedef struct eng_writer_entry_s {
  eng_writer_val_type_t val_type;
  union {
    bitmap_t *bitmap_copy;
    uint32_t int32;
    char *str_copy;
  } val;
  // Owned by consumer
  _Atomic(uint64_t) *flush_version_ptr;
  uint64_t version;

  eng_container_db_key_t db_key;
} eng_writer_entry_t;

typedef struct eng_writer_msg_s {
  eng_writer_entry_t *entries; // pointer to array
  uint32_t count;
} eng_writer_msg_t;

void eng_writer_queue_free_msg_entry(eng_writer_entry_t *e);
void eng_writer_queue_free_msg(eng_writer_msg_t *msg);

#endif