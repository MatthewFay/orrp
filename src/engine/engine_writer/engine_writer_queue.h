#ifndef ENG_WRITER_QUEUE_H
#define ENG_WRITER_QUEUE_H

#include "ck_ring.h"
#include "engine_writer_queue_msg.h"
#include <stdbool.h>

#define ENG_WRITER_QUEUE_CAPACITY 65536

typedef struct eng_writer_queue_s {
  ck_ring_t ring;
  ck_ring_buffer_t ring_buffer[ENG_WRITER_QUEUE_CAPACITY];
} eng_writer_queue_t;

bool eng_writer_queue_init(eng_writer_queue_t *eng_writer_queue);
void eng_writer_queue_destroy(eng_writer_queue_t *eng_writer_queue);

bool eng_writer_queue_enqueue(eng_writer_queue_t *eng_writer_queue,
                              eng_writer_msg_t *msg);

#endif