#ifndef CONTEXT_H
#define CONTEXT_H

// --- ENGINE CONTEXT --- //

#include "container.h"

typedef struct eng_context_s {
  eng_container_t *sys_c;
} eng_context_t;

eng_context_t *eng_create_ctx(void);

// Close engine context - Called on graceful shutdown
void eng_close_ctx(eng_context_t *ctx);

#endif