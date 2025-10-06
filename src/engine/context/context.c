#include "context.h"
#include "engine/container/container.h"
#include <stdlib.h>

eng_context_t *eng_create_ctx() {
  eng_context_t *ctx = calloc(1, sizeof(eng_context_t));
  if (!ctx)
    return NULL;
  return ctx;
}

void eng_close_ctx(eng_context_t *ctx) {
  if (!ctx) {
    return;
  }
  if (ctx->sys_c) {
    eng_container_close(ctx->sys_c);
  }

  free(ctx);
}