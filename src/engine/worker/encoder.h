#ifndef ENCODER_H
#define ENCODER_H
#include "engine/cmd_context/cmd_context.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Encode event into MessagePack
bool encode_event(cmd_ctx_t *cmd_ctx, uint32_t event_id, char **data_out,
                  size_t *size_out);

#endif