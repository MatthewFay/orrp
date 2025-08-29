#include "core/conversions.h"
#include <stdio.h>

int conv_uint32_to_string(char *buffer, size_t buffer_size, uint32_t value) {
  int chars_written = snprintf(buffer, buffer_size, "%u", value);

  if (chars_written < 0 || (size_t)chars_written >= buffer_size) {
    return -1;
  }

  return chars_written;
}