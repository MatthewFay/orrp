#ifndef CONVERSIONS_H
#define CONVERSIONS_H

#include <stddef.h>
#include <stdint.h>

/**
 * @brief Safely converts a uint32_t to a null-terminated string.
 *
 * @param buffer The character buffer to write the string into.
 * @param buffer_size The total size of the buffer.
 * @param value The uint32_t value to convert.
 * @return The number of characters written (excluding the null terminator),
 * or -1 on error.
 */
int conv_uint32_to_string(char *buffer, size_t buffer_size, uint32_t value);

#endif // CONVERSIONS_H
