#ifndef CONSUMER_SCHEMA_H
#define CONSUMER_SCHEMA_H

#include "consumer_cache_entry.h"
#include "engine/container/container_types.h"
#include "engine/op/op.h"
#include "engine/op_queue/op_queue_msg.h"
#include <stdbool.h>

/**
 * Schema validation and type mapping for consumer cache entries
 *
 * This module determines:
 * 1. What value type should be stored for a given database key
 * 2. Whether an operation's value type matches the expected schema
 */

// ============================================================================
// Type Conversion
// ============================================================================

/**
 * Get the cache entry value type for a given database key
 * Used when creating new cache entries
 *
 * @param db_key The database key identifying which DB to query
 * @return The cache entry value type, or CONSUMER_CACHE_ENTRY_VAL_BM on error
 */
consumer_cache_entry_val_type_t
consumer_schema_get_value_type(const eng_container_db_key_t *db_key);

/**
 * Convert operation value type to cache entry value type
 *
 * @param op_val_type The operation value type
 * @return The corresponding cache entry value type
 */
consumer_cache_entry_val_type_t
consumer_schema_op_to_cache_type(op_value_type_t op_val_type);

// ============================================================================
// Validation
// ============================================================================

typedef struct {
  bool valid;
  const char *error_msg; // NULL on success
} schema_validation_result_t;

/**
 * Validate that an operation's value type matches the schema for its target DB
 *
 * @param op The operation to validate
 * @return Validation result indicating success or error
 */
schema_validation_result_t consumer_schema_validate_op(const op_t *op);

/**
 * Validate an operation queue message
 * Checks both the operation and ensures ser_db_key is present
 *
 * @param msg The message to validate
 * @return Validation result indicating success or error
 */
schema_validation_result_t
consumer_schema_validate_msg(const op_queue_msg_t *msg);

#endif // CONSUMER_SCHEMA_H