#ifndef ID_MANAGER_H
#define ID_MANAGER_H

#include "context.h"
#include "lmdb.h"
#include <stdbool.h>
#include <stdint.h>

// Initializes the ID manager. Must be called once at server startup.
// It will load the last known counter values from the system container.
bool id_manager_init(eng_context_t *ctx);

// Destroys the ID manager resources. Called on graceful shutdown.
void id_manager_destroy(void);

// --- Entity ID Functions ---

// Gets the next available global entity ID. Thread-safe.
uint32_t id_manager_get_next_entity_id(void);

// Gets the last reserved entity ID for the background writer to persist.
uint32_t id_manager_get_last_reserved_entity_id(void);

// --- Event ID Functions ---

// Gets the next available event ID for a specific container. Thread-safe.
uint32_t id_manager_get_next_event_id(eng_container_t *container, MDB_txn *txn);

// Gets the last reserved event ID for a specific container.
// Used by the background writer.
uint32_t id_manager_get_last_reserved_event_id(const char *container_name);

#endif // ID_MANAGER_H