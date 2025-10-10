#include "worker.h"
#include "core/db.h"
#include "core/hash.h"
#include "core/lock_striped_ht.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/container/container.h"
#include "engine/dc_cache/dc_cache.h"
#include "engine/op_queue/op_queue.h"
#include "engine/op_queue/op_queue_msg.h"
#include "engine/worker/worker_ops.h"
#include "lmdb.h"
#include "log/log.h"
#include "uthash.h"
#include "uv.h"
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

LOG_INIT(worker);

// spin count before sleeping
#define WORKER_SPIN_LIMIT 100
#define WORKER_MAX_SLEEP_MS 64

#define OP_QUEUE_HASH_SEED 0

atomic_uint_fast32_t g_next_entity_id = ATOMIC_VAR_INIT(0);

lock_striped_ht_t g_next_event_id_by_container;

static uint32_t _get_next_entity_id() {
  return atomic_fetch_add(&g_next_entity_id, 1);
}

static bool _get_entity_mapping_by_str(worker_t *worker, MDB_txn *sys_txn,
                                       const char *entity_id_str,
                                       worker_entity_mapping_t **mapping_out,
                                       bool *is_new_ent_out) {
  worker_entity_mapping_t *em = NULL;
  *is_new_ent_out = false;
  *mapping_out = NULL;

  HASH_FIND_STR(worker->entity_mappings, entity_id_str, em);
  if (em) {
    LOG_DEBUG("Entity mapping cache hit: %s -> %u", entity_id_str,
              em->ent_int_id);
    *mapping_out = em;
    return true;
  }

  LOG_DEBUG("Entity mapping cache miss: %s", entity_id_str);

  bool created_txn = false;
  if (!sys_txn) {
    sys_txn = db_create_txn(worker->config.eng_ctx->sys_c->env, true);
    if (!sys_txn) {
      LOG_ERROR("Failed to create transaction for entity mapping lookup: %s",
                entity_id_str);
      return false;
    }
    created_txn = true;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = (char *)entity_id_str;

  if (!db_get(worker->config.eng_ctx->sys_c->data.sys->sys_dc_metadata_db,
              sys_txn, &db_key, &r)) {
    LOG_ERROR("Failed to get entity metadata from DB: %s", entity_id_str);
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  em = malloc(sizeof(worker_entity_mapping_t));
  if (!em) {
    LOG_ERROR("Failed to allocate entity mapping for: %s", entity_id_str);
    db_get_result_clear(&r);
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  em->ent_str_id = strdup(entity_id_str);
  if (!em->ent_str_id) {
    LOG_ERROR("Failed to duplicate entity string ID: %s", entity_id_str);
    free(em);
    db_get_result_clear(&r);
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  if (r.status == DB_GET_OK) {
    em->ent_int_id = *(uint32_t *)r.value;
    LOG_DEBUG("Loaded existing entity: %s -> %u", entity_id_str,
              em->ent_int_id);
  } else {
    em->ent_int_id = _get_next_entity_id();
    *is_new_ent_out = true;
    LOG_INFO("Created new entity: %s -> %u", entity_id_str, em->ent_int_id);
  }

  db_get_result_clear(&r);
  if (created_txn)
    db_abort_txn(sys_txn);

  HASH_ADD_KEYPTR(hh, worker->entity_mappings, em->ent_str_id,
                  strlen(em->ent_str_id), em);

  *mapping_out = em;
  return true;
}

static void _cleanup_container_lookup(eng_container_t *dc, MDB_txn *txn,
                                      db_get_result_t *get_r) {
  if (dc)
    eng_dc_cache_release_container(dc);
  if (txn)
    db_abort_txn(txn);
  if (get_r)
    db_get_result_clear(get_r);
}

static bool _get_next_event_id_for_container(const char *container_name,
                                             uint32_t *event_id_out) {
  atomic_uint_fast32_t *next_event_id = NULL;

  // Check cache first
  if (lock_striped_ht_get_string(&g_next_event_id_by_container, container_name,
                                 (void **)&next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    LOG_DEBUG("Event ID cache hit for container %s: %u", container_name,
              *event_id_out);
    return true;
  }

  LOG_DEBUG("Event ID cache miss for container: %s", container_name);

  // Cache miss - need to load from DB
  eng_container_t *dc = eng_dc_cache_get(container_name);
  if (!dc) {
    LOG_ERROR("Failed to get container from cache: %s", container_name);
    return false;
  }

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    LOG_ERROR("Failed to create transaction for container: %s", container_name);
    eng_dc_cache_release_container(dc);
    return false;
  }

  MDB_dbi db;
  if (!eng_container_get_db_handle(dc, USER_DB_METADATA, &db)) {
    LOG_ERROR("Failed to get metadata DB handle for container: %s",
              container_name);
    _cleanup_container_lookup(dc, txn, NULL);
    return false;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = USR_NEXT_EVENT_ID_KEY;

  if (!db_get(db, txn, &db_key, &r)) {
    LOG_ERROR("Failed to get next event ID from DB for container: %s",
              container_name);
    _cleanup_container_lookup(dc, txn, &r);
    return false;
  }

  uint32_t next =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : USR_NEXT_EVENT_ID_INIT_VAL;

  next_event_id = malloc(sizeof(atomic_uint_fast32_t));
  if (!next_event_id) {
    LOG_ERROR("Failed to allocate atomic counter for container: %s",
              container_name);
    _cleanup_container_lookup(dc, txn, &r);
    return false;
  }

  atomic_init(next_event_id, next);
  LOG_INFO("Initialized event ID counter for container %s at %u",
           container_name, next);

  // Try to insert into cache
  if (lock_striped_ht_put_string(&g_next_event_id_by_container, container_name,
                                 next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    _cleanup_container_lookup(dc, txn, &r);
    return true;
  }

  // Race condition - someone else inserted, use theirs
  LOG_DEBUG("Race condition detected inserting event ID for container: %s",
            container_name);
  free(next_event_id);

  if (lock_striped_ht_get_string(&g_next_event_id_by_container, container_name,
                                 (void **)&next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    _cleanup_container_lookup(dc, txn, &r);
    return true;
  }

  // Something went very wrong
  LOG_ERROR(
      "Failed to retrieve event ID after race condition for container: %s",
      container_name);
  _cleanup_container_lookup(dc, txn, &r);
  return false;
}

bool worker_init_global(eng_context_t *eng_ctx) {
  log_init_worker();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize worker logging\n");
    return false;
  }

  if (!eng_ctx || !eng_ctx->sys_c) {
    LOG_ERROR("Invalid engine context in worker_init_global");
    return false;
  }

  MDB_txn *sys_txn = db_create_txn(eng_ctx->sys_c->env, true);
  if (!sys_txn) {
    LOG_ERROR("Failed to create system transaction in worker_init_global");
    return false;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = SYS_NEXT_ENT_ID_KEY;

  if (!db_get(eng_ctx->sys_c->data.sys->sys_dc_metadata_db, sys_txn, &db_key,
              &r)) {
    LOG_ERROR("Failed to get next entity ID from system DB");
    db_abort_txn(sys_txn);
    return false;
  }

  uint32_t next_ent_id =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : SYS_NEXT_ENT_ID_INIT_VAL;

  db_get_result_clear(&r);
  db_abort_txn(sys_txn);

  atomic_store(&g_next_entity_id, next_ent_id);
  LOG_INFO("Initialized global entity ID counter at %u", next_ent_id);

  if (!lock_striped_ht_init_string(&g_next_event_id_by_container)) {
    LOG_ERROR("Failed to initialize event ID hash table");
    return false;
  }

  LOG_INFO("Worker global initialization complete");
  return true;
}

static bool _queue_up_ops(worker_t *worker, worker_ops_t *ops) {
  for (uint32_t i = 0; i < ops->num_ops; i++) {
    op_queue_msg_t *msg = ops->ops[i];
    unsigned long hash =
        xxhash64(msg->ser_db_key, strlen(msg->ser_db_key), OP_QUEUE_HASH_SEED);
    int queue_idx = hash & (worker->config.op_queue_total_count - 1);
    op_queue_t *queue = &worker->config.op_queues[queue_idx];

    if (!op_queue_enqueue(queue, msg)) {
      LOG_ERROR("Failed to enqueue op %u/%u to queue %d", i + 1, ops->num_ops,
                queue_idx);
      // Failed to enqueue - clean up remaining ops
      for (uint32_t j = i; j < ops->num_ops; j++) {
        op_queue_msg_free(ops->ops[j]);
      }
      return false;
    }
    LOG_DEBUG("Enqueued op to queue %d: %s", queue_idx, msg->ser_db_key);
  }

  return true;
}

static bool _process_msg(worker_t *worker, cmd_queue_msg_t *msg,
                         MDB_txn **sys_txn_ptr) {
  if (!msg || !msg->command) {
    LOG_WARN("Received invalid message: null command");
    return false;
  }

  bool is_new_ent = false;
  char *container_name = msg->command->in_tag_value->literal.string_value;

  uint32_t event_id = 0;
  if (!_get_next_event_id_for_container(container_name, &event_id)) {
    LOG_ERROR("Failed to get next event ID for container: %s", container_name);
    return false;
  }

  char *entity_id_str = msg->command->entity_tag_value->literal.string_value;
  worker_entity_mapping_t *em = NULL;

  if (!*sys_txn_ptr) {
    *sys_txn_ptr = db_create_txn(worker->config.eng_ctx->sys_c->env, true);
    if (!*sys_txn_ptr) {
      LOG_ERROR("Failed to create system transaction for message processing");
      return false;
    }
  }

  if (!_get_entity_mapping_by_str(worker, *sys_txn_ptr, entity_id_str, &em,
                                  &is_new_ent)) {
    LOG_ERROR("Failed to get entity mapping for: %s", entity_id_str);
    return false;
  }

  worker_ops_t ops = {0};

  if (!worker_create_ops(msg, container_name, em->ent_str_id, em->ent_int_id,
                         is_new_ent, event_id, &ops)) {
    LOG_ERROR("Failed to create ops for entity %s in container %s",
              entity_id_str, container_name);
    return false;
  }

  LOG_DEBUG("Created %u ops for entity %u, event %u", ops.num_ops,
            em->ent_int_id, event_id);

  bool success = _queue_up_ops(worker, &ops);

  // ops are now owned by queues if successful, but we still need
  // to free the ops array itself
  worker_ops_clear(&ops);

  return success;
}

// Returns num messages processed
static int _do_work(worker_t *worker) {
  size_t prev_num_msgs = 0;
  size_t num_msgs = 0;
  cmd_queue_msg_t *msg = NULL;
  MDB_txn *sys_txn = NULL;

  do {
    prev_num_msgs = num_msgs;
    for (uint32_t i = 0; i < worker->config.cmd_queue_consume_count; i++) {
      uint32_t cmd_queue_idx = worker->config.cmd_queue_consume_start + i;
      cmd_queue_t *queue = &worker->config.cmd_queues[cmd_queue_idx];

      if (cmd_queue_dequeue(queue, &msg)) {
        num_msgs++;
        if (!_process_msg(worker, msg, &sys_txn)) {
          LOG_WARN("Failed to process message from queue %u", cmd_queue_idx);
        }
        cmd_queue_free_msg(msg);
        msg = NULL;
      }
    }
  } while (prev_num_msgs != num_msgs);

  // `sys_txn` created lazily
  if (sys_txn) {
    db_abort_txn(sys_txn);
  }

  return num_msgs;
}

static void _worker_thread_func(void *arg) {
  worker_t *worker = (worker_t *)arg;

  log_init_worker();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize logging for worker thread\n");
    return;
  }

  LOG_INFO("Worker thread started");

  int backoff = 1;
  int spin_count = 0;
  size_t total_processed = 0;

  while (!worker->should_stop) {
    int processed = _do_work(worker);
    if (processed > 0) {
      total_processed += processed;
      backoff = 1;
      spin_count = 0;

      // Log stats periodically
      if (total_processed % 10000 == 0) {
        LOG_INFO("Worker processed %zu messages", total_processed);
      }
    } else {
      if (spin_count < WORKER_SPIN_LIMIT) {
        sched_yield();
        spin_count++;
      } else {
        uv_sleep(backoff);
        backoff =
            backoff < WORKER_MAX_SLEEP_MS ? backoff * 2 : WORKER_MAX_SLEEP_MS;
      }
    }
  }

  LOG_INFO("Worker thread exiting [total_processed=%zu]", total_processed);
}

bool worker_start(worker_t *worker, const worker_config_t *config) {
  if (!worker || !config) {
    LOG_ERROR("Invalid arguments to worker_start");
    return false;
  }

  worker->config = *config;
  worker->should_stop = false;
  worker->messages_processed = 0;
  worker->entity_mappings = NULL;

  if (uv_thread_create(&worker->thread, _worker_thread_func, worker) != 0) {
    LOG_ERROR("Failed to create worker thread");
    return false;
  }

  LOG_INFO("Worker thread created successfully");
  return true;
}

bool worker_stop(worker_t *worker) {
  if (!worker) {
    LOG_ERROR("Invalid worker in worker_stop");
    return false;
  }

  LOG_INFO("Stopping worker thread...");
  worker->should_stop = true;

  if (uv_thread_join(&worker->thread) != 0) {
    LOG_ERROR("Failed to join worker thread");
    return false;
  }

  LOG_INFO("Worker thread stopped successfully");
  return true;
}

void worker_cleanup(worker_t *worker) {
  if (!worker) {
    return;
  }

  // Free cached entity mappings
  worker_entity_mapping_t *current, *tmp;
  size_t freed_count = 0;

  HASH_ITER(hh, worker->entity_mappings, current, tmp) {
    HASH_DEL(worker->entity_mappings, current);
    free(current->ent_str_id);
    free(current);
    freed_count++;
  }
  worker->entity_mappings = NULL;

  LOG_INFO("Worker cleanup complete: freed %zu entity mappings", freed_count);
}