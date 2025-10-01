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
#include "uthash.h"
#include "uv.h"
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

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
    *mapping_out = em;
    return true;
  }

  bool created_txn = false;
  if (!sys_txn) {
    sys_txn = db_create_txn(worker->config.eng_ctx->sys_c->env, true);
    if (!sys_txn)
      return false;
    created_txn = true;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = (char *)entity_id_str;

  if (!db_get(worker->config.eng_ctx->sys_c->data.sys->sys_dc_metadata_db,
              sys_txn, &db_key, &r)) {
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  em = malloc(sizeof(worker_entity_mapping_t));
  if (!em) {
    db_get_result_clear(&r);
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  em->ent_str_id = strdup(entity_id_str);
  if (!em->ent_str_id) {
    free(em);
    db_get_result_clear(&r);
    if (created_txn)
      db_abort_txn(sys_txn);
    return false;
  }

  if (r.status == DB_GET_OK) {
    em->ent_int_id = *(uint32_t *)r.value;
  } else {
    em->ent_int_id = _get_next_entity_id();
    *is_new_ent_out = true;
  }

  db_get_result_clear(&r);
  if (created_txn)
    db_abort_txn(sys_txn);

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
    return true;
  }

  // Cache miss - need to load from DB
  eng_container_t *dc = eng_dc_cache_get(container_name);
  if (!dc) {
    return false;
  }

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    eng_dc_cache_release_container(dc);
    return false;
  }

  MDB_dbi db;
  if (!eng_container_get_db_handle(dc, USER_DB_METADATA, &db)) {
    _cleanup_container_lookup(dc, txn, NULL);
    return false;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = USR_NEXT_EVENT_ID_KEY;

  if (!db_get(db, txn, &db_key, &r)) {
    _cleanup_container_lookup(dc, txn, &r);
    return false;
  }

  uint32_t next =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : USR_NEXT_EVENT_ID_INIT_VAL;

  next_event_id = malloc(sizeof(atomic_uint_fast32_t));
  if (!next_event_id) {
    _cleanup_container_lookup(dc, txn, &r);
    return false;
  }

  atomic_init(next_event_id, next); // new atomic

  // Try to insert into cache
  if (lock_striped_ht_put_string(&g_next_event_id_by_container, container_name,
                                 next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    _cleanup_container_lookup(dc, txn, &r);
    return true;
  }

  // Race condition - someone else inserted, use theirs
  free(next_event_id);

  if (lock_striped_ht_get_string(&g_next_event_id_by_container, container_name,
                                 (void **)&next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    _cleanup_container_lookup(dc, txn, &r);
    return true;
  }

  // Something went very wrong
  _cleanup_container_lookup(dc, txn, &r);
  return false;
}

bool worker_init_global(eng_context_t *eng_ctx) {
  if (!eng_ctx || !eng_ctx->sys_c) {
    return false;
  }

  MDB_txn *sys_txn = db_create_txn(eng_ctx->sys_c->env, true);
  if (!sys_txn)
    return false;

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = SYS_NEXT_ENT_ID_KEY;

  if (!db_get(eng_ctx->sys_c->data.sys->sys_dc_metadata_db, sys_txn, &db_key,
              &r)) {
    db_abort_txn(sys_txn);
    return false;
  }

  uint32_t next_ent_id =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : SYS_NEXT_ENT_ID_INIT_VAL;

  db_get_result_clear(&r);
  db_abort_txn(sys_txn);

  atomic_store(&g_next_entity_id, next_ent_id);

  if (!lock_striped_ht_init_string(&g_next_event_id_by_container)) {
    return false;
  }

  return true;
}

static bool _queue_up_ops(worker_t *worker, worker_ops_t *ops) {
  // uint32_t queued_count = 0;

  for (uint32_t i = 0; i < ops->num_ops; i++) {
    op_queue_msg_t *msg = ops->ops[i];
    unsigned long hash = xxhash64(msg->routing_key, strlen(msg->routing_key),
                                  OP_QUEUE_HASH_SEED);
    int queue_idx = hash & (worker->config.op_queue_total_count - 1);
    op_queue_t *queue = &worker->config.op_queues[queue_idx];

    if (!op_queue_enqueue(queue, msg)) {
      // Failed to enqueue - clean up remaining ops
      for (uint32_t j = i; j < ops->num_ops; j++) {
        op_queue_msg_free(ops->ops[j]);
      }
      return false;
    }
    // queued_count++;
  }

  return true;
}

static bool _process_msg(worker_t *worker, cmd_queue_msg_t *msg,
                         MDB_txn *sys_txn) {
  if (!msg || !msg->command) {
    return false;
  }

  bool is_new_ent = false;
  char *container_name = msg->command->in_tag_value->literal.string_value;

  uint32_t event_id = 0;
  if (!_get_next_event_id_for_container(container_name, &event_id)) {
    return false;
  }

  char *entity_id_str = msg->command->entity_tag_value->literal.string_value;
  worker_entity_mapping_t *em = NULL;

  if (!_get_entity_mapping_by_str(worker, sys_txn, entity_id_str, &em,
                                  &is_new_ent)) {
    return false;
  }

  worker_ops_t ops = {0};

  if (!worker_create_ops(msg, container_name, em->ent_str_id, em->ent_int_id,
                         is_new_ent, event_id, &ops)) {
    return false;
  }

  bool success = _queue_up_ops(worker, &ops);

  // TODO BUG!
  // Note: ops are now owned by queues if successful, but we still need
  // to free the ops array itself
  // worker_ops_free(&ops);

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
        _process_msg(worker, msg, sys_txn);
        // Can we replace these with 1 function?
        cmd_queue_free_msg_contents(msg);
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
  int backoff = 1;
  int spin_count = 0;

  while (!worker->should_stop) {
    if (_do_work(worker) > 0) {
      backoff = 1;
      spin_count = 0;
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
}

bool worker_start(worker_t *worker, const worker_config_t *config) {
  if (!worker || !config) {
    return false;
  }

  worker->config = *config;
  worker->should_stop = false;
  worker->messages_processed = 0;
  worker->entity_mappings = NULL;

  if (uv_thread_create(&worker->thread, _worker_thread_func, worker) != 0) {
    return false;
  }
  return true;
}

bool worker_stop(worker_t *worker) {
  if (!worker) {
    return false;
  }

  worker->should_stop = true;

  if (uv_thread_join(&worker->thread) != 0) {
    return false;
  }

  return true;
}

void worker_cleanup(worker_t *worker) {
  if (!worker) {
    return;
  }

  // Free cached entity mappings
  worker_entity_mapping_t *current, *tmp;
  HASH_ITER(hh, worker->entity_mappings, current, tmp) {
    HASH_DEL(worker->entity_mappings, current);
    free(current->ent_str_id);
    free(current);
  }
  worker->entity_mappings = NULL;
}