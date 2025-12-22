#include "worker.h"
#include "core/data_constants.h"
#include "core/db.h"
#include "core/lock_striped_ht.h"
#include "core/mmap_array.h"
#include "engine/cmd_queue/cmd_queue.h"
#include "engine/cmd_queue/cmd_queue_msg.h"
#include "engine/container/container.h"
#include "engine/container/container_types.h"
#include "engine/engine_writer/engine_writer_queue.h"
#include "engine/engine_writer/engine_writer_queue_msg.h"
#include "engine/op_queue/op_queue.h"
#include "engine/op_queue/op_queue_msg.h"
#include "engine/routing/routing.h"
#include "engine/worker/worker_ops.h"
#include "engine/worker/worker_writer.h"
#include "lmdb.h"
#include "log/log.h"
#include "query/ast.h"
#include "uthash.h"
#include "worker_log.h"
#include <sched.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

LOG_INIT(worker);

// spin count before sleeping
#define WORKER_SPIN_LIMIT 100
#define WORKER_MAX_SLEEP_MS 64

atomic_uint_fast32_t g_next_entity_id =
    ATOMIC_VAR_INIT(SYS_NEXT_ENT_ID_INIT_VAL);

lock_striped_ht_t g_next_event_id_by_container;

static uint32_t _get_next_entity_id() {
  return atomic_fetch_add(&g_next_entity_id, 1);
}

static bool _ensure_sys_container(eng_container_t **sys_c_ptr) {
  if (*sys_c_ptr) {
    return true;
  }

  container_result_t cr = container_get_system();
  if (!cr.success) {
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=system");
    return false;
  }

  *sys_c_ptr = cr.container;
  return true;
}

static bool _get_entity_mapping(worker_t *worker, eng_container_t **sys_c_ptr,
                                MDB_txn **sys_txn_ptr,
                                ast_literal_node_t *ent_node,
                                uint32_t *ent_id_out, bool *is_new_ent_out,
                                bool *created_sys_txn) {
  *is_new_ent_out = false;
  bool ent_is_str = ent_node->type == AST_LITERAL_STRING;

  if (ent_is_str) {
    khint_t k =
        kh_get(str_u32, worker->str_to_entity_id, ent_node->string_value);
    if (k != kh_end(worker->str_to_entity_id)) {
      *ent_id_out = kh_value(worker->str_to_entity_id, k);
      LOG_ACTION_DEBUG(ACT_CACHE_HIT,
                       "context=entity_mapping entity=\"%s\" ent_int_id=%u",
                       ent_node->string_value, *ent_id_out);
      return true;
    }
  } else {
    khint_t k =
        kh_get(i64_u32, worker->int_to_entity_id, ent_node->number_value);
    if (k != kh_end(worker->int_to_entity_id)) {
      *ent_id_out = kh_value(worker->int_to_entity_id, k);
      LOG_ACTION_DEBUG(ACT_CACHE_HIT,
                       "context=entity_mapping entity=%lld ent_int_id=%u",
                       (long long)ent_node->number_value, *ent_id_out);
      return true;
    }
  }

  LOG_ENT_DEBUG(ACT_CACHE_MISS, ent_node, "context=entity_mapping");

  if (!_ensure_sys_container(sys_c_ptr)) {
    LOG_ENT_ERROR(ACT_CONTAINER_OPEN_FAILED, ent_node,
                  "context=entity_mapping err=\"No system container\"");
    return false;
  }
  if (!*sys_txn_ptr) {
    MDB_txn *txn = db_create_txn((*sys_c_ptr)->env, true);
    if (!txn) {
      LOG_ENT_ERROR(
          ACT_TXN_FAILED, ent_node,
          "context=entity_mapping err=\"failed to create sys transaction\"");
      return false;
    }
    *sys_txn_ptr = txn;
    *created_sys_txn = true;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  if (ent_is_str) {
    db_key.type = DB_KEY_STRING;
    db_key.key.s = ent_node->string_value;
  } else {
    db_key.type = DB_KEY_I64;
    db_key.key.i64 = ent_node->number_value;
  }

  if (!db_get((*sys_c_ptr)->data.sys->sys_dc_metadata_db, *sys_txn_ptr, &db_key,
              &r)) {
    LOG_ENT_ERROR(ACT_DB_READ_FAILED, ent_node,
                  "context=entity_metadata db=sys_dc_metadata");
    return false;
  }

  if (r.status == DB_GET_OK) {
    *ent_id_out = *(uint32_t *)r.value;
    LOG_ENT_DEBUG(ACT_DB_READ, ent_node,
                  "context=entity_mapping ent_int_id=%u status=existing",
                  *ent_id_out);
  } else {
    *ent_id_out = _get_next_entity_id();
    *is_new_ent_out = true;
    LOG_ENT_INFO(ACT_CACHE_ENTRY_CREATED, ent_node,
                 "context=entity_mapping ent_int_id=%u status=new",
                 *ent_id_out);
  }

  db_get_result_clear(&r);

  int ret;
  khiter_t k;

  if (ent_is_str) {
    k = kh_put(str_u32, worker->str_to_entity_id,
               strdup(ent_node->string_value), &ret);
    kh_value(worker->str_to_entity_id, k) = *ent_id_out;
  } else {
    k = kh_put(i64_u32, worker->int_to_entity_id, ent_node->number_value, &ret);
    kh_value(worker->int_to_entity_id, k) = *ent_id_out;
  }

  char buffer[SLOT_SIZE] = {0};
  if (ent_is_str) {
    buffer[0] = VAL_TYPE_STR;
    strncpy(buffer + TAG_UNION_SIZE, ent_node->string_value,
            MAX_ENTITY_STR_LEN);
    buffer[SLOT_SIZE - 1] = '\0';
  } else {
    buffer[0] = VAL_TYPE_I64;
    // Use memcpy to avoid memory alignment bus errors
    memcpy(buffer + TAG_UNION_SIZE, &ent_node->number_value, sizeof(int64_t));
  }
  if (mmap_array_set(&(*sys_c_ptr)->data.sys->entity_id_map, *ent_id_out,
                     buffer) != 0) {
    // TODO: error handling
    return false;
  }

  return true;
}

static bool _get_user_dc(worker_t *worker, const char *container_name,
                         worker_user_dc_t **user_dc_out) {
  worker_user_dc_t *user_dc = NULL;
  HASH_FIND_STR(worker->user_dcs, container_name, user_dc);
  if (user_dc) {
    *user_dc_out = user_dc;
    return true;
  }

  container_result_t cr = container_get_or_create_user(container_name);
  if (!cr.success) {
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                     container_name);
    return false;
  }
  eng_container_t *dc = cr.container;

  MDB_txn *txn = db_create_txn(dc->env, true);
  if (!txn) {
    LOG_ACTION_ERROR(ACT_TXN_BEGIN, "err=\"failed\" container=\"%s\"",
                     container_name);
    container_release(dc);
    return false;
  }

  user_dc = calloc(1, sizeof(worker_user_dc_t));
  if (!user_dc) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"user_dc\" container=\"%s\"", container_name);
    db_abort_txn(txn);
    container_release(dc);
    return false;
  }

  user_dc->container_name = strdup(container_name);
  if (!user_dc->container_name) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"container_name_dup\" container=\"%s\"",
                     container_name);
    free(user_dc);
    db_abort_txn(txn);
    container_release(dc);
    return false;
  }

  user_dc->dc = dc;
  user_dc->txn = txn;

  HASH_ADD_KEYPTR(hh, worker->user_dcs, user_dc->container_name,
                  strlen(user_dc->container_name), user_dc);

  *user_dc_out = user_dc;
  return true;
}

static bool _get_next_event_id_for_container(worker_t *worker,
                                             const char *container_name,
                                             uint32_t *event_id_out) {
  atomic_uint_fast32_t *next_event_id = NULL;

  // Check cache first
  if (lock_striped_ht_get_string(&g_next_event_id_by_container, container_name,
                                 (void **)&next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    LOG_ACTION_DEBUG(ACT_CACHE_HIT,
                     "context=\"event_id\" container=\"%s\" event_id=%u",
                     container_name, *event_id_out);
    return true;
  }

  LOG_ACTION_DEBUG(ACT_CACHE_MISS, "context=\"event_id\" container=\"%s\"",
                   container_name);

  // Cache miss - need to load from DB
  worker_user_dc_t *user_dc = NULL;
  if (!_get_user_dc(worker, container_name, &user_dc)) {
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                     container_name);
    return false;
  }
  MDB_dbi db;
  if (!container_get_user_db_handle(user_dc->dc, USR_DB_METADATA, &db)) {
    LOG_ACTION_ERROR(ACT_DB_HANDLE_FAILED, "db=metadata container=\"%s\"",
                     container_name);
    return false;
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = USR_NEXT_EVENT_ID_KEY;

  if (!db_get(db, user_dc->txn, &db_key, &r)) {
    LOG_ACTION_ERROR(ACT_DB_READ_FAILED,
                     "context=\"next_event_id\" container=\"%s\"",
                     container_name);
    return false;
  }

  uint32_t next =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : USR_NEXT_EVENT_ID_INIT_VAL;
  db_get_result_clear(&r);

  next_event_id = malloc(sizeof(atomic_uint_fast32_t));
  if (!next_event_id) {
    LOG_ACTION_ERROR(ACT_MEMORY_ALLOC_FAILED,
                     "context=\"event_id_counter\" container=\"%s\"",
                     container_name);
    return false;
  }

  atomic_init(next_event_id, next);
  LOG_ACTION_INFO(ACT_COUNTER_INIT,
                  "counter_type=event_id container=\"%s\" value=%u",
                  container_name, next);

  // duplicate `container_name` since it belongs to msg
  char *cache_key = strdup(container_name);

  // Try to insert into cache
  if (lock_striped_ht_put_string(&g_next_event_id_by_container, cache_key,
                                 next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    return true;
  }

  // Race condition - someone else inserted, use theirs
  LOG_ACTION_DEBUG(ACT_RACE_CONDITION,
                   "context=\"event_id_insert\" container=\"%s\"",
                   container_name);
  free(next_event_id);
  free(cache_key);

  if (lock_striped_ht_get_string(&g_next_event_id_by_container, container_name,
                                 (void **)&next_event_id)) {
    *event_id_out = atomic_fetch_add(next_event_id, 1);
    return true;
  }

  // Something went very wrong
  LOG_ACTION_ERROR(ACT_RACE_CONDITION,
                   "context=\"event_id_retrieve_after_race\" container=\"%s\" "
                   "err=\"failed\"",
                   container_name);
  return false;
}

worker_init_result_t worker_init_global(void) {
  container_result_t cr = container_get_system();
  if (!cr.success) {
    return (worker_init_result_t){.success = false,
                                  .msg = "Failed to get system container"};
  }
  eng_container_t *sys_c = cr.container;
  MDB_txn *sys_txn = db_create_txn(sys_c->env, true);
  if (!sys_txn) {
    return (worker_init_result_t){
        .success = false,
        .msg = "Failed to create system transaction in worker_init_global"};
  }

  db_get_result_t r = {0};
  db_key_t db_key;
  db_key.type = DB_KEY_STRING;
  db_key.key.s = SYS_NEXT_ENT_ID_KEY;

  if (!db_get(sys_c->data.sys->sys_dc_metadata_db, sys_txn, &db_key, &r)) {
    db_abort_txn(sys_txn);
    return (worker_init_result_t){
        .success = false, .msg = "Failed to get next entity ID from system DB"};
  }

  uint32_t next_ent_id =
      r.status == DB_GET_OK ? *(uint32_t *)r.value : SYS_NEXT_ENT_ID_INIT_VAL;

  db_get_result_clear(&r);
  db_abort_txn(sys_txn);

  atomic_store(&g_next_entity_id, next_ent_id);

  if (!lock_striped_ht_init_string(&g_next_event_id_by_container)) {
    return (worker_init_result_t){
        .success = false, .msg = "Failed to initialize event ID hash table"};
  }

  return (worker_init_result_t){.success = true, .next_ent_id = next_ent_id};
}

static bool _queue_up_ops(worker_t *worker, worker_ops_t *ops) {
  for (uint32_t i = 0; i < ops->num_ops; i++) {
    op_queue_msg_t *msg = ops->ops[i];
    int queue_idx = route_key_to_queue(msg->ser_db_key,
                                       worker->config.op_queue_total_count);
    op_queue_t *queue = &worker->config.op_queues[queue_idx];

    if (!op_queue_enqueue(queue, msg)) {
      LOG_ACTION_ERROR(ACT_MSG_ENQUEUE_FAILED,
                       "msg_type=op op_num=%u/%u queue_id=%d", i + 1,
                       ops->num_ops, queue_idx);
      // Failed to enqueue - clean up remaining ops
      for (uint32_t j = i; j < ops->num_ops; j++) {
        op_queue_msg_free(ops->ops[j]);
      }
      return false;
    }
    LOG_ACTION_DEBUG(ACT_MSG_ENQUEUED, "msg_type=op queue_id=%d key=\"%s\"",
                     queue_idx, msg->ser_db_key);
  }

  return true;
}

static bool _write_to_event_ent_map(worker_t *worker, char *container_name,
                                    uint32_t ent_id, uint32_t event_id) {
  worker_user_dc_t *user_dc = NULL;
  if (!_get_user_dc(worker, container_name, &user_dc)) {
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                     container_name);
    return false;
  }
  if (mmap_array_set(&user_dc->dc->data.usr->event_to_entity_map, event_id,
                     &ent_id) != 0) {
    // TODO: error handling
    return false;
  }
  return true;
}

static bool _write_to_event_ts_map(worker_t *worker, char *container_name,
                                   int64_t ts, uint32_t event_id) {
  worker_user_dc_t *user_dc = NULL;
  if (!_get_user_dc(worker, container_name, &user_dc)) {
    LOG_ACTION_ERROR(ACT_CONTAINER_OPEN_FAILED, "container=\"%s\"",
                     container_name);
    return false;
  }
  if (mmap_array_set(&user_dc->dc->data.usr->event_to_ts_map, event_id, &ts) !=
      0) {
    // TODO: error handling
    return false;
  }
  return true;
}

static bool _send_to_writer(eng_writer_msg_t *writer_msg, worker_t *worker) {
  if (!writer_msg)
    return false;
  if (!eng_writer_queue_enqueue(&worker->config.writer->queue, writer_msg)) {
    LOG_ACTION_ERROR(ACT_FLUSH_FAILED,
                     "context=\"send_to_writer\" entries_prepared=%u",
                     writer_msg->count);
    return false;
  }
  LOG_ACTION_INFO(ACT_PERF_FLUSH_COMPLETE, "entries_flushed=%u",
                  writer_msg->count);
  return true;
}

static bool _process_msg(worker_t *worker, cmd_queue_msg_t *msg,
                         eng_container_t **sys_c_ptr, MDB_txn **sys_txn_ptr,
                         bool *created_sys_txn_out) {
  if (!msg || !msg->command) {
    LOG_ACTION_WARN(ACT_MSG_INVALID, "err=null_command");
    return false;
  }

  uint32_t ent_int_id = 0;
  bool is_new_ent = false;
  ast_literal_node_t *ent_node = &msg->command->entity_tag_value->literal;
  char *container_name = msg->command->in_tag_value->literal.string_value;

  if (!_get_entity_mapping(worker, sys_c_ptr, sys_txn_ptr, ent_node,
                           &ent_int_id, &is_new_ent, created_sys_txn_out)) {
    LOG_ENT_ERROR(ACT_ENTITY_MAPPING_FAILED, ent_node,
                  "context=process_msg container=\"%s\"", container_name);
    return false;
  }

  uint32_t event_id = 0;
  if (!_get_next_event_id_for_container(worker, container_name, &event_id)) {
    LOG_ENT_ERROR(ACT_EVENT_ID_FAILED, ent_node, "container=\"%s\"",
                  container_name);
    return false;
  }

  if (!_write_to_event_ent_map(worker, container_name, ent_int_id, event_id)) {
    LOG_ENT_ERROR(ACT_EVENT_ID_FAILED, ent_node, "container=\"%s\"",
                  container_name);
    return false;
  }

  if (!_write_to_event_ts_map(worker, container_name, msg->command->arrival_ts,
                              event_id)) {
    LOG_ENT_ERROR(ACT_EVENT_TS_FAILED, ent_node, "container=\"%s\"",
                  container_name);
    return false;
  }

  eng_writer_msg_t *writer_msg = worker_create_writer_msg(
      msg, container_name, event_id, ent_int_id, ent_node, is_new_ent);
  if (!writer_msg) {
    LOG_ENT_ERROR(ACT_WORKER_WRITER_MSG_FAILED, ent_node, "container=\"%s\"",
                  container_name);
    return false;
  }

  // TODO: consider batching multiple groups of entries
  if (!_send_to_writer(writer_msg, worker)) {
    eng_writer_queue_free_msg(writer_msg);
    return false;
  }

  worker_ops_t ops = {0};
  worker_ops_result_t ops_result =
      worker_create_ops(msg, container_name, ent_int_id, event_id, &ops);

  if (!ops_result.success) {
    LOG_ENT_ERROR(ACT_OP_CREATE_FAILED, ent_node,
                  "container=\"%s\", err=\"%s\" context=\"%s\"", container_name,
                  ops_result.error_msg, ops_result.context);
    return false;
  }

  LOG_ENT_DEBUG(ACT_OP_CREATED, ent_node, "num_ops=%u event_id=%u", ops.num_ops,
                event_id);

  bool success = _queue_up_ops(worker, &ops);

  // ops are now owned by queues if successful, but we still need
  // to free the ops array itself
  worker_ops_clear(&ops);

  return success;
}

// Returns num messages processed
static int _do_work(worker_t *worker, eng_container_t **sys_c,
                    MDB_txn **sys_txn, bool *created_sys_txn_out) {
  size_t prev_num_msgs = 0;
  size_t num_msgs_processed = 0;
  cmd_queue_msg_t *msg = NULL;

  do {
    prev_num_msgs = num_msgs_processed;
    for (uint32_t i = 0; i < worker->config.cmd_queue_consume_count; i++) {
      uint32_t cmd_queue_idx = worker->config.cmd_queue_consume_start + i;
      cmd_queue_t *queue = &worker->config.cmd_queues[cmd_queue_idx];

      if (cmd_queue_dequeue(queue, &msg)) {
        if (_process_msg(worker, msg, sys_c, sys_txn, created_sys_txn_out)) {
          num_msgs_processed++;
        } else {
          LOG_ACTION_WARN(ACT_MSG_PROCESS_FAILED, "queue_id=%u", cmd_queue_idx);
        }

        cmd_queue_free_msg(msg);
        msg = NULL;
      }
    }
  } while (prev_num_msgs != num_msgs_processed);

  return num_msgs_processed;
}

static void _worker_cleanup(worker_t *worker) {
  if (!worker) {
    return;
  }
  size_t freed_count = 0;

  if (worker->str_to_entity_id) {
    khint_t k;
    // kh_begin returns 0
    // kh_end returns the size of the bucket array (capacity)
    for (k = kh_begin(worker->str_to_entity_id);
         k != kh_end(worker->str_to_entity_id); ++k) {
      // kh_exist checks if this specific bucket is occupied
      // (buckets can be empty or deleted)
      if (kh_exist(worker->str_to_entity_id, k)) {
        const char *key_ptr = kh_key(worker->str_to_entity_id, k);
        if (key_ptr) {
          free((void *)key_ptr);
          freed_count++;
        }
      }
    }

    kh_destroy(str_u32, worker->str_to_entity_id);
    kh_destroy(i64_u32, worker->int_to_entity_id);
    worker->str_to_entity_id = NULL;
    worker->int_to_entity_id = NULL;

    LOG_ACTION_INFO(ACT_CLEANUP_COMPLETE,
                    "context=worker entity_mappings_freed=%zu", freed_count);
  }
}

static void _worker_thread_func(void *arg) {
  worker_t *worker = (worker_t *)arg;

  log_init_worker();
  if (!LOG_CATEGORY) {
    fprintf(stderr, "FATAL: Failed to initialize logging for worker thread\n");
    return;
  }

  LOG_ACTION_INFO(ACT_THREAD_STARTED, "thread_type=worker");

  int backoff = 1;
  int spin_count = 0;
  size_t total_processed = 0;
  eng_container_t *sys_c = NULL;
  MDB_txn *sys_txn = NULL;
  bool created_sys_txn = false;
  worker->user_dcs = NULL;

  while (!worker->should_stop) {
    int processed = _do_work(worker, &sys_c, &sys_txn, &created_sys_txn);

    if (processed > 0) {
      total_processed += processed;
      backoff = 1;
      spin_count = 0;

      if (total_processed % 10000 == 0) {
        LOG_ACTION_INFO(ACT_WORKER_STATS, "msgs_processed=%zu",
                        total_processed);
      }
    } else {
      if (created_sys_txn) {
        db_abort_txn(sys_txn);
        sys_txn = NULL;
        created_sys_txn = false;
      }

      if (worker->user_dcs) {
        worker_user_dc_t *user_dc, *user_dc_tmp;

        HASH_ITER(hh, worker->user_dcs, user_dc, user_dc_tmp) {
          HASH_DEL(worker->user_dcs, user_dc);
          db_abort_txn(user_dc->txn);
          container_release(user_dc->dc);
          free(user_dc->container_name);
          free(user_dc);
        }
        worker->user_dcs = NULL;
      }

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

  if (sys_txn) {
    db_abort_txn(sys_txn);
    sys_txn = NULL;
  }
  _worker_cleanup(worker);

  LOG_ACTION_INFO(ACT_THREAD_STOPPED, "thread_type=worker total_processed=%zu",
                  total_processed);
}

worker_result_t worker_start(worker_t *worker, const worker_config_t *config) {
  if (!worker || !config) {
    return (worker_result_t){.success = false,
                             .msg = "Invalid arguments to worker_start"};
  }

  worker->config = *config;
  worker->should_stop = false;
  worker->messages_processed = 0;
  worker->str_to_entity_id = kh_init(str_u32);
  worker->int_to_entity_id = kh_init(i64_u32);

  if (uv_thread_create(&worker->thread, _worker_thread_func, worker) != 0) {
    return (worker_result_t){.success = false,
                             .msg = "Failed to create worker thread"};
  }

  return (worker_result_t){.success = true};
}

worker_result_t worker_stop(worker_t *worker) {
  if (!worker) {
    return (worker_result_t){.success = false,
                             .msg = "Invalid worker in worker_stop"};
  }

  worker->should_stop = true;

  if (uv_thread_join(&worker->thread) != 0) {
    return (worker_result_t){.success = false,
                             .msg = "Failed to join worker thread"};
  }

  return (worker_result_t){.success = true};
}

static void _global_event_id_cleanup_cb(void *key, void *value, void *ctx) {
  (void)ctx;
  char *container_name_key = (char *)key;
  atomic_uint_fast32_t *next_id_val = (atomic_uint_fast32_t *)value;

  if (container_name_key) {
    free(container_name_key);
  }

  if (next_id_val) {
    free(next_id_val);
  }
}

void worker_destroy_global(void) {
  // Note: lock_striped_ht_iterate is not thread-safe,
  // which is why we must ensure all workers are joined before calling this.
  lock_striped_ht_iterate(&g_next_event_id_by_container,
                          _global_event_id_cleanup_cb, NULL);

  lock_striped_ht_destroy(&g_next_event_id_by_container);
}