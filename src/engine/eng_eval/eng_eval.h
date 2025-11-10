#pragma once
#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include "lmdb.h"
#include "query/ast.h"

typedef struct eng_eval_result_s {
  bool success;
  const char *err_msg;
  bitmap_t *entities;
} eng_eval_result_t;

eng_eval_result_t eng_eval_resolve_exp_to_entities(ast_node_t *exp,
                                                   eng_container_t *container,
                                                   MDB_txn *user_txn,
                                                   MDB_txn *sys_txn);