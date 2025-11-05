#pragma once
#include "core/bitmaps.h"
#include "engine/container/container_types.h"
#include "lmdb.h"
#include "query/ast.h"

bitmap_t *eng_eval_resolve_exp_to_entities(ast_node_t *node, MDB_txn *txn,
                                           eng_user_dc_t *user_db,
                                           char **err_msg);