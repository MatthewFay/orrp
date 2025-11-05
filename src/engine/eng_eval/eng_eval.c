#include "eng_eval.h"
#include "core/bitmaps.h"

bitmap_t *eng_eval_resolve_exp_to_entities(ast_node_t *node, MDB_txn *txn,
                                           eng_user_dc_t *user_db,
                                           char **err_msg) {
  // ... all the recursive logic we discussed in the last
  // message
  // (case LOGICAL_NODE, case COMPARISON_NODE, case TAG_NODE)
}