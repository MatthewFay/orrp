#include "query/parser.h"
#include "core/queue.h"
#include <stdlib.h>

ast_node_t *parse(Queue *tokens) {
  if (!tokens)
    return NULL;
}