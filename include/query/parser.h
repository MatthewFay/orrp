#ifndef PARSER_H
#define PARSER_H

#include "core/queue.h"
#include "query/ast.h"
ast_node_t *parse(Queue *tokens);

#endif // PARSER_H
