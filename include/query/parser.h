#ifndef PARSER_H
#define PARSER_H

#include "query/ast.h"
#include "query/tokenizer.h"
ast_node_t *parse(token_t *tokens);

#endif // PARSER_H
