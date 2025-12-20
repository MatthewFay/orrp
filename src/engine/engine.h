#ifndef ENG_H
#define ENG_H

#include "query/ast.h"

typedef struct api_response_s api_response_t;

// Initialize the engine
bool eng_init(void);
// Shut down the engine
void eng_shutdown(void);

// Write an event
void eng_event(api_response_t *r, ast_node_t *ast, int64_t arrival_ts);

// Query
void eng_query(api_response_t *r, ast_node_t *ast);

#endif