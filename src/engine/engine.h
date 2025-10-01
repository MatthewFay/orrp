#ifndef ENG_H
#define ENG_H

#include "context/context.h"
#include "query/ast.h"

#define MAX_CUSTOM_TAGS 10

typedef struct api_response_s api_response_t;

// Initialize the engine
eng_context_t *eng_init(void);
// Shut down the engine
void eng_shutdown(eng_context_t *ctx);

// Write an event
void eng_event(api_response_t *r, ast_node_t *ast);

#endif