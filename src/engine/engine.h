#ifndef ENG_H
#define ENG_H

#include "context.h"
#include "query/ast.h"

#define MAX_CUSTOM_TAGS 10
// TODO: db_constants.h
extern const char *SYS_NEXT_ENT_ID_KEY;
extern const u_int32_t SYS_NEXT_ENT_ID_INIT_VAL;
extern const char *SYS_DB_METADATA_NAME;
extern const char *USR_NEXT_EVENT_ID_KEY;
extern const u_int32_t USR_NEXT_EVENT_ID_INIT_VAL;
extern const char *USR_DB_METADATA_NAME;

typedef struct api_response_s api_response_t;

// Initialize the engine
eng_context_t *eng_init(void);
// Shut down the engine
void eng_shutdown(eng_context_t *ctx);

// Write an event
void eng_event(api_response_t *r, eng_context_t *ctx, ast_node_t *ast);

#endif