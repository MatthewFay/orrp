#ifndef STACK_H
#define STACK_H

#include <stdbool.h>
#include <stddef.h>

typedef struct stack_node_s {
  void *value;
  struct stack_node_s *next;
} stack_node_t;

typedef struct stack_s {
  struct stack_node_s *top;
  size_t count;
} c_stack_t;

c_stack_t *stack_create(void);

bool stack_is_empty(c_stack_t *stack);

bool stack_push(c_stack_t *stack, void *value);

void *stack_pop(c_stack_t *stack);

void *stack_peek(c_stack_t *stack);

void stack_free(c_stack_t *stack);

#endif
