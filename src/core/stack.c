#include "core/stack.h"
#include <stdlib.h>

c_stack_t *stack_create() {
  c_stack_t *stack = calloc(1, sizeof(c_stack_t));
  if (!stack)
    return NULL;
  stack->top = NULL;
  return stack;
}

bool stack_is_empty(c_stack_t *stack) { return stack->top == NULL; }

bool stack_push(c_stack_t *stack, void *value) {
  stack_node_t *node = calloc(1, sizeof(stack_node_t));
  if (!node)
    return false;
  node->value = value;
  node->next = stack->top;
  stack->top = node;
  stack->count++;
  return true;
}

void *pop(c_stack_t *stack) {
  if (stack_is_empty(stack))
    return NULL;
  stack_node_t *temp_node = stack->top;
  void *value = temp_node->value;
  stack->top = temp_node->next;
  free(temp_node);
  stack->count--;
  return value;
}

void *stack_peek(c_stack_t *stack) {
  if (stack_is_empty(stack))
    return NULL;
  return stack->top->value;
}

// Does not free stored values
void stack_free(c_stack_t *stack) {
  while (!stack_is_empty(stack)) {
    pop(stack);
  }
  free(stack);
}
