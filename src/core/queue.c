#include "core/queue.h"
#include <stdio.h>
#include <stdlib.h>

queue_t *queue_create(void) {
  queue_t *q = malloc(sizeof(queue_t));
  if (!q) {
    return NULL;
  }
  q->head = NULL;
  q->tail = NULL;
  q->size = 0;
  return q;
}

void queue_destroy(queue_t *q) {
  if (!q) {
    return;
  }

  queue_node_t *current = q->head;
  while (current != NULL) {
    queue_node_t *temp = current;
    current = current->next;
    // NOTE: We do NOT free temp->value. That is the user's responsibility.
    free(temp);
  }

  free(q);
}

void queue_enqueue(queue_t *q, void *value) {
  if (!q)
    return;

  queue_node_t *newNode = malloc(sizeof(queue_node_t));
  if (!newNode) {
    fprintf(stderr, "Error: Failed to allocate memory for new queue node.\n");
    return; // TODO: handle error more gracefully
  }
  newNode->value = value;
  newNode->next = NULL;

  if (queue_empty(q)) {
    // If the queue is empty, the new node is both the head and the tail.
    q->head = newNode;
    q->tail = newNode;
  } else {
    // Otherwise, add the new node to the end and update the tail pointer.
    q->tail->next = newNode;
    q->tail = newNode;
  }
  q->size++;
}

void *queue_dequeue(queue_t *q) {
  if (!q || queue_empty(q)) {
    return NULL;
  }

  queue_node_t *temp = q->head;
  void *value = temp->value;

  q->head = q->head->next;
  free(temp); // Free the dequeued node

  // If the queue becomes empty after the dequeue, the tail pointer must also be
  // set to NULL. Otherwise, it would be a dangling pointer, pointing to freed
  // memory.
  if (q->head == NULL) {
    q->tail = NULL;
  }

  q->size--;
  return value;
}

void *queue_peek(const queue_t *q) {
  if (!q || queue_empty(q)) {
    return NULL;
  }
  return q->head->value;
}

bool queue_empty(const queue_t *q) { return (q == NULL) || (q->size == 0); }

int queue_size(const queue_t *q) {
  if (!q)
    return 0;
  return q->size;
}