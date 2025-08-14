#include "core/queue.h"
#include <stdio.h>
#include <stdlib.h>

typedef struct Node {
  void *value;
  struct Node *next;
} Node;

struct Queue {
  Node *head; // Points to the front of the queue
  Node *tail; // Points to the back of the queue
  int size;
};

Queue *q_create(void) {
  Queue *q = malloc(sizeof(Queue));
  if (!q) {
    return NULL;
  }
  q->head = NULL;
  q->tail = NULL;
  q->size = 0;
  return q;
}

void q_destroy(Queue *q) {
  if (!q) {
    return;
  }

  Node *current = q->head;
  while (current != NULL) {
    Node *temp = current;
    current = current->next;
    // NOTE: We do NOT free temp->value. That is the user's responsibility.
    free(temp);
  }

  free(q);
}

void q_enqueue(Queue *q, void *value) {
  if (!q)
    return;

  Node *newNode = malloc(sizeof(Node));
  if (!newNode) {
    fprintf(stderr, "Error: Failed to allocate memory for new queue node.\n");
    return; // TODO: handle error more gracefully
  }
  newNode->value = value;
  newNode->next = NULL;

  if (q_empty(q)) {
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

void *q_dequeue(Queue *q) {
  if (!q || q_empty(q)) {
    return NULL;
  }

  Node *temp = q->head;
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

void *q_peek(const Queue *q) {
  if (!q || q_empty(q)) {
    return NULL;
  }
  return q->head->value;
}

bool q_empty(const Queue *q) { return (q == NULL) || (q->size == 0); }

int q_size(const Queue *q) {
  if (!q)
    return 0;
  return q->size;
}