#ifndef QUEUE_H
#define QUEUE_H

#include <stdbool.h>

typedef struct queue_node_s {
  void *value;
  struct queue_node_s *next;
} queue_node_t;

// A Non-Thead safe Basic queue
typedef struct queue_s {
  queue_node_t *head; // Points to the front of the queue
  queue_node_t *tail; // Points to the back of the queue
  int size;
} queue_t;

/**
 * @brief Creates and returns a new, empty queue.
 * @return A pointer to the newly created queue, or NULL if memory allocation
 * fails.
 */
queue_t *queue_create(void);

/**
 * @brief Frees all memory associated with the queue.
 * @param q A pointer to the queue.
 */
void queue_destroy(queue_t *q);

/**
 * @brief Adds a value to the end of the queue.
 * @param q A pointer to the queue.
 * @param value The pointer to the value to be stored.
 */
void queue_enqueue(queue_t *q, void *value);

/**
 * @brief Removes and returns the value from the front of the queue.
 * @param q A pointer to the queue.
 * @return The value from the front of the queue, or NULL if the queue is empty.
 */
void *queue_dequeue(queue_t *q);

/**
 * @brief Returns the value at the front of the queue without removing it.
 * @param q A pointer to the queue.
 * @return The value from the front of the queue, or NULL if the queue is empty.
 */
void *queue_peek(const queue_t *q);

/**
 * @brief Checks if the queue is empty.
 * @param q A pointer to the queue.
 * @return True if the queue is empty, false otherwise.
 */
bool queue_empty(const queue_t *q);

/**
 * @brief Returns the number of items in the queue.
 * @param q A pointer to the queue.
 * @return The size of the queue.
 */
int queue_size(const queue_t *q);

#endif // QUEUE_H