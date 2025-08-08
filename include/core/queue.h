#ifndef QUEUE_H
#define QUEUE_H

#include <stdbool.h>

typedef struct Queue Queue;

/**
 * @brief Creates and returns a new, empty queue.
 * @return A pointer to the newly created queue, or NULL if memory allocation
 * fails.
 */
Queue *q_create(void);

/**
 * @brief Frees all memory associated with the queue.
 * @param q A pointer to the queue.
 */
void q_destroy(Queue *q);

/**
 * @brief Adds a value to the end of the queue.
 * @param q A pointer to the queue.
 * @param value The pointer to the value to be stored.
 */
void q_enqueue(Queue *q, void *value);

/**
 * @brief Removes and returns the value from the front of the queue.
 * @param q A pointer to the queue.
 * @return The value from the front of the queue, or NULL if the queue is empty.
 */
void *q_dequeue(Queue *q);

/**
 * @brief Returns the value at the front of the queue without removing it.
 * @param q A pointer to the queue.
 * @return The value from the front of the queue, or NULL if the queue is empty.
 */
void *q_peek(const Queue *q);

/**
 * @brief Checks if the queue is empty.
 * @param q A pointer to the queue.
 * @return True if the queue is empty, false otherwise.
 */
bool q_empty(const Queue *q);

/**
 * @brief Returns the number of items in the queue.
 * @param q A pointer to the queue.
 * @return The size of the queue.
 */
int q_size(const Queue *q);

#endif // QUEUE_H