#pragma once

int route_key_to_consumer(const char *ser_db_key, int op_queue_total_count,
                          int op_queues_per_consumer);

int route_key_to_queue(const char *ser_db_key, int op_queue_total_count);