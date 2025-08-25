# Technical Design: Write-Ahead Log (WAL) Architecture

**Author:** Gemini & Matt Fay

**Date:** August 24, 2025

**Status:** Final

## 1. Overview

This document describes the design for a Write-Ahead Log (WAL) architecture to significantly improve the database's write performance and provide durability against crashes. The current model, which performs a direct read-modify-write cycle to LMDB for every tag in every event, creates a performance bottleneck.

This new architecture decouples the fast ingestion path from the slower persistence path. Ingest threads will perform a quick, sequential write to a log file and update an in-memory cache before returning a success response to the client. A separate background thread will then asynchronously write these changes from the in-memory cache to the main LMDB data store.

## 2. Goals and Non-Goals

### 2.1. Goals
* **High Write Throughput:** Drastically reduce the latency of write commands (`EVENT`).
* **Durability:** Ensure that acknowledged writes are not lost in the event of a server crash.
* **Eventual Consistency:** Data will be immediately available for reads from memory and will be persisted to the main data store within a short, configurable time frame.

### 2.2. Non-Goals
* This design does not cover the full implementation of the `QUERY` read path.
* This design does not address horizontal scaling, sharding, or replication.

## 3. High-Level Architecture

The system will be composed of four main components: Ingest Threads, a Write-Ahead Log, an In-Memory Data Cache, and a Background Writer Thread.

1.  A client sends an `EVENT` command.
2.  An **Ingest Thread** receives the command.
3.  The Ingest Thread appends the command to the **Write-Ahead Log (WAL)** file on disk and performs an `fsync`. If this fails, an error is returned to the client.
4.  The Ingest Thread updates the **In-Memory Cache**, modifying the relevant data objects (e.g., bitmaps) and marking them as "dirty."
5.  The Ingest Thread responds **OK** to the client.
6.  Independently, the **Background Writer** thread wakes up, finds the "dirty" objects in the cache, and writes them to the main **LMDB Data Store**.

---

## 4. Component Deep Dive

### 4.1. In-Memory Data Cache

A single, global, shared cache for deserialized data objects (primarily `bitmap_t` pointers).

* **Implementation:** A `uthash` hash map combined with a doubly-linked list for LRU management.
* **Key:** A generic, composite string key: `"container_name:db_name:key"`.
    * Example 1: `"analytics_2025:inverted_event_index_db:loc:sf"`
    * Example 2: `"analytics_2025:count_index_db:purchase_count:prod123"`
* **Value:** A cache node struct containing:
    * A pointer to the data object (e.g., `bitmap_t*`).
    * A boolean `is_dirty` flag.
    * Pointers for the LRU list (`prev`/`next`).
    * Pointers for a separate "dirty list."
Example:
```c
typedef struct EnvCacheNode {
  // --- Hash map fields ---
  char* key;
  UT_hash_handle hh;

  // --- LRU List fields ---
  struct EnvCacheNode* prev;
  struct EnvCacheNode* next;

  // --- Dirty List fields ---
  struct EnvCacheNode* dirty_prev;
  struct EnvCacheNode* dirty_next;

  // --- Data payload & State ---
  void* data_object; // e.g., bitmap_t*
  bool is_dirty;
} EnvCacheNode;
```
* **Sizing & Eviction Policy:** The cache will be size-limited by an LRU policy. When a cache miss occurs and the cache is full, the least recently used item will be evicted. To prevent blocking the ingest thread, the eviction logic is as follows:
    * The ingest thread unlinks the node from the main cache's hash map and LRU list.
    * If the node is "dirty," it is **left on the dirty list**.
    * The background writer is then responsible for the final flush to LMDB and the deallocation of the node's memory. *This ensures only the background writer ever performs slow writes.*
* **Locking:** Access will be protected by a `uv_rwlock_t`. `EVENT` commands will take a **write lock**, while future `QUERY` commands will take a **read lock**.

### 4.2. Write-Ahead Log (WAL)

Provides the durability guarantee.

* **Scope & Concurrency:** A single, global log. Appends from multiple ingest threads will be serialized using a dedicated `uv_mutex_t`.
* **Format:** A binary, append-only file using a **Type-Length-Value (TLV)** format.
* **Log Segmentation:** The WAL will be split into segments (e.g., `wal.00001`, `wal.00002`). When a segment reaches a predefined size (e.g., 64MB), it will be closed, and a new one will be opened. This rotation will be performed by whichever ingest thread happens to be writing when the size limit is exceeded, protected by the WAL mutex.

#### WAL for Internal Commands
The WAL is not just for user commands; it is the source of truth for all durable state changes.

If an operation like incrementing an ID counter needs to survive a crash (which it does, to prevent reusing IDs), it must be recorded in the WAL before the in-memory state is changed. The background writer is responsible for eventually persisting that state to LMDB, but the WAL provides the immediate durability guarantee.

### 4.3. The "Dirty List"

A dedicated queue of cache nodes that need to be written to disk.

* **Implementation:** A simple, standalone doubly-linked list.
* **Function:** When an ingest thread modifies a cache node, it checks the `is_dirty` flag. If `false`, it sets it to `true` and appends the node to the tail of the dirty list. This O(1) check prevents duplicate entries.
* **Locking:** Access to the dirty list will be protected by its own `uv_mutex_t`.

### 4.4. Background Writer Thread

A single, dedicated thread (`uv_thread_t`) for persisting data.

* **Trigger:** Wakes up periodically (e.g., every 100ms).
* **Operation (Lock-and-Swap):**
    1.  Acquires the mutex for the dirty list.
    2.  **Swaps** the global dirty list head pointer with a local one, making the global list empty.
    3.  **Immediately releases the mutex.** This minimizes the time ingest threads are blocked.
    4.  Processes its local list of dirty nodes:
        * Starts a single LMDB write transaction per container.
        * For each node, serializes the data object and writes it to the appropriate LMDB database.
        * Commits the transaction. If the commit fails, the nodes remain on the local list to be retried on the next pass.
        * After a successful commit, the nodes are cleared from the local list and can be considered fully persisted.

**Important: Instead of processing one dirty item at a time, the background writer should process a whole batch of dirty items within a single, larger LMDB transaction.**

---

## 5. Phased Implementation Plan

This architecture can be built serially in four distinct phases.

### Phase 1: Implement the In-Memory Cache
* **Goal:** Achieve an immediate performance boost for hot data reads and writes.
* **Steps:**
    1.  Create the unified cache node struct and the main cache manager struct.
    2.  Implement the `uthash` and LRU list logic.
    3.  Integrate the `uv_rwlock_t` to make the cache thread-safe.
    4.  Modify `eng_event` to use the cache: on a write, check the cache first. If an object is present, update it in memory. If not, load it from LMDB, add it to the cache, then update it.
    5.  **At this stage, all writes still go directly to LMDB.** The cache only helps avoid re-reading data.

### Phase 2: Implement the Background Writer, Dirty List, and multiple ingest threads
* **Goal:** Decouple the ingest path from slow LMDB transaction commits. Then parallelize the now-fast ingest path to handle more concurrent client connections.
* **Steps:**
    1.  Implement the Dirty List: Create the standalone dirty list, its mutex, and add the necessary flags and pointers to your main cache node struct.
    2.  Create the Background Writer: Implement the single background writer thread with its "lock-and-swap" logic to process the dirty list.
    3.  Modify `eng_event`: when an object is updated in the cache, set its `is_dirty` flag and add it to the dirty list. **Remove the direct write to LMDB**.
    4. Introduce libuv Worker Threads: This is the final step. With all the locking and asynchronous logic in place, you can now safely dispatch your eng_event calls to the libuv thread pool (uv_queue_work). Since the function is now thread-safe, you can immediately scale your ingestion capabilities.

### Phase 3: Implement the WAL for Durability
* **Goal:** Add crash safety.
* **Steps:**
    1.  Design and implement the binary TLV format for `EVENT` commands.
    2.  Implement the WAL append logic, including the mutex and `fsync` call.
    3.  Integrate the WAL append call into the `eng_event` function. This must happen *before* the in-memory cache is updated. An error here must be returned to the client.
    4.  Implement the crash recovery logic on server startup: check for a "clean shutdown" flag. If not present, read and replay the WAL entries to rebuild the in-memory cache state.

### Phase 4: Implement Checkpointing & Log Deletion
* **Goal:** Ensure the WAL does not grow indefinitely.
* **Steps:**
    1.  Implement WAL segmentation logic in the ingest path.
    2.  Implement a checkpointing mechanism. The background writer, after a successful flush, will record a checkpoint (e.g., the last fully persisted WAL segment number) into a metadata DBI in the system container.
    3.  Create a cleanup task that periodically checks the current checkpoint and safely deletes any older, obsolete WAL segments.
  
## WAL

A WAL entry needs to be a self-contained, binary representation of the command. A simple TLV (Type-Length-Value) format is perfect for this. Each record would start with a header containing a checksum (for integrity) and the total length of the record.

Here’s what a WAL record for `EVENT in:analytics entity:user123 loc:sf` could look like:

| Field | Size (bytes) | Example Value | Description |
|---|---|---|---|
| **Record Header** | | | |
| CRC32 Checksum | 4 | `0xAB12CD34` | Checksum of the entire record to detect corruption. |
| Record Length | 4 | `55` | Total size of the record in bytes (excluding this field). |
| **Record Body** | | | |
| Command Type | 1 | `0x01` | A byte representing the `EVENT` command. |
| Container Name | 1 + 4 + N | `T:0x02, L:9, V:"analytics"` | A TLV field for the `in:` tag. |
| Entity ID | 1 + 4 + N | `T:0x03, L:7, V:"user123"` | A TLV field for the `entity:` tag. |
| Tag 1 Key | 1 + 4 + N | `T:0x04, L:3, V:"loc"` | A TLV field for the first custom tag's key. |
| Tag 1 Value | 1 + 4 + N | `T:0x05, L:2, V:"sf"` | A TLV field for the first custom tag's value. |

### WAL Replay Process

The replay process must be simple and deterministic to guarantee correctness.

#### File and Entry Order
File Order: You are correct that the replay must happen in order. The WAL segments are named with an increasing sequence number (e.g., wal.00001, wal.00002). On startup, the recovery process finds the last successful checkpoint, determines the first WAL segment after that checkpoint, and then reads every segment in strict numerical order.

Entry Order: Since the WAL is an append-only log, the entries within each file are already in the exact order they were originally executed. By reading the file from beginning to end, you are replaying the events in their original sequence.

#### Checking for Persistence
This is a key point: during a crash recovery, the replay process does not check if an entry has already been persisted to LMDB.

It operates under the assumption that the LMDB state might be stale and that the WAL is the "source of truth." The goal is simply to rebuild the in-memory cache to its correct, pre-crash state. If the replay process adds an ID to an in-memory bitmap that was already saved to disk, that's fine. The operation is idempotent—running it multiple times has the same result as running it once. This "dumb" replay is much faster and simpler than trying to cross-reference with the main database.

#### Replay Action: In-Memory vs. Disk
For a fast recovery, the replay process should only update the in-memory cache.

Writing every replayed event to LMDB during startup would be extremely slow and would significantly increase the server's downtime after a crash.

The correct recovery flow is:

1. Read the WAL entries sequentially.
2. For each entry, apply the operation to the in-memory cache (e.g., load the bitmap if needed and add the ID).
3. Mark all the replayed cache items as "dirty."
4. Once the entire WAL is replayed, the server can begin accepting new connections.

The background writer thread then starts its normal job, and over its next few cycles, it will automatically flush all the "dirty" data from the rebuilt cache to the LMDB store.

