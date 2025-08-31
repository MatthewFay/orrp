# IMPLEMENTATION_NOTES.md

## 1. Overview

This document outlines the implementation details for `orrp`, a lightwight high-performance, event-based analytics database. The core principle of the system is to store **immutable facts** as **events**. Each `EVENT` command records a single, atomic fact, and the system is optimized for high-speed ingestion and complex, real-time queries on this event data.

The system is built in C, leveraging LMDB for storage, Roaring Bitmaps for compressed bitsets, and `libuv` for asynchronous I/O.

---

## 2. Protocol Syntax

The protocol uses a simple, space-delimited structure: `<COMMAND> <prefixed_tags...>`. All tags are prefixed, and their order does not matter.

### 2.1. The `EVENT` Command

The `EVENT` command records a new event and associates a set of tags with it. This is the primary method for writing data into the database.

* **Structure**: `EVENT in:<container> entity:<entity_id> [tag:value]...`
* **Counting**: A single tag value can be suffixed with `+` to signal that a counter should be incremented for that specific `(tag, entity)` pair. This is limited to one per command in V1.

**Examples**:
```
EVENT in:new_signups entity:user456

// A simple login event with a counter on the action
EVENT in:analytics_2025_08 entity:user123 loc:ca action:login+ day:2025_08_16

// An event with a quoted, case-sensitive tag value
EVENT in:products entity:prod_abc name:"Big Widget" category:electronics
```

### 2.2. The `QUERY` Command

The `QUERY` command executes a query against one or more data containers.

* **Structure**: `QUERY in:<container> [in:<container2>...] exp:(<expression>) [take:<limit>] [cursor:<composite_key>]`
* **Expression**: The query logic must be wrapped in `exp:(...)`. It supports bitwise `AND`, `OR`, `NOT` operators for event membership and comparison operators (`>`, `<`, `=`, etc.) for event counts.

**Examples**:
```
// Find users in California who have logged in more than 3 times
QUERY in:analytics_2025_01 exp:(loc:ca AND (action:login > 3))

// Cross-container query with pagination (Will be in a future version)
QUERY in:analytics_2025_08 in:analytics_2025_07 exp:(view:prod_123 > 3) take:100 cursor:analytics_2025_08_54321
```

---

## 3. Core Concepts

### 3.1. Data Containers

* A **Data Container** is a single LMDB database file (e.g., `analytics_2025_08.mdb`).
* This design provides **isolation**, **portability**, and simplifies **purging/archiving** (e.g., deleting a file to delete a month's data).
* There are two types:
    * **System Container**: A single LMDB file (`system.mdb`) containing the global entity directory.
    * **User Container**: Multiple files for user-defined data, typically partitioned by time.

### 3.2. IDs: Entities vs. Events

The distinction between Entity and Event IDs is critical for performance and scalability.

* **Entity IDs (`uint32_t`)**: Represents a unique entity (user, product, etc.) **across the entire system**. They are managed by a **global directory** to ensure consistency. `uint32_t` is used as it's the optimal size for Roaring Bitmaps, supporting up to ~4.2 billion unique entities.
* **Event IDs (`uint32_t`)**: Represents a unique event **only within its parent data container**. Each container has its own independent event counter. This avoids a global write bottleneck and allows all event-based indexes to use the highly optimized 32-bit Roaring Bitmaps.

### 3.3. Concurrency Model

The engine will operate with a **single writer thread** and can support **multiple reader threads**, which is a perfect fit for LMDB's architecture. This simplifies the design by eliminating the need for complex locking on writes.

---

## 4. Data Storage Model

### 4.1. The System Container (Global Directory)

The `system.mdb` file will contain key LMDB databases (DBIs) for global state:

* `dbi_entity_str_to_int`: Maps entity strings (`user123`) to `uint32_t` IDs.
* `dbi_entity_int_to_str`: Maps `uint32_t` IDs back to strings for results.
* `dbi_global_counters`: Stores the `last_entity_id` counter (`uint32_t`).

### 4.2. The User Container (Event Data)

Each user data container (e.g., `analytics_2025_08.mdb`) will contain the following DBIs:

* **`dbi_inverted_index`**: The core of the query engine.
    * **Key**: The tag (e.g., `loc:ca`).
    * **Value**: A Roaring Bitmap of all local `event_id`s that have this tag.
* **`dbi_event_to_entity`**: A map to find out who performed an event.
    * **Key**: The local `event_id` (`uint32_t`).
    * **Value**: The global `entity_id` (`uint32_t`) associated with the event.
* **`dbi_metadata`**: Stores container-specific state, like the `last_event_id` counter for this container.
* **`dbi_counter_store`**: Stores the raw counts for countable tags.
    * **Key**: A composite of `(tag, entity_id)`.
    * **Value**: The `uint32_t` count.
* **`dbi_count_index`**: An inverted index for fast count-based queries.
    * **Key**: A composite of `(tag, count)`.
    * **Value**: A Roaring Bitmap of `entity_id`s that currently have that exact count for that tag.

This event context is the most powerful feature of the database.

---

## 5. Query Execution Flow & Pagination

A complex, cross-container query is executed by processing each container independently and then merging the final results.

1.  **Parse & Plan**: The command is parsed. The engine identifies the target containers and the query clauses.
2.  **Execute in Each Container**: For each container specified (`in:...`), the engine performs the following steps:
    * Solve all membership clauses (`loc:ca`, etc.) by querying `dbi_inverted_index` to get bitmaps of local **event IDs**.
    * Perform bitwise `AND`/`OR`/`NOT` on these event bitmaps to get a final set of matching event IDs for that container.
    * **Translate to Entities**: The final event bitmap is converted into a bitmap of global **entity IDs** using the container's `dbi_event_to_entity` map.
    * Solve any count clauses (`action:login > 3`) by querying `dbi_count_index` to get a bitmap of global **entity IDs**.
    * Perform a final `AND` between the membership results and count results to get the definitive bitmap of matching entity IDs for that container.
3.  **Combine Final Results**: The engine takes the entity ID bitmaps from each container and performs a final **bitwise OR** to produce the complete result set.
4.  **Return Results**:
    * The `take:<limit>` value determines how many results to return.
    * The engine iterates through the final entity ID bitmap, looks up the string for each `uint32_t` ID in the global directory, and returns the list.
    * **Pagination**: High-performance, cursor-based pagination will be supported using a composite key. The server returns a `cursor` like `"analytics_2025_08_54321"` (`{container_name}_{last_event_id}`). The client sends this back on the next request, allowing the engine to efficiently resume the query from the correct position.

### Counters and Cumulative Bitmaps

* Count events per entity id and create bitmaps that support flexible range queries (>, <, ==, etc.).
* The correct approach is to decompose the data and let LMDB manage the individual counters and bitmaps.
* The key insight is to use **cumulative bitmaps**, where a entity, once added to a count's bitmap, is never removed.

### Querying with Cumulative Bitmaps

This data structure makes range queries extremely efficient. Let's assume B(N) is the bitmap retrieved for count N.

`count >= N`: Simply retrieve B(N). This is a single database lookup.

`count > N`: Simply retrieve B(N+1). This is also a single database lookup.

`count == N`: This requires a set difference operation: B(N) ANDNOT B(N+1). This involves two lookups and one very fast bitmap operation.

`count < N`: Retrieve everyone who has done the event at least once, and subtract everyone who has done it N or more times: B(1) ANDNOT B(N).

`count <= N`: Similarly, B(1) ANDNOT B(N+1).

---

## 6. Lexing and Parsing

* The **tokenizer** will convert the input string into a stream of tokens. It is simple and stateless.
* The **parser** will consume the token stream and validate it against the language grammar.
* **Case Sensitivity**:
    * Unquoted values (command names, tags, unquoted values like `ca`) are **case-insensitive** and should be lowercased by the tokenizer.
    * Quoted string literals (`"Big Widget"`) are **case-sensitive**, and the tokenizer will pass their exact contents (without quotes) to the parser.
* **Key Token Types**: `TOKEN_IDENTIFIER` (for unquoted text), `TOKEN_LITERAL_STRING`, `TOKEN_LITERAL_NUMBER`, and specific tokens for keywords (`TOKEN_KW_IN`, `TOKEN_KW_ENTITY`, etc.).

---

## 7. Engine Architecture

* The `main()` function will call an `engine_init()` function.
* This will return a pointer to an `Engine` context struct, which holds the handle to the system container and other global state. This `Engine*` will be passed to all top-level API functions.
* The engine will maintain an **LRU cache** (using `uthash`) of open user data container handles (`char* name -> DataContainer*`) to avoid the overhead of constantly opening and closing LMDB files.

---

## 8. Future Considerations (V2+)

The following features are explicitly deferred from V1 to keep the initial implementation focused.

* **Idempotency**: Will be implemented using an event id (`id:<event id>`) passed from client.
* **Namespaces**: Support for `namespace::key:value` to avoid tag key collisions.
* **Request/Correlation ID**: Allowing clients to pass an ID that is returned with the response, to help map asynchronous requests.
* **Conditional/Bulk Writes**: Advanced write commands like `EVENT ... if:(expression)` or creating events from query results.
* Comparison query (counts) using tag key. For example, we have tag "viewed_product:prod123". Querying on "viewed_product" > 3

## 9. Testing

* Unit tests using Unity
* E2E tests using client (TBD tech)
* Valgrind for memory testing
* Performance/load testing

## 10. SDK

* In future, will build a client SDK.


