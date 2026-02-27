# Getting Started with orrp

Welcome! This guide will help you set up orrp, build it from source, and start writing data and running queries.

## Prerequisites

orrp is a C/C++ project with several dependencies. Make sure you have:

- **C Compiler**: GCC (C11 standard support required)
- **Build Tools**: `make`
- **Git**: For cloning the repository
- **Go** (1.19+): Optional, only needed to use the interactive client and run tests

## Building orrp

orrp uses a Makefile-based build system. First, clone the repository:

```bash
git clone https://github.com/MatthewFay/orrp.git
cd orrp
```

### Build Modes

orrp supports two build modes:

- **Debug Mode** (default): Optimizations disabled, debug symbols enabled, verbose logging
- **Release Mode**: Full optimizations (O3), SIMD instruction sets, link-time optimization

### Debug Build

For development and testing:

```bash
make
```

This creates the executable at `bin/orrp`.

### Release Build

For production use:

```bash
make BUILD=release
```

This builds with maximum optimizations, producing a significantly faster binary.

## Running orrp

Once built, start the server:

```bash
./bin/orrp
```

By default, orrp listens on `127.0.0.1:7878`. The server will create a data directory at `data` to store your databases. You should see output indicating the server is ready to accept connections.

## Using the Interactive Client

orrp comes with a Go client for interactive queries. Navigate to the `client` directory:

```bash
cd client
go run . -addr 127.0.0.1:7878
```

You'll see a prompt where you can type commands:

```
------------------------------------------------
Connected to orrp at 127.0.0.1:7878
Type 'exit' to quit.
------------------------------------------------
> 
```

## Basic Commands

### Ingesting Events

Insert an event with key-value tags:

```
EVENT in:mydb entity:user1 action:login location:us
```

This creates an event in namespace `mydb`, associated with entity `user1`, with tags:
- `action: login`
- `location: us`

Multiple events can be created:

```
EVENT in:mydb entity:user2 action:logout location:eu
EVENT in:mydb entity:user3 action:login location:ca
```

### Querying Events

Query events using a WHERE clause with tag-based filtering:

```
QUERY in:mydb where:(location:us)
```

This returns all events in namespace `mydb` where `location` equals `us`.

More complex queries:

```
QUERY in:mydb where:(location:us AND type:login)
```

This returns events matching both conditions (AND operation). orrp supports complex nested queries, including AND, OR, and NOT.

**Note**: orrp is **eventually consistent**. There may be a slight delay before newly written events appear in query results.

## Testing

### Running Unit Tests

Build and run all unit tests:

```bash
make test
```

This compiles and executes tests for the core data structures, database engine, query parser, and other components.

To only build tests without running them:

```bash
make test_build
```

### End-to-End Testing

The Go client includes comprehensive end-to-end tests. From the `client` directory:

```bash
go run . -mode e2e
```

This runs test suites covering:
- **ingest**: Basic event ingestion
- **query**: Querying with filters and conditions
- **pagination**: Result pagination
- **robustness**: Edge cases and error handling

Run specific test suites:

```bash
go run . -mode e2e -suites ingest,query
```

### Load Testing

Measure performance under concurrent load:

```bash
go run . -mode load -workers 20 -duration 5
```

This launches 20 concurrent workers for 5 seconds, reporting:
- Operations per second (RPS)
- Latency percentiles (p50, p95, p99)
- Total operations completed

### Performance Benchmarks

Generate an HTML performance report tailored to your hardware:

```bash
go run . -mode bench
```

This runs standardized workloads (e.g., 50/50 read/write mix) and generates a detailed report comparing performance across different scenarios.

## Project Structure

- `src/` - C source code for the database engine
- `include/` - Header files for the engine and query parser
- `lib/` - Third-party dependencies (libuv, LMDB, Roaring Bitmaps, etc.)
- `client/` - Go client for development, testing, and benchmarking
- `tests/` - Unit tests for core components
- `docs/` - Documentation (this site)

## Key Concepts

### Namespaces

Each database in orrp is a separate namespace. Events are isolated per namespace. It is recommended to partition namespaces by time, but this is not required. Examples: `analytics_02_2026`, `audit_logs_01_01_2026`, `metrics_03_01_2026`.

By using namespaces correctly, it makes backups/deletion of data trivial.

### Entities

Events are associated with an entity (e.g., a user, device, or service). This allows efficient querying of events for a particular entity.

### Tags

Events contain arbitrary key-value tags. Examples:
- `user_id:123`
- `action:purchase`
- `amount:99.99`
- `country:US`

Tags are used in WHERE clauses to filter events.

### Immutability

Events in orrp are immutable. Once written, they cannot be modified or deleted. This design choice enables high write throughput and consistency guarantees.

### Eventual Consistency

orrp prioritizes write performance over immediate consistency. When you write an event, it's acknowledged immediately, but it may take a few milliseconds before it appears in query results.

## Next Steps

- **Learn the Query Language**: See [Query Language](query-language.md) for detailed syntax and examples
- **Understand Architecture**: Check [Architecture](architecture.md) for technical details
- **Check Benchmarks**: Review performance characteristics in [Benchmarks](benchmarks/index.md)
- **Contribute**: Visit the [GitHub repository](https://github.com/MatthewFay/orrp) to contribute or ask questions

## Questions?

For issues, feature requests, or questions, open an issue or discussion on [GitHub](https://github.com/MatthewFay/orrp).