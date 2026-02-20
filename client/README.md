## orrp client

This is a Go project that allows for manual/interactive testing (with msgpack decoding -> JSON), e2e, load, and benchmark testing. See below on how to run with different modes.

### Modes

1. `interactive`: `go run .` - This is the default mode. It runs a CLI to an orrp db and allows you to run commands directly. The msgpack response from the server is decoded to human readable JSON.
2. `e2e`: `go run . -mode e2e` - This will run a series of test suites against an orrp db. The primary purpose of these tests are to ensure, in an end-to-end scenario, that the db is working as expected.
3. `load`: `go run . -mode load` - This will run a load test against an orrp db, collecting stats like RPS, p95 latency, etc.
4. `bench`: `go run . -mode bench` - Runs standardized, measurable workloads (e.g., 50/50 Read/Write) and generates an HTML Performance Report tied to your local hardware.
