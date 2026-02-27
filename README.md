<p align="center">
    <img src="docs/assets/orrp.png" alt="orrp" />
</p>

# orrp

orrp is a high-performance events database that is built to be blazing fast and handle use cases like analytics, event tracking, IoT workloads, and performance/network auditing. 

If you're interested in learning more, head over to the [Docs page](https://matthewfay.github.io/orrp/). 

## Why use orrp?

orrp is built for workloads with high writes and high reads. It is an eventually consistent database that stores immutable events with key-value tags. Each event is associated with an entity, which could be a user, a service, a device, etc.

orrp exposes a SQL-like query language, and supports indexes, pagination, and range queries.

## Benchmarks

[Go here for benchmarks.](docs/benchmarks/index.md)

## Version

orrp is currently at version 0.1.0. In other words, it is a fairly new project and is still apt to change a lot. There are many features on the roadmap.

orrp follows semver.

## Questions?

If you want to contribute or have a question, feel free to open a Discussion or Issue in this repository.
