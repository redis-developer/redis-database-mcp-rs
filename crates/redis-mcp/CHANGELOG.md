# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0](https://github.com/redis-developer/redis-database-mcp-rs/releases/tag/redis-mcp-v0.1.0) - 2026-10-22

Initial release of the transport-independent Redis database MCP library. See
[the initial release boundary](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/initial-release.md)
for the supported Redis, module, and MCP matrix.

### Added

- `RedisMcp` builder that composes Tower-MCP routers for one fixed Redis
  target, with standalone and Redis Cluster adapters built on redis-tower
  (`redis://` and `rediss://`).
- Additive Cargo features per command family: `keyspace`, `strings`,
  `hashes`, `lists`, `sets`, `sorted-sets`, `streams`, `bitmaps`, `arrays`,
  `hyperloglog`, `geospatial`, `vector-sets`, `pubsub`, `scripting`, `json`,
  `search`, and `timeseries`, plus `diagnostics`, `sessions`, `transactions`,
  `admin`, `bulk`, `coordination`, and `guidance`. The default `full` feature
  enables all of them.
- Runtime access modes (`read-only` by default, `read-write`, `full`), family
  and bundle selection, and a governed native argv invocation engine whose
  raw-command policy fails closed on unknown commands.
- Capability metadata that gates tools by Redis version and by the RedisJSON,
  Search/Query Engine, and RedisTimeSeries modules.
- Encoded byte and entry budgets on tool output, and explicit bounds on scans,
  fan-out, blocking calls, transactions, bulk load and seed workflows, and
  Pub/Sub and MONITOR sessions scoped to a host-provided owner.
- Redis Streams-backed agent coordination with durable approval elicitation
  (`coordination` feature).
- Curated Redis guidance as MCP prompts and resources, and official Redis
  command documentation as resources when the host supplies a fetcher.
- `validate_redis_target_url` for offline validation of a fixed Redis target.
- MCP protocol versions `2025-11-25` and `2026-07-28`. Minimum supported Rust
  version 1.90.
