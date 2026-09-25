# redis-mcp-server

`redis-mcp-server` is the thin standalone host for the `redis-mcp` library. It
assembles one fixed Redis target and serves the same typed surface over MCP
stdio or Streamable HTTP. It is a server and one-shot process, not the planned
first-party Redis CLI/REPL.

## Install

```console
cargo install redis-mcp-server
```

Run over stdio:

```console
redis-mcp-server \
  --url redis://127.0.0.1:6379 \
  --access read-write \
  --stdio
```

Run over Streamable HTTP:

```console
REDIS_MCP_HTTP_BEARER_TOKEN='replace-with-a-secret' \
  redis-mcp-server \
    --url redis://127.0.0.1:6379 \
    --access read-write \
    --http 127.0.0.1:8080
```

Loopback is the default. A non-loopback bind requires explicit remote
acknowledgement, Bearer authentication, and a Host allowlist. The built-in
listener is plain HTTP; terminate TLS before exposing it over an untrusted
network. The Redis target may use `rediss://` for TLS.

## Configuration and deployment

CLI values override environment variables, which override an explicitly
selected TOML file, which overrides built-in defaults. HTTP Bearer tokens are
environment-only so they do not appear in process arguments or checked-in
configuration. Redis credentials may be supplied in the configured target URL.
The packaged `redis-mcp.example.toml` lists every non-secret setting and its
default.

The default binary compiles the full library plus the opt-in command-document
fetcher. Runtime defaults remain curated and read-only. Slim builds mirror the
library feature names:

```console
cargo install redis-mcp-server --no-default-features \
  --features keyspace,strings,hashes,diagnostics
```

Use repeated `--cluster-url` values or `REDIS_CLUSTER_URLS` for a Cluster
target. RedisJSON, Search, TimeSeries, scripting, administration, raw
invocation, and transactions require explicit runtime policy where applicable.
Capability discovery can advertise unavailable tools with a stable preflight
error or hide them.

Durable Redis Streams-backed agent handoffs are also opt-in:

```console
redis-mcp-server --url redis://127.0.0.1:6379 --access full \
  --enable-bundle coordination --stdio
```

The bundle exposes bounded publish, claim, MRTR approval, complete, status,
and recovery tools plus `redis-mcp://guidance/agent-handoffs`. Recovery is the
only full-access operation; the ordinary lifecycle requires read-write access.
Approval requires a 2026-07-28 client that advertises form elicitation.

## Interim human interface

Until a first-party Redis CLI/REPL ships, mcp-repl derives interactive and
one-shot commands directly from this server's MCP schemas:

```console
mcp-repl -- redis-mcp-server --url redis://127.0.0.1:6379 --stdio
mcp-repl --json --exec 'redis_ping' -- \
  redis-mcp-server --url redis://127.0.0.1:6379 --stdio
```

See the repository's
[verified recipes](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/mcp-repl-recipes.md)
for schemas, pagination, sessions, and scripting.

## Documentation and support

The [initial release boundary and compatibility matrix](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/docs/initial-release.md)
documents supported Redis/module versions, MCP protocols, security defaults,
and intentional exclusions. The full
[server configuration](https://github.com/redis-developer/redis-database-mcp-rs/blob/main/crates/redis-mcp-server/redis-mcp.example.toml)
is versioned with the binary.

Future redisctl adoption and a first-party Redis CLI/REPL are separate follow-on
work; they are not included in this package.

## License

MIT or Apache-2.0, at your option.
