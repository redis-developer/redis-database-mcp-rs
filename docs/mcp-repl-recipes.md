# Dogfooding the server with mcp-repl

`mcp-repl` is the interim human interface to `redis-mcp-server`. It discovers
the live MCP surface, turns every tool into a command, validates arguments
from JSON Schema, and supports interactive and one-shot use without adding a
terminal dependency to the library or server.

These recipes were verified with mcp-repl 0.3.8 against the stdio server. The
server also exposes the same catalog at its Streamable HTTP `/mcp` endpoint;
the repeatable campaign below remains stdio-based so it can own the child
process and its complete lifecycle.

## Build the server

Build the complete server:

```console
cargo build -p redis-mcp-server --all-features
```

To exercise compile-time family composition too, build a strings-only server
into a separate target directory:

```console
cargo build -p redis-mcp-server \
  --no-default-features --features strings \
  --target-dir target/mcp-repl-slim
```

The examples assume Redis is available at `redis://127.0.0.1:6379`.

## Interactive use

Start with the curated catalog and the access level appropriate for the
session:

```console
mcp-repl -- ./target/debug/redis-mcp-server \
  --url redis://127.0.0.1:6379 --access read-write --stdio
```

At the prompt, the normal discovery and invocation path is enough:

```text
tools
find sorted set
describe redis_zrange
redis_set key=greeting value=hello expiration={"type":"seconds","value":60}
redis_get key=greeting
```

Complex values use JSON after `=`. That preserves tagged unions, binary input,
and bounds exactly as the MCP schema declares them:

```text
redis_zrange key=board range={"kind":"score","min":{"kind":"inclusive","value":"1.5"},"max":{"kind":"positive_infinity"},"limit":20} withscores=true
redis_set key=YmluYXJ5AGtleQ== key_encoding=base64 value=/wAB value_encoding=base64
```

Capture stores the structured result directly. It is useful for pagination
and session handles:

```text
page = redis_scan pattern=order:* count=50
redis_scan pattern=order:* count=50 cursor=$page.page.continuation.cursor

sub = redis_subscribe subscriptions=[{"value":"events"}]
redis_publish channel={"value":"events"} message={"value":"hello"}
redis_pubsub_read session_id=$sub.session_id wait_ms=1000
redis_pubsub_close session_id=$sub.session_id
```

MONITOR follows the same finite session shape:

```text
monitor = redis_monitor_start
redis_monitor_read session_id=$monitor.session_id wait_ms=1000
redis_monitor_close session_id=$monitor.session_id
```

## One-shot and NDJSON use

For a raw stdio child, use repeatable `-e` commands before `--`. Every command
runs in the same MCP session:

```console
mcp-repl --json \
  -e 'redis_set key=greeting value=hello' \
  -e 'redis_get key=greeting' \
  -- ./target/debug/redis-mcp-server \
  --url redis://127.0.0.1:6379 --access read-write --stdio
```

`--json` emits one compact JSON value per command and keeps stdout
machine-only. For example:

```console
mcp-repl --json -e 'redis_get key=greeting' -- \
  ./target/debug/redis-mcp-server \
  --url redis://127.0.0.1:6379 --access read-only --stdio \
  | jq -r '.structuredContent.value'
```

mcp-repl's experimental generated CLI can be built with its
`unstable-dynamic-cli` Cargo feature. It works with named profiles and HTTP
targets. Raw stdio commands have no unambiguous boundary between server
arguments and a generated command, so `-e` remains the supported one-shot
form here.

## Schema contract

The checked contract pins `redis_zrange`, one of the surface's most demanding
schemas: defaults, tagged rank/score/lex unions, exact decimal inputs, binary
lex bounds, and output pagination.

Validate it before invocation:

```console
mcp-repl \
  --schema-contract docs/mcp-repl-contracts/redis-zrange.json \
  --schema-mode strict \
  -e 'describe redis_zrange' \
  -- ./target/debug/redis-mcp-server \
  --url redis://127.0.0.1:6379 --access read-only --stdio
```

Regenerate it intentionally after reviewing a schema change:

```text
snapshot redis_zrange docs/mcp-repl-contracts/redis-zrange.json
```

## Repeatable campaign

[`scripts/check_mcp_repl.sh`](../scripts/check_mcp_repl.sh) verifies that every
tool in the default and fully enabled catalogs can be discovered and described,
then runs set/get, captured pagination, Pub/Sub, and cleanup workflows. Point
it at the strings-only binary to include the minimal-family catalog:

```console
MCP_REPL_BIN=mcp-repl \
REDIS_URL=redis://127.0.0.1:6379 \
REDIS_MCP_SLIM_SERVER_BIN=target/mcp-repl-slim/debug/redis-mcp-server \
scripts/check_mcp_repl.sh
```

The verified catalog sizes are:

| Server shape | Access and bundles | Tools |
| --- | --- | ---: |
| Curated default | read-only | 113 |
| Fully enabled | full, raw, transactions, every optional bundle | 335 |
| Strings only | full | 19 |

Every tool in all three catalogs has a unique name, object input schema,
output schema, annotations, and a successful `describe` result.

## Known gaps and decisions

- mcp-repl 0.3.8 cancels the foreground command on Ctrl-C, but a spawned stdio
  server still receives the terminal signal and exits. Upstream issue
  [mcp-repl#278](https://github.com/joshrotenberg/mcp-repl/issues/278) tracks
  process-group isolation. Until it lands, let finite server timeouts complete
  or restart the REPL after Ctrl-C.
- The server currently advertises no task-capable tools. Its operations are
  finite and request cancellation is the current lifecycle boundary; add
  server-directed tasks only for a workflow that materially benefits from
  surviving the original call.
- Streamable HTTP sessionless callers that use Pub/Sub or MONITOR need a
  distinct configured Bearer token per isolated principal. The credential,
  rather than caller-controlled client metadata, owns reusable handles.
- mcp-repl remains an external development client. A future first-party Redis
  frontend should consume a public connection/surface/coercion/call seam if
  that core is extracted, while keeping Reedline, rendering, history, and
  Redis-specific dialect behavior in the application layer.
