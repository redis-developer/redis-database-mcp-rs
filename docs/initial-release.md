# Initial library and server release

The initial product boundary is deliberately library first. It ships a
composable Redis MCP surface and a thin server host; it does not ship a bespoke
human CLI/REPL.

## What ships

| Package | Role | Entry points |
| --- | --- | --- |
| `redis-mcp` | Transport-independent Tower-MCP library | `RedisMcp` router builder, Redis command-family composition, native argv policy engine, standalone and Cluster adapters |
| `redis-mcp-server` | Ready-made host around the library | MCP stdio and Streamable HTTP at `/mcp` |
| mcp-repl (external) | Interim generated human interface | Interactive REPL and one-shot/NDJSON execution derived from the server schema |

The library owns schemas, structured results, access classification,
capability requirements, output limits, and the Redis execution boundary. A
host owns transport, authenticated identity, target selection, and product
policy. The server supplies those host responsibilities for one fixed Redis
target.

redisctl adoption and a Redis-specific CLI/REPL remain follow-on work. They can
consume the same library or MCP surface, but neither is implied by the initial
release. Prebuilt binaries and container images are separate release artifacts.
The interim consumption path is GitHub source at a reviewed commit SHA;
package-specific tags and releases follow a separate reviewed release PR.
Crates.io installation is deferred.

## Use a reviewed Git source revision (interim)

The repository is public. Pin an immutable, reviewed commit SHA rather than a
moving branch or tag. For example:

```toml
[dependencies]
redis-mcp = { git = "https://github.com/redis-developer/redis-database-mcp-rs", rev = "<reviewed-commit-sha>" }
```

For a feature subset, add `default-features = false` and the required feature
list. Install the server from that same reviewed revision with:

```console
cargo install --git https://github.com/redis-developer/redis-database-mcp-rs \
  --rev <reviewed-commit-sha> --bin redis-mcp-server --locked
```

The public repository needs no source-access credentials. A GitHub release
does not make these packages available through crates.io.

## Future crates.io installation (not yet available)

Use the full library surface:

```toml
[dependencies]
redis-mcp = "0.1"
```

Use a compile-time subset:

```toml
[dependencies]
redis-mcp = { version = "0.1", default-features = false, features = ["keyspace", "strings", "hashes"] }
```

Install the server:

```console
cargo install redis-mcp-server
```

The server's dependency declares both a crates.io version and a workspace path.
Cargo removes the workspace path from the published manifest, so local
development uses the sibling crate while a future registry release can resolve
the published `redis-mcp` version. The examples in this section are plans, not
currently working crates.io installation instructions.

## Compile-time and runtime composition

The library's additive family features are `keyspace`, `strings`, `hashes`,
`lists`, `sets`, `sorted-sets`, `streams`, `bitmaps`, `arrays`, `hyperloglog`,
`geospatial`, `vector-sets`, `pubsub`, `scripting`, `json`, `search`, and
`timeseries`. Cross-cutting features are `diagnostics`, `sessions`,
`transactions`, `admin`, `bulk`, and `guidance`. `all-families` enables every
family; `full` enables all families and cross-cutting surfaces and is the
default.

Compile-time inclusion is not runtime authorization. A host still chooses
families or bundles, access mode (`read-only`, `read-write`, or `full`), raw
policy, capability behavior, and session backends. The standalone server's
default runtime surface is curated and read-only even though its default binary
contains the full feature set.

## Supported matrix

The matrix describes release-gated coverage, not every version that might work.

| Area | Release-gated support |
| --- | --- |
| Rust | 1.90 (workspace MSRV and CI toolchain) |
| MCP | `2025-11-25` and `2026-07-28`; stdio and Streamable HTTP expose the same router |
| Redis standalone | 6.2, 7.2, 7.4, 8.0, 8.2, 8.4, 8.6, 8.8, and 8.10.1 |
| Redis Cluster | Three-primary integration coverage on 6.2, 8.8, and 8.10.1 |
| Redis protocols | RESP2 and RESP3 request/reply coverage; RESP2 remains the ordinary adapter default |
| Redis Stack | `redis/redis-stack-server:7.4.0-v8` integration image |
| Modules | RedisJSON, Search/Query Engine, and RedisTimeSeries when discovered and explicitly enabled |
| Redis transport security | `redis://`, `rediss://`, authentication in URLs, and bounded connection setup through redis-tower |
| MCP HTTP security | Loopback default; Bearer authentication plus Host allowlisting required for non-loopback; Origin validation; TLS termination must be external |

The compatibility jobs run the library's live Redis contract across every
listed standalone series. The complete workspace and server stdio/HTTP suites
run against the primary CI Redis version. Dedicated jobs cover the listed
Cluster and Redis Stack pins. Capability metadata gates commands introduced by
newer Redis or module releases, so a full build does not imply every connected
target can execute every tool.

## Security and resource defaults

- Redis ACLs are the authorization boundary. MCP access levels, annotations,
  bundle selection, and raw policy are defense in depth.
- The target is fixed at startup and never accepted from tool input.
- Runtime access defaults to read-only. Administration, scripting, module
  families, raw invocation, and transactions require explicit policy.
- Unknown raw commands fail closed under the classified policy. The stronger
  unrestricted policy still blocks connection-state, streaming, replication,
  transaction, script/function, and indefinite-blocking forms that require a
  dedicated contract.
- Tool results have encoded byte and entry ceilings. Scans, fan-out,
  transactions, bulk workflows, sessions, blocking calls, HTTP bodies,
  concurrency, and request/drain duration have explicit bounds.
- Stateful Pub/Sub and MONITOR handles are scoped to a host-provided owner.
  The HTTP host binds legacy sessions and final-protocol principals to matched
  credentials and cleans Redis resources on DELETE, TTL expiry, and shutdown.
- Official Redis documentation serving is compiled into the full server but is
  disabled at runtime because it introduces outbound HTTPS.

## Intentional exclusions

The release does not provide Cloud or Enterprise REST APIs, redisctl profile
management, arbitrary per-call Redis targets, unbounded connection-stateful
commands, credential generation, filesystem configuration persistence, or
uncoordinated Cluster topology mutation. Deprecated, internal, container-only,
and unsafe commands keep explicit dispositions in the command ledger instead
of becoming accidental raw support.

The complete source-of-truth mappings are the
[core command ledger](redis-command-coverage.md), the module fixtures, and the
checked-in catalog contract snapshots. The architecture document explains the
few commands that are standalone-only or require same-slot Cluster keys.

## Release evidence

`scripts/check_release.sh` is the local and CI packaging gate. It:

1. verifies the `redis-mcp` crate with Cargo;
2. assembles both crate archives and checks their README/license contents and
   normalized dependency manifests;
3. compiles a clean-room consumer against the packaged library in minimal,
   default, and full feature modes;
4. compiles the packaged server against the packaged library in minimal,
   default, and all-feature modes; and
5. verifies family composition, command-coverage ledgers, the competitive
   surface scorecard, and the checked-in catalog/guidance snapshots.

See [releasing.md](releasing.md) for the current GitHub-only release path, the
future registry publish order, and the operator checklist.
