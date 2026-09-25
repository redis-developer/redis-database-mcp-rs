# Release process

`redis-mcp-server` depends on `redis-mcp`, so crates are published in that
order. Keep their workspace versions in lockstep for the initial release line.

## Prerequisites

- use the Rust toolchain declared by `workspace.package.rust-version`;
- start from a clean commit on the intended release branch;
- verify the Redis, Redis Stack, and Cluster CI matrix is green;
- reconcile any deliberate public schema changes in
  `crates/redis-mcp/tests/snapshots`; and
- reconcile changes to Redis's command inventory in the pinned coverage
  ledgers rather than weakening the drift checks.

## Package gate

Run:

```console
./scripts/check_release.sh
```

The script builds from Cargo's `.crate` archives in a temporary directory. It
does not treat the workspace checkout as proof that the published packages are
self-contained.

Inspect the exact payloads when needed:

```console
cargo package -p redis-mcp --list
cargo package -p redis-mcp-server --no-verify --list
```

The server cannot be independently verified against crates.io until the
matching library version exists there. The release script instead patches the
server's normalized version dependency to the extracted library archive, which
tests the same two-package boundary before publication.

## Publish order

1. Dry-run and publish the library:

   ```console
   cargo publish -p redis-mcp --dry-run
   cargo publish -p redis-mcp
   ```

2. Wait until the exact `redis-mcp` version resolves from the crates.io index.
3. Dry-run and publish the server:

   ```console
   cargo publish -p redis-mcp-server --dry-run
   cargo publish -p redis-mcp-server
   ```

4. Verify `cargo install redis-mcp-server --version <version>` in a clean Cargo
   home and run `redis-mcp-server --help`.
5. Create the GitHub release from the same commit. Link the crate pages and the
   initial-release notes; do not advertise prebuilt binaries or containers
   unless those artifacts were produced and independently verified.

## Release-note checklist

Every release note should call out:

- library and server versions plus the supported Rust version;
- MCP protocol/transport changes;
- additions or removals in command families and runtime defaults;
- supported Redis, Cluster, and module pins;
- security-default or raw-policy changes;
- changed output, concurrency, timeout, session, or fan-out bounds;
- intentionally excluded or deferred commands affected by the release; and
- migration notes for public schemas, feature names, or configuration keys.

The initial release boundary and current compatibility matrix live in
[initial-release.md](initial-release.md). Future redisctl adoption and a
first-party CLI/REPL stay clearly labeled as follow-on work until they ship.
