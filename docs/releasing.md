# Release process

`redis-mcp` is released to crates.io by [release-plz](https://release-plz.dev).
`.github/workflows/release-plz.yml` runs it on every push to `main`, and
`release-plz.toml` configures it. `redis-mcp-server` is not in that flow yet;
see [Publishing the server](#publishing-the-server).

## Library releases

Each release-plz run on `main` does two things:

1. **Release PR.** It opens or updates a PR labeled `release` that bumps the
   version from the conventional commits since the last release and adds a
   section to `crates/redis-mcp/CHANGELOG.md`. Edit that section in the PR
   if the generated notes need work.
2. **Release.** It publishes any `redis-mcp` version that crates.io does not
   have yet, pushes a `redis-mcp-v<version>` tag, and creates a GitHub
   release from that version's changelog section.

`release_always = true` means step 2 does not wait for a release PR: any
commit that reaches `main` with an unpublished version is published, including
a version edited by hand. Change versions only through the release PR. Both
crates inherit `[workspace.package] version`, so a release PR also moves the
server's version.

The job runs in the `crates-io` environment and does not wait for CI on
`main`, so merge release PRs only after their checks pass. release-plz opens
release PRs with the workflow's `GITHUB_TOKEN`, and GitHub does not run CI on
PRs opened that way. Close and reopen a release PR to run CI on it.

Until trusted publishing is configured for `redis-mcp`, the job authenticates
to crates.io with the `CARGO_REGISTRY_TOKEN` repository secret. crates.io only
allows a trusted publisher on a crate that already exists, so the first
publish needs the token.

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

## Publishing the server

`redis-mcp-server` depends on `redis-mcp`, so it can only be published after
the matching library version is on crates.io. `release-plz.toml` sets
`release = false` for it, so release-plz does not version, changelog, tag, or
publish it. To move it into the automated flow:

1. Remove its `[[package]]` table from `release-plz.toml` and put both
   packages in one `version_group`, since they share the workspace version.
2. Add `crates/redis-mcp-server/CHANGELOG.md` with a section for the version
   that will be published first.
3. Make sure the workflow can authenticate for the server's first publish
   (the `CARGO_REGISTRY_TOKEN` secret), then add the server crate to the same
   trusted publisher afterwards. release-plz publishes the library before the
   server.

To publish the server by hand instead:

1. Wait until the exact `redis-mcp` version resolves from the crates.io index.
2. Dry-run and publish the server:

   ```console
   cargo publish -p redis-mcp-server --dry-run
   cargo publish -p redis-mcp-server
   ```

3. Verify `cargo install redis-mcp-server --version <version>` in a clean Cargo
   home and run `redis-mcp-server --help`.
4. Create a `redis-mcp-server-v<version>` tag and GitHub release from the same
   commit. Link the crate pages and the initial-release notes; do not advertise
   prebuilt binaries or containers unless those artifacts were produced and
   independently verified.

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
