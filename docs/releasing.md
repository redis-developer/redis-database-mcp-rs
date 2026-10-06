# Release process

`redis-mcp` and `redis-mcp-server` share a workspace version. For now, releases
are **GitHub-only**: `release-plz.toml` reads versions from Git tags and skips
Cargo registry publishing. This does not make either crate available on
crates.io. Repository visibility is a separate owner decision.

## GitHub-only release flow

On a push to `main`, the `release-pr` job prepares a **draft** release PR for
the two workspace packages. It never publishes a crate. A maintainer reviews
the version, generated changelogs, package contents, and the ordinary CI and
package checks before marking that PR ready and merging it. Keep the release
PR draft if any check is red or awaiting approval.

After a merge, the `release` job runs only when the `CI` workflow completed
successfully for a `push` to `main`. `release_always = false` further requires
the tested commit to be associated with a merged `release-plz-` PR. It then
creates package-specific tags and GitHub releases:

- `redis-mcp-v<version>`
- `redis-mcp-server-v<version>`

The first release has no prior matching tags; release-plz treats both packages
as initial releases. Subsequent versions are determined from those tags, not
from the registry. Both packages are in one version group, and the server's
dependency on the library keeps the workspace version synchronized. Do not
create or move these tags manually. If the first release PR does not propose
matching versions, stop and investigate rather than merging it.

The workflow uses the repository `GITHUB_TOKEN`, with separate job permissions
for PR creation and tag/release creation. In repository Actions settings,
enable **Allow GitHub Actions to create and approve pull requests**. GitHub
places CI runs on `GITHUB_TOKEN`-created release PRs in an approval-required
state; a maintainer with write access must approve those runs and verify they
pass before merging. No personal token, registry token, or trusted-publishing
permission is required. The workflow has an explicit repository/main guard,
checks out full Git history, and pins the release-plz action and CLI versions.

The workflow PR itself is not a release PR and must not create a tag or GitHub
release when merged. To start the first release, review the draft release PR
that the next `main` push generates; merge it only after all checks pass.

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

## Future crates.io publication (not enabled)

The commands below describe a later, separately approved migration. Do not
run them as part of a GitHub-only release. Before enabling registry publishing,
remove `git_only`, decide whether release-plz or an operator owns publishing,
verify the package gate and registry credentials, and reconcile the Git tags
with the versions already in crates.io. Avoid publishing a version number that
already names a different GitHub source release.

`redis-mcp-server` depends on `redis-mcp`, so registry packages must be
published in that order.

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
5. Reconcile the GitHub release and both package tags with the published crate
   versions. Do not advertise prebuilt binaries or containers unless those
   artifacts were produced and independently verified.

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
