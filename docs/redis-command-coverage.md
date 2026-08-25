# Official Redis command coverage

The command-completeness gate is pinned to Redis 8.10.1 at upstream commit
`3399357e7c17b668289386b8a15a3037bc4527b1`.

Two checked-in files keep the upstream facts separate from product decisions:

- [`redis-commands-8.10.1.json`](../crates/redis-mcp/tests/fixtures/redis-commands-8.10.1.json)
  is normalized from
  the pinned server's `COMMAND` and `COMMAND DOCS` replies. It contains every
  advertised command and subcommand, not a hand-maintained command list.
- [`redis-command-coverage.json`](../crates/redis-mcp/tests/fixtures/redis-command-coverage.json)
  gives each of
  those definitions exactly one library disposition, access tier, Cluster
  behavior, rationale, catalog evidence, and tracking issue where required.

Redis's own source documentation warns consumers not to treat the raw
`src/commands/*.json` files as the public command metadata because some flags
are populated during generation. The fixture therefore follows the upstream
recommendation and extracts the running server's command introspection output.
CI independently regenerates it from the pinned official Redis image.
The CI container bypasses that image's convenience entrypoint because the
entrypoint auto-loads bundled Search, JSON, Bloom, and Time Series modules;
the core ledger intentionally starts `redis-server` directly. Redis's built-in
Vector Set commands remain part of the core inventory.

## Current ledger

| Disposition | Commands | Meaning |
| --- | ---: | --- |
| `typed` | 182 | Covered by cataloged structured MCP tools. |
| `native` | 32 | Available through fail-closed classified native invocation. |
| `session` | 40 | Implemented or planned through a bounded dedicated connection/workflow. |
| `planned` | 86 | Assigned to a concrete command-completeness backlog issue. |
| `excluded` | 62 | Outside the product or safety boundary with an explicit rationale. |
| `deprecated` | 21 | Redis marks the command deprecated; richer replacements are preferred. |
| `internal` | 8 | Redis marks the command as a system command. |
| `container` | 18 | Namespace-only command whose useful subcommands are mapped separately. |

The counts describe official Redis command definitions, not MCP tool count.
One structured tool can compose multiple commands, and one Redis command can
support multiple tools.

Planned and session work is tied to issues #32, #58, #61, #62, #63,
and #66. The six already-implemented Pub/Sub connection commands retain their
closed implementation reference, #30.

Bitmap/bitfield, geospatial, and HyperLogLog coverage is fully typed. Its
contracts cap item counts and bitmap write extent, preserve exact integer and
coordinate tokens, label probabilistic cardinality explicitly, and classify
destination-overwriting forms as full access with native same-slot Cluster
semantics.

The Redis 8 modern-data slice adds 42 typed command mappings: the complete
18-command Redis Array family, 13 vector-set operations, and 11 finite string,
hash, list, and Stream deltas. Variable requests are capped, Array and vector
values are binary-safe, Array indices retain their unsigned 64-bit range, and
ARGREP, ARSCAN, and VRANGE expose continuation contracts. Minimum versions are
enforced at Redis 8.0, 8.2, 8.4, 8.8, and 8.10, with live RESP2, RESP3, and
three-master Cluster coverage. The remaining eight reviewed additions are
explicitly classified as connection-local migration state, destructive
node-local lifecycle, or internal protocol commands rather than agent-safe
database operations.

## Enforced invariants

The contract tests fail when:

- the pinned source and coverage ledger contain different, missing, duplicate,
  or unsorted command names;
- source release, tag, commit, command count, Cluster behavior, internal flags,
  or deprecations drift;
- an entry lacks a disposition, access tier, rationale, or required issue;
- a typed/composed mapping names a missing catalog tool, lacks matching command
  capability evidence, or disagrees with the tool's access tier; or
- a `native` entry stops passing the fail-closed classified invocation policy.

This keeps unrestricted raw execution from becoming an accidental definition
of command support.

## Updating the pin

1. Check out the intended Redis release and verify its exact commit.
2. Start that `redis-server` with no third-party modules.
3. Regenerate the normalized source:

       python3 scripts/update_redis_commands.py --host 127.0.0.1 --port 6379

4. Reconcile every added, removed, or changed entry in
   `crates/redis-mcp/tests/fixtures/redis-command-coverage.json`. New commands
   must be reviewed; the generator deliberately does not invent product
   dispositions.
5. Run:

       python3 scripts/update_redis_commands.py --host 127.0.0.1 --port 6379 --check
       cargo test -p redis-mcp --test command_coverage
       cargo test -p redis-mcp raw::tests::official_native_ledger_entries_stay_fail_closed_and_classified

When running `redis-cli` through another process, such as the pinned CI
container, pass a command prefix with `--redis-cli-command`.
