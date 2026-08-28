# Batching, transactions, and scripts

Redis gives four ways to make several operations behave like one. They
differ in atomicity, failure behavior, and cost; choosing the wrong one
either wastes round trips or fabricates atomicity that is not there.

## The four options

1. **Multi-key commands** — `redis_mget`, `redis_mset`, `redis_del`,
   `redis_sinterstore`, and friends. One command, genuinely atomic, cheapest
   possible batching. Always prefer a purpose-built multi-key command when
   one exists.
2. **Client-side batching / bulk workflows** — independent commands issued
   without waiting for each reply. Not atomic: other clients' commands
   interleave, and a mid-batch failure leaves earlier writes applied. This
   library's `redis_bulk_load` and `redis_bulk_seed` are the governed form:
   bounded batches with explicit stop/continue-on-error, per-record failure
   identity, and honest partial-application reporting.
3. **MULTI/EXEC transactions** — `redis_transaction` runs one bounded
   command list atomically: all queued commands execute back-to-back with
   nothing interleaved. Optional watched keys give optimistic concurrency —
   the transaction reports `aborted` if a watched key changed after the
   watch.
4. **Server-side scripts and functions** — `redis_eval` / `redis_evalsha`
   (Lua) and `redis_fcall` (Redis 7 Functions, managed via
   `redis_function_load` / `redis_function_list`). Atomic like a
   transaction, plus the ability to compute on values mid-flight — read,
   decide, write in one step.

## Decision rules

- Need results of earlier commands to build later ones, atomically → a
  script (`redis_eval_ro` for pure reads, `redis_eval` for writes). MULTI
  queues commands before anything runs, so a transaction cannot branch on
  its own reads.
- Need all-or-nothing application of a known command list → a transaction
  (`redis_transaction`). Note Redis has no rollback: a command that fails at
  runtime inside EXEC (wrong type, for example) does not undo its
  neighbors — the per-command result alignment reports exactly which
  entries failed.
- Need "only commit if this key did not change" → a transaction with
  watched keys; retry on `aborted`. This is Redis's compare-and-set.
- Loading or seeding data → the bulk tools. They pipeline internally under
  explicit batch, concurrency, byte, and duration bounds; atomicity is
  deliberately not claimed, and dry runs validate first.
- Just reducing round trips for independent operations → multi-key commands
  where they exist, bulk workflows otherwise. Do not pay transaction
  overhead for batching.

## Costs and boundaries to respect

- Transactions hold one freshly dialed dedicated connection per call in
  this library, so MULTI state can never leak between MCP calls; command
  count, watch keys, request bytes, and duration are all bounded. Nested
  commands pass the same classification policy as `redis_command`, so the
  bundle requires the raw opt-in and full access.
- A connection loss after EXEC was sent may leave the outcome unknown; the
  library reports that explicitly and never replays a possibly committed
  transaction. Design retries around idempotent command lists where
  possible.
- Scripts run on the single main thread: a 50 ms script blocks everything
  for 50 ms. Keep scripts small, declare every key (they are same-slot
  validated on Cluster), and prefer `redis_evalsha` after `redis_script_load`
  to avoid resending source. `redis_function_stats` and `redis_script_kill`
  are the operational escape hatches.
- On Redis Cluster, every option above is slot-scoped: all keys of a
  transaction, script, or multi-key command must share one hash slot (see
  `redis-mcp://guidance/cluster-key-design`). Cross-slot work means either
  hash-tag redesign or per-slot decomposition with partial-failure handling
  — which is exactly what the bulk tools' per-record reporting is for.
- Blocking operations never belong inside transactions or scripts; inside
  MULTI they return immediately by design. Use the dedicated finite
  blocking tools (`redis_blpop`, `redis_blmove`, `redis_bzmpop`) instead.

## Durability is a separate axis

None of the four options makes a write durable by itself. `redis_wait`
reports how many replicas acknowledged a dedicated connection's write
position, and `redis_waitaof` (Redis 7.2+) reports AOF fsync coverage —
both as achieved counts against requested counts, without turning Redis
into a synchronous-replication system. Treat them as observability for a
durability decision, not as a guarantee.
