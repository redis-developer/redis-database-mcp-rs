# Durable agent handoffs

The `coordination` bundle turns Redis Streams consumer groups into a small,
durable handoff protocol. It is intentionally narrower than arbitrary Stream
commands: the library owns the key layout, Cluster hash tags, idempotency,
ownership checks, payload bounds, and completion acknowledgement.

## Lifecycle

1. A producer calls `redis_handoff_publish` with a capability, bounded tagged
   payload, and a stable idempotency key. The returned handle is the only
   locator needed by later status and completion calls.
2. Workers poll explicit shards with `redis_handoff_claim`. Polling one shard
   at a time is deliberate: a Redis Cluster read cannot span hash slots.
3. When a risky action needs human confirmation, the claimant calls
   `redis_handoff_request_approval` with a stable idempotency key and a bounded,
   non-sensitive question. Redis records `awaiting_approval` before the MCP
   client renders the boolean form. Accept, decline, and cancel are distinct
   durable outcomes; none automatically completes the handoff.
4. The claimant processes the payload and calls `redis_handoff_complete` with
   a stable completion idempotency key and bounded tagged result. Completion is
   rejected while an approval is pending.
5. Producers or current claimants inspect `redis_handoff_status` for durable
   state and a bounded event timeline.
6. An operator or trusted recovery agent uses `redis_handoff_recover` only
   after a workload-specific idle threshold. Recovery requires `full` access.

Payloads are explicit `json`, `text`, or standard-base64 `binary` values.
Principal identity is supplied by the host separately from tool input. HTTP
servers derive it from authenticated bearer credentials; session identifiers
are not durable identities.

## Operational rules

- Use a fresh idempotency key for a logically new publish, approval, or
  completion and reuse the same key for retries.
- Never put credentials, secrets, or hidden reasoning in an approval message.
  The approval form contains only one required boolean `confirm` field.
- A 2026-07-28 client must advertise form elicitation. If it cannot, the call
  fails with the protocol's missing-capability error while Redis retains the
  inspectable `awaiting_approval` state. Recovery durably cancels that pending
  approval before transferring ownership; the new claimant uses a fresh
  idempotency key if approval is still required.
- Divide shard polling among workers; the default shard count is exposed by
  application configuration, not inferred from Redis keys.
- Choose a recovery idle threshold longer than the normal processing budget.
- Grant the coordination service account only the commands listed by the
  coordination entries in `redis-mcp://catalog` and its configured key prefix.
- Treat handles as opaque. Their representation may evolve even though the
  versioned semantic contract remains stable.

Ordinary coordination remains synchronous MCP. Approval uses 2026-07-28 MRTR,
so the handler ends at `input_required` and resumes from the client's retry;
the continuation itself lives in Redis rather than process memory. MCP Tasks,
notifications, and richer workflow policies remain later layers.
