# Redis MCP coordination Phase 1 contract

Status: accepted Phase 1 contract. Parent design: issue #109. Tasks,
elicitation, and notifications are deferred; this contract is synchronous and
works with ordinary MCP tool and resource reads.

## Surface and policy

The `coordination` Cargo feature compiles one opt-in runtime bundle:

| Tool | Access | Retry contract |
| --- | --- | --- |
| `redis_handoff_publish` | read-write | Required publisher/idempotency key returns the original handle. |
| `redis_handoff_claim` | read-write | Claims at most one new entry; a finite `wait_ms` is capped at 5 seconds. |
| `redis_handoff_complete` | read-write | Required claimant/idempotency key returns the committed completion. |
| `redis_handoff_status` | read-only | Principal-authorized bounded read. |
| `redis_handoff_recover` | full | Claims at most one pending entry older than `min_idle_ms`. |

`redis-mcp://coordination/handoffs/{handle}` returns the same principal-
authorized state and latest bounded timeline as the status tool. Every tool
result that identifies a handoff includes this canonical URI. Clients without
resource support call `redis_handoff_status`; resource subscriptions and
notifications are optional hints, never a correctness dependency.

## Identity

`CoordinationPrincipal` is a typed host request extension. It is never an
input field and its `Debug` output is redacted. A host must derive it from a
durable authenticated subject. The standalone HTTP server hashes the bearer
credential, so one principal survives MCP session replacement; anonymous
sessions are isolated and are not durable across reconnects. Stdio routers
receive one random principal for their lifetime.

Only the publisher or current claimant can read state or payload. Only the
current claimant can complete. `XREADGROUP` assigns new work once; recovery
can transfer a pending entry only through the full-access tool and the caller's
explicit idle policy.

## Envelope

Publish accepts a capability, required idempotency key, optional correlation
ID, future Unix-millisecond deadline, W3C traceparent, bounded JSON metadata,
and a tagged payload. Completion uses the same payload envelope for its result.

```json
{
  "capability": "incident_triage",
  "idempotency_key": "incident-42-v1",
  "correlation_id": "incident-42",
  "deadline_at_ms": 1790294400000,
  "traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
  "metadata": {"severity": "high"},
  "payload": {
    "type": "json",
    "value": {"incident": 42},
    "schema_ref": "https://example.invalid/schemas/incident-v1"
  }
}
```

Payload types are `json`, `text`, and standard padded-base64 `binary`.
`content_type` is available for text and binary; `schema_ref` is optional on
all variants. The encoded payload defaults to 256 KiB and metadata to 16 KiB.
Capabilities, idempotency keys, correlation IDs, worker labels, content types,
schema references, trace metadata, shard counts, wait durations, event counts,
and outputs all have library-enforced bounds.

The MCP server generates timestamps. `attempts` starts at zero and increments
only after a claim or recovery is durably recorded. A deadline is durable
context for workers and policy layers; Phase 1 does not silently discard or
acknowledge expired work.

## Handle, sharding, and keys

The handle is versioned and opaque to callers. Internally it carries the safe
capability, shard, random handoff ID, and publisher/idempotency digests, so any
server can route it without a cross-slot directory. Do not parse it outside
this library.

Shard selection is deterministic:

1. SHA-256 the UTF-8 idempotency key.
2. Interpret the first two bytes as an unsigned big-endian integer.
3. Take modulo the configured shard count (default 16).

Every key for one handoff shares `{namespace:capability:shard}` as its Redis
Cluster hash tag:

```text
rmcp:{redis-mcp:incident_triage:3}:inbox
rmcp:{redis-mcp:incident_triage:3}:handoff:<random-id>
rmcp:{redis-mcp:incident_triage:3}:handoff:<random-id>:events
rmcp:{redis-mcp:incident_triage:3}:idem:<principal-digest>:<idempotency-digest>
rmcp:{redis-mcp:incident_triage:3}:handoff:<random-id>:completion:<principal-digest>:<idempotency-digest>
```

The inbox is a Stream with consumer group `redis-mcp-handoffs-v1`. The state
is a Hash. The timeline is a Stream. Idempotency records are Strings pointing
to the committed handle. Claim reads target exactly one explicit shard so a
Cluster operation never spans slots.

## Stored fields

Inbox entries contain `handle`, serialized `payload`, serialized `metadata`,
`correlation_id`, `deadline_at_ms`, and `traceparent`. State contains those
fields plus `capability`, `shard`, `status`, publisher digest, inbox
`stream_id`, `published_at_ms`, `attempts`, and—after transitions—claimant
digest, consumer, claim/completion timestamps, and serialized result. Timeline
entries contain an event (`published`, `claimed`, `recovered`, or `completed`)
and server timestamp.

Principal material and raw idempotency keys are never stored. SHA-256 digests
are routing/lookup identifiers, not authentication secrets.

## Atomicity and delivery semantics

Publish is one bounded Lua operation over four declared same-slot keys. It
checks the idempotency record, creates the consumer group when needed, appends
the inbox entry, writes state and timeline, then commits the idempotency
record. A retry after an ambiguous transport result returns the original
handle rather than creating another entry.

Claim uses finite `XREADGROUP COUNT 1`, then atomically records ownership,
timeline, and attempt count. A crash between the group read and ownership
record leaves the entry in Redis's pending entries list (PEL); the PEL is the
authority for recovery.

Completion is one bounded Lua operation. It verifies claimant ownership,
writes the result and completed state, appends the timeline, acknowledges the
PEL entry, and finally commits completion idempotency. Result durability
therefore precedes acknowledgement. A retry after an ambiguous result returns
the committed completion.

Recovery uses `XAUTOCLAIM COUNT 1` with an explicit positive idle threshold,
then atomically records transferred ownership and increments attempts.

This is **at-least-once delivery**, not exactly-once processing. Idempotent
publish and completion prevent duplicate protocol records, but an agent can
perform an external side effect and crash before completion. Work handlers
must use their own idempotency key for external effects.

## Stable failure classes and ACLs

Malformed handles, bounds, shard mismatches, and Cluster cross-slot attempts
are invalid requests. Missing/foreign ownership is an authorization-style tool
failure. Timeouts and connection failures retain the shared Redis error
taxonomy. Redis script ACL failures are normalized to Authorization even when
Redis wraps them in an `ERR ACL failure in script` response. Tool errors never
echo credentials, principal values, payloads, or Redis addresses.

The catalog lists every command needed by each coordination tool. Scope the
Redis ACL user to the `rmcp:*` key prefix (or a narrower configured namespace)
and only those commands. Direct `redis_x*` tools and their existing keys,
schemas, and behavior are unchanged.
