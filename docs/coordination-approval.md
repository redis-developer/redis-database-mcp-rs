# Durable coordination approval

Issue #118 adds the first Phase 2 extension to the opt-in `coordination`
bundle. It uses MCP 2026-07-28 multi-round-trip requests (MRTR) for one
bounded boolean approval while Redis remains the durable source of truth.
It leaves the task lifecycle in #105 and the broader design in #109 open.

## Tool contract

`redis_handoff_request_approval` requires read-write access, the opaque
handoff handle, a stable idempotency key, and a 1..=2048-byte non-sensitive
message. Only the current durable claimant may call it.

On the first round the server atomically changes the handoff from `claimed` to
`awaiting_approval`, stores the request and appends `approval_requested` to the
handoff timeline. Only then does it return `input_required` with one form
elicitation request containing the required boolean `confirm` field.

The client answers and retries the same tool call. The server validates the
opaque approval ID in `requestState`, the stable idempotency digest, and the
current coordination principal against Redis. It then atomically records one
of:

- `approval_accepted`
- `approval_declined`
- `approval_cancelled`

The handoff returns to `claimed`; approval never acknowledges or completes the
Stream entry. `redis_handoff_complete` rejects an `awaiting_approval` handoff.

## Idempotency and recovery

Both rounds are safe to retry. A repeated idempotency key with the same
message reuses the original approval. Once resolved, retries return the first
committed outcome without asking the user again. Reusing a key with a
different message is rejected.

No continuation registry lives in the MCP process. Approval fields share the
handoff Hash and its existing Cluster hash tag, while audit events use the
same-slot timeline Stream. If the claimant exits, `redis_handoff_recover`
transfers the pending Stream entry without discarding `awaiting_approval`; the
new claimant can reissue the same approval and resolve it.

## Capability fallback

The tool requires the 2026-07-28 lifecycle and a client advertising form
elicitation. When that capability is absent, the protocol returns its stable
missing-capability error. Because Redis was updated before `input_required`,
the handoff remains durably `awaiting_approval` and is visible through
`redis_handoff_status` and the canonical handoff resource.

Approval messages must not contain credentials, secrets, or hidden reasoning.
The server accepts no arbitrary approval form fields and applies its normal
output budget to both pending and resolved status representations before the
first mutation.
