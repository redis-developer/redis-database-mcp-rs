#!/usr/bin/env bash

set -euo pipefail

mcp_repl_bin=${MCP_REPL_BIN:-mcp-repl}
server_bin=${REDIS_MCP_SERVER_BIN:-target/debug/redis-mcp-server}
redis_url=${REDIS_URL:-redis://127.0.0.1:6379}
slim_server_bin=${REDIS_MCP_SLIM_SERVER_BIN:-}
schema_contract=${REDIS_MCP_SCHEMA_CONTRACT:-docs/mcp-repl-contracts/redis-zrange.json}

for required in "$mcp_repl_bin" "$server_bin" jq; do
    if ! command -v "$required" >/dev/null 2>&1; then
        printf 'required executable not found: %s\n' "$required" >&2
        exit 1
    fi
done

campaign_tmp=$(mktemp -d "${TMPDIR:-/tmp}/redis-mcp-repl.XXXXXX")
cleanup() {
    find "$campaign_tmp" -type f -delete
    rmdir "$campaign_tmp"
}
trap cleanup EXIT

describe_surface() {
    local label=$1
    local binary=$2
    shift 2
    local tools_file="$campaign_tmp/$label-tools.json"
    local descriptions_file="$campaign_tmp/$label-descriptions.ndjson"
    local -a server_args=("$@")
    local -a exec_args=(--json -e tools)

    "$mcp_repl_bin" "${exec_args[@]}" -- "$binary" \
        --url "$redis_url" --stdio "${server_args[@]}" >"$tools_file"
    jq -e '
        type == "array" and length > 0 and
        ([.[].name] | length == (unique | length)) and
        all(.[];
            (.inputSchema.type == "object") and
            (.outputSchema != null) and
            (.annotations != null)
        )
    ' "$tools_file" >/dev/null

    while IFS= read -r tool_name; do
        exec_args+=(-e "describe $tool_name")
    done < <(jq -r '.[].name' "$tools_file")

    "$mcp_repl_bin" "${exec_args[@]}" -- "$binary" \
        --url "$redis_url" --stdio "${server_args[@]}" >"$descriptions_file"
    local expected
    expected=$(jq 'length' "$tools_file")
    jq -s -e --argjson expected "$expected" '
        length == ($expected + 1) and
        all(.[1:][];
            .kind == "tool" and
            .definition.inputSchema.type == "object" and
            .definition.outputSchema != null
        )
    ' "$descriptions_file" >/dev/null

    printf '%-18s %4d tools discovered and described\n' "$label" "$expected"
}

default_args=(--access read-only --no-discovery)
full_args=(
    --access full
    --raw
    --transactions
    --no-discovery
    --enable-bundle admin
    --enable-bundle bulk
    --enable-bundle invocation
    --enable-bundle json
    --enable-bundle search
    --enable-bundle scripting
    --enable-bundle timeseries
)

describe_surface default "$server_bin" "${default_args[@]}"
describe_surface full "$server_bin" "${full_args[@]}"

"$mcp_repl_bin" --schema-contract "$schema_contract" --schema-mode strict --json \
    -e 'describe redis_zrange' -- "$server_bin" \
    --url "$redis_url" --stdio "${default_args[@]}" \
    | jq -e '.kind == "tool" and .definition.name == "redis_zrange"' >/dev/null
printf '%-18s strict redis_zrange contract passed\n' schema-contract

if [[ -n "$slim_server_bin" ]]; then
    describe_surface strings-only "$slim_server_bin" --access full --no-discovery
else
    printf '%-18s skipped (set REDIS_MCP_SLIM_SERVER_BIN)\n' strings-only
fi

key_prefix="redis-mcp:dogfood:$$"
workflow_file="$campaign_tmp/workflows.ndjson"
"$mcp_repl_bin" --json \
    -e "redis_set key=$key_prefix:string value=hello expiration={\"type\":\"seconds\",\"value\":60}" \
    -e "redis_get key=$key_prefix:string" \
    -e "redis_set key=$key_prefix:page:1 value=one" \
    -e "redis_set key=$key_prefix:page:2 value=two" \
    -e "page = redis_scan pattern=$key_prefix:page:* count=1" \
    -e "redis_scan pattern=$key_prefix:page:* count=1 cursor=\$page.page.continuation.cursor" \
    -e "sub = redis_subscribe subscriptions=[{\"value\":\"$key_prefix:channel\"}]" \
    -e "redis_publish channel={\"value\":\"$key_prefix:channel\"} message={\"value\":\"hello\"}" \
    -e "redis_pubsub_read session_id=\$sub.session_id wait_ms=1000" \
    -e "redis_pubsub_close session_id=\$sub.session_id" \
    -e "redis_unlink keys=[\"$key_prefix:string\",\"$key_prefix:page:1\",\"$key_prefix:page:2\"]" \
    -- "$server_bin" --url "$redis_url" --stdio --access full --no-discovery \
    >"$workflow_file"

jq -s -e '
    length == 11 and
    all(.[]; .error == null) and
    .[1].structuredContent.exists == true and
    .[1].structuredContent.value == "hello" and
    .[8].structuredContent.messages[0].payload.value == "hello" and
    .[9].structuredContent.closed == true
' "$workflow_file" >/dev/null
printf '%-18s set/get, pagination capture, Pub/Sub, cleanup passed\n' workflows

printf 'mcp-repl campaign passed\n'
