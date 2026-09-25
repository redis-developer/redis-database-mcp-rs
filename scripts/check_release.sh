#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/redis-mcp-release.XXXXXX")"
trap 'rm -rf "$work_dir"' EXIT

export CARGO_TARGET_DIR="$work_dir/target"
package_dir="$CARGO_TARGET_DIR/package"
extract_dir="$work_dir/extracted"
mkdir -p "$extract_dir"

echo "==> Verifying redis-mcp package"
cargo package \
  --manifest-path "$repo_root/Cargo.toml" \
  --locked \
  -p redis-mcp

echo "==> Assembling workspace packages for server verification"
cargo package \
  --manifest-path "$repo_root/Cargo.toml" \
  --locked \
  --workspace \
  --no-verify

shopt -s nullglob
library_archives=("$package_dir"/redis-mcp-[0-9]*.crate)
server_archives=("$package_dir"/redis-mcp-server-[0-9]*.crate)
if [[ ${#library_archives[@]} -ne 1 ]]; then
  echo "expected one redis-mcp archive, found ${#library_archives[@]}" >&2
  exit 1
fi
if [[ ${#server_archives[@]} -ne 1 ]]; then
  echo "expected one redis-mcp-server archive, found ${#server_archives[@]}" >&2
  exit 1
fi

tar -xzf "${library_archives[0]}" -C "$extract_dir"
tar -xzf "${server_archives[0]}" -C "$extract_dir"
library_dirs=("$extract_dir"/redis-mcp-[0-9]*)
server_dirs=("$extract_dir"/redis-mcp-server-[0-9]*)
library_dir="${library_dirs[0]}"
server_dir="${server_dirs[0]}"

for packaged_crate in "$library_dir" "$server_dir"; do
  for required_file in README.md LICENSE-MIT LICENSE-APACHE; do
    if [[ ! -s "$packaged_crate/$required_file" ]]; then
      echo "missing $required_file from $(basename "$packaged_crate")" >&2
      exit 1
    fi
  done
  if grep -Eq 'workspace[[:space:]]*=[[:space:]]*true' "$packaged_crate/Cargo.toml"; then
    echo "normalized package manifest still contains workspace inheritance" >&2
    exit 1
  fi
done

if awk '
  $0 == "[dependencies.redis-mcp]" { in_dependency = 1; next }
  /^\[/ { in_dependency = 0 }
  in_dependency && /^path[[:space:]]*=/ { found = 1 }
  END { exit found ? 0 : 1 }
' "$server_dir/Cargo.toml"; then
  echo "server package contains a workspace-only path dependency" >&2
  exit 1
fi

patch_arg="patch.crates-io.redis-mcp.path=\"$library_dir\""
consumer_dir="$work_dir/library-consumer"
cp -R "$repo_root/release-tests/library-consumer" "$consumer_dir"
consumer_manifest="$consumer_dir/Cargo.toml"

echo "==> Compiling clean-room library consumers"
for feature in minimal library-default library-full; do
  cargo check \
    --manifest-path "$consumer_manifest" \
    --no-default-features \
    --features "$feature" \
    --config "$patch_arg"
done

echo "==> Compiling packaged server surfaces"
cargo check \
  --manifest-path "$server_dir/Cargo.toml" \
  --no-default-features \
  --features keyspace,strings \
  --config "$patch_arg"
cargo check \
  --manifest-path "$server_dir/Cargo.toml" \
  --config "$patch_arg"
cargo check \
  --manifest-path "$server_dir/Cargo.toml" \
  --all-features \
  --config "$patch_arg"

echo "==> Verifying feature composition and catalog snapshots"
cargo test \
  --manifest-path "$repo_root/Cargo.toml" \
  --locked \
  -p redis-mcp \
  --no-default-features \
  --features strings \
  --test family_composition
cargo test \
  --manifest-path "$repo_root/Cargo.toml" \
  --locked \
  -p redis-mcp \
  --test family_composition
cargo test \
  --manifest-path "$repo_root/Cargo.toml" \
  --locked \
  -p redis-mcp \
  --all-features \
  --test router_contract \
  catalog_matches_checked_in_contract_snapshot

echo "release package checks passed"
