#!/usr/bin/env python3
"""Generate the pinned Redis command metadata fixture from COMMAND output."""

from __future__ import annotations

import argparse
import difflib
import json
import shlex
import subprocess
import sys
from pathlib import Path
from typing import Any


REDIS_RELEASE = "8.10.1"
REDIS_TAG = "8.10.1"
REDIS_COMMIT = "3399357e7c17b668289386b8a15a3037bc4527b1"
REPOSITORY = "https://github.com/redis/redis"
ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "crates" / "redis-mcp" / "tests" / "fixtures" / "redis-commands-8.10.1.json"


def redis_json(redis_cli: list[str], host: str, port: int, *arguments: str) -> Any:
    process = subprocess.run(
        [*redis_cli, "-h", host, "-p", str(port), "--json", *arguments],
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(process.stdout)


def redis_version(redis_cli: list[str], host: str, port: int) -> str:
    process = subprocess.run(
        [*redis_cli, "-h", host, "-p", str(port), "INFO", "server"],
        check=True,
        capture_output=True,
        text=True,
    )
    info = process.stdout
    for line in info.splitlines():
        if line.startswith("redis_version:"):
            return line.split(":", 1)[1].strip()
    raise RuntimeError("INFO server did not report redis_version")


def cluster_behavior(group: str, hints: list[str], key_specs: list[object]) -> str:
    policies = sorted(
        hint.removeprefix("request_policy:")
        for hint in hints
        if hint.startswith("request_policy:")
    )
    if policies:
        return policies[0]
    if key_specs:
        return "key_routed"
    if group == "cluster":
        return "cluster_control_plane"
    return "node_local"


def normalize_command(command: list[Any], docs: dict[str, Any]) -> dict[str, Any]:
    wire_name = command[0]
    name = wire_name.upper().replace("|", " ")
    flags = sorted(command[2])
    acl_categories = sorted(category.removeprefix("@") for category in command[6])
    hints = sorted(command[7])
    key_specs = command[8]
    subcommands = command[9] if len(command) > 9 else []
    entry: dict[str, Any] = {
        "name": name,
        "container": name.split(" ", 1)[0] if " " in name else None,
        "summary": docs["summary"],
        "group": docs["group"],
        "since": docs["since"],
        "arity": command[1],
        "command_flags": flags,
        "acl_categories": acl_categories,
        "doc_flags": sorted(docs.get("doc_flags", [])),
        "hints": hints,
        "key_spec_count": len(key_specs),
        "cluster_behavior": cluster_behavior(docs["group"], hints, key_specs),
        "has_subcommands": bool(subcommands),
        "deprecated_since": docs.get("deprecated_since"),
        "replaced_by": docs.get("replaced_by"),
    }
    return entry


def walk_commands(
    commands: list[list[Any]], docs: dict[str, Any]
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for command in commands:
        wire_name = command[0]
        command_docs = docs[wire_name]
        normalized.append(normalize_command(command, command_docs))
        subcommands = command[9] if len(command) > 9 else []
        if subcommands:
            normalized.extend(walk_commands(subcommands, command_docs["subcommands"]))
    return normalized


def generate(redis_cli: list[str], host: str, port: int) -> str:
    actual_version = redis_version(redis_cli, host, port)
    if actual_version != REDIS_RELEASE:
        raise RuntimeError(
            f"expected Redis {REDIS_RELEASE}, but {host}:{port} reports {actual_version}"
        )

    commands = redis_json(redis_cli, host, port, "COMMAND")
    docs = redis_json(redis_cli, host, port, "COMMAND", "DOCS")
    normalized = walk_commands(commands, docs)
    normalized.sort(key=lambda command: command["name"])
    names = [command["name"] for command in normalized]
    if len(names) != len(set(names)):
        raise RuntimeError("COMMAND returned duplicate normalized command names")

    document = {
        "source": {
            "repository": REPOSITORY,
            "release": REDIS_RELEASE,
            "tag": REDIS_TAG,
            "commit": REDIS_COMMIT,
            "extracted_from": ["COMMAND", "COMMAND DOCS"],
            "generator": "scripts/update_redis_commands.py",
        },
        "command_count": len(normalized),
        "commands": normalized,
    }
    return json.dumps(document, indent=2, sort_keys=False) + "\n"


def check(expected: str) -> int:
    actual = OUTPUT.read_text()
    if actual == expected:
        print(f"{OUTPUT.relative_to(ROOT)} is current")
        return 0
    diff = difflib.unified_diff(
        actual.splitlines(),
        expected.splitlines(),
        fromfile=str(OUTPUT),
        tofile="generated",
        lineterm="",
    )
    print("\n".join(diff), file=sys.stderr)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--redis-cli-command",
        default="redis-cli",
        help="Command prefix used to invoke redis-cli (shell quoting is supported)",
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6379)
    parser.add_argument("--check", action="store_true")
    arguments = parser.parse_args()

    generated = generate(
        shlex.split(arguments.redis_cli_command), arguments.host, arguments.port
    )
    if arguments.check:
        return check(generated)
    OUTPUT.write_text(generated)
    print(f"wrote {OUTPUT.relative_to(ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
