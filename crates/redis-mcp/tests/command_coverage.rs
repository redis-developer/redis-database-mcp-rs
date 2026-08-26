use std::collections::{BTreeMap, BTreeSet};

use redis_mcp::{ToolMetadata, tool_catalog};
use serde::Deserialize;

const REDIS_RELEASE: &str = "8.10.1";
const REDIS_COMMIT: &str = "3399357e7c17b668289386b8a15a3037bc4527b1";
const COMMAND_COUNT: usize = 449;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
struct SourceMetadata {
    repository: String,
    release: String,
    tag: String,
    commit: String,
    extracted_from: Vec<String>,
    generator: String,
}

#[derive(Debug, Deserialize)]
struct CommandSource {
    source: SourceMetadata,
    command_count: usize,
    commands: Vec<SourceCommand>,
}

#[derive(Debug, Deserialize)]
struct SourceCommand {
    name: String,
    cluster_behavior: String,
    has_subcommands: bool,
    doc_flags: Vec<String>,
    deprecated_since: Option<String>,
}

#[derive(Debug, Deserialize)]
struct CoverageLedger {
    source: SourceMetadata,
    source_command_count: usize,
    dispositions: Vec<String>,
    commands: Vec<CoveredCommand>,
}

#[derive(Debug, Deserialize)]
struct CoveredCommand {
    name: String,
    disposition: String,
    access: String,
    cluster_behavior: String,
    tools: Vec<String>,
    tracking_issue: Option<u64>,
    rationale: String,
    invocation: Option<Vec<String>>,
}

fn source() -> CommandSource {
    serde_json::from_str(include_str!("fixtures/redis-commands-8.10.1.json"))
        .expect("pinned Redis command source must be valid JSON")
}

fn coverage() -> CoverageLedger {
    serde_json::from_str(include_str!("fixtures/redis-command-coverage.json"))
        .expect("Redis command coverage ledger must be valid JSON")
}

fn ordered_names<'a>(names: impl Iterator<Item = &'a str>) -> Vec<&'a str> {
    let names = names.collect::<Vec<_>>();
    let mut expected = names.clone();
    expected.sort_unstable();
    assert_eq!(names, expected, "command entries must remain sorted");
    names
}

#[test]
fn official_redis_commands_are_pinned_and_mapped_exactly_once() {
    let source = source();
    let coverage = coverage();

    assert_eq!(source.source.repository, "https://github.com/redis/redis");
    assert_eq!(source.source.release, REDIS_RELEASE);
    assert_eq!(source.source.tag, REDIS_RELEASE);
    assert_eq!(source.source.commit, REDIS_COMMIT);
    assert_eq!(source.source.extracted_from, ["COMMAND", "COMMAND DOCS"]);
    assert_eq!(source.source.generator, "scripts/update_redis_commands.py");
    assert_eq!(source.command_count, COMMAND_COUNT);
    assert_eq!(source.commands.len(), COMMAND_COUNT);
    assert_eq!(coverage.source, source.source);
    assert_eq!(coverage.source_command_count, source.command_count);
    assert_eq!(coverage.commands.len(), source.commands.len());

    let source_names = ordered_names(source.commands.iter().map(|command| command.name.as_str()));
    let coverage_names = ordered_names(
        coverage
            .commands
            .iter()
            .map(|command| command.name.as_str()),
    );
    assert_eq!(source_names, coverage_names);
    assert_eq!(
        source_names.iter().copied().collect::<BTreeSet<_>>().len(),
        source_names.len(),
        "official command names must be unique"
    );

    let known_dispositions = coverage
        .dispositions
        .iter()
        .map(String::as_str)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        known_dispositions,
        BTreeSet::from([
            "composed",
            "container",
            "deprecated",
            "excluded",
            "internal",
            "native",
            "planned",
            "session",
            "typed",
        ])
    );

    for (source, covered) in source.commands.iter().zip(&coverage.commands) {
        assert_eq!(covered.name, source.name);
        assert_eq!(covered.cluster_behavior, source.cluster_behavior);
        assert!(
            known_dispositions.contains(covered.disposition.as_str()),
            "unknown disposition for {}",
            covered.name
        );
        assert!(
            matches!(covered.access.as_str(), "read_only" | "read_write" | "full"),
            "missing access tier for {}",
            covered.name
        );
        assert!(
            !covered.rationale.trim().is_empty(),
            "missing rationale for {}",
            covered.name
        );

        if source.has_subcommands {
            assert_eq!(covered.disposition, "container", "{}", covered.name);
        }
        if source.doc_flags.iter().any(|flag| flag == "syscmd") {
            assert_eq!(covered.disposition, "internal", "{}", covered.name);
        }
        if source.deprecated_since.is_some() {
            assert_eq!(covered.disposition, "deprecated", "{}", covered.name);
        }

        match covered.disposition.as_str() {
            "typed" | "composed" => {
                assert!(!covered.tools.is_empty(), "{}", covered.name);
                assert_eq!(covered.tracking_issue, None, "{}", covered.name);
                assert_eq!(covered.invocation, None, "{}", covered.name);
            }
            "native" => {
                let invocation = covered.invocation.as_ref().expect("native invocation");
                assert!(!invocation.is_empty(), "{}", covered.name);
                assert_eq!(invocation.join(" "), covered.name);
                assert_eq!(covered.tracking_issue, None, "{}", covered.name);
                assert!(covered.tools.is_empty(), "{}", covered.name);
            }
            "session" | "planned" => {
                assert!(covered.tracking_issue.is_some(), "{}", covered.name);
                assert_eq!(covered.invocation, None, "{}", covered.name);
            }
            _ => {
                assert_eq!(covered.tracking_issue, None, "{}", covered.name);
                assert_eq!(covered.invocation, None, "{}", covered.name);
            }
        }
    }
}

#[test]
fn typed_composed_and_session_tools_have_live_catalog_evidence() {
    let catalog = tool_catalog()
        .iter()
        .map(|tool| (tool.name, tool))
        .collect::<BTreeMap<_, _>>();

    for covered in coverage().commands {
        if !matches!(
            covered.disposition.as_str(),
            "typed" | "composed" | "session" | "deprecated"
        ) {
            continue;
        }
        for tool_name in &covered.tools {
            let tool = catalog
                .get(tool_name.as_str())
                .unwrap_or_else(|| panic!("{} references missing tool {tool_name}", covered.name));
            if matches!(covered.disposition.as_str(), "typed" | "composed") {
                assert_tool_evidence(&covered, tool);
                assert_eq!(
                    covered.access,
                    tool.required_access.as_str(),
                    "{} disagrees with {tool_name} access",
                    covered.name
                );
            }
        }
    }
}

fn assert_tool_evidence(covered: &CoveredCommand, tool: &ToolMetadata) {
    let redis_command = covered.name.split(' ').next().expect("command name");
    assert!(
        tool.capability_requirements()
            .required_commands()
            .contains(&redis_command),
        "{} maps to {} without {} capability evidence",
        covered.name,
        tool.name,
        redis_command
    );
}

#[test]
fn planned_and_session_work_is_tied_to_the_known_backlog() {
    let tracked = coverage()
        .commands
        .into_iter()
        .filter_map(|command| command.tracking_issue)
        .collect::<BTreeSet<_>>();
    assert_eq!(tracked, BTreeSet::from([30, 32, 58, 61, 63, 66]));
}
