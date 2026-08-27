#![cfg(feature = "timeseries")]

use std::collections::{BTreeMap, BTreeSet};

use redis_mcp::{
    AccessMode, NativeRedisInvocation, RawCommandPolicy, RedisCommand, RedisError,
    RedisInvocationEngine, RedisModule, RedisValue, RedisVersion, tool_catalog,
};
use serde::Deserialize;

const TIMESERIES_RELEASE: &str = "1.12.6";
const TIMESERIES_MODULE_VERSION: u64 = 11206;
const COMMAND_COUNT: usize = 17;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct SourceMetadata {
    repository: String,
    release: String,
    module: String,
    module_version: u64,
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
    flags: Vec<String>,
    first_key: i64,
    last_key: i64,
    key_step: i64,
}

#[derive(Debug, Deserialize)]
struct CoverageLedger {
    module: String,
    release: String,
    command_count: usize,
    commands: Vec<CoveredCommand>,
}

#[derive(Debug, Deserialize)]
struct CoveredCommand {
    name: String,
    disposition: String,
    access: String,
    cluster_behavior: String,
    tools: Vec<String>,
    rationale: String,
}

fn source() -> CommandSource {
    serde_json::from_str(include_str!(
        "fixtures/redis-timeseries-commands-1.12.6.json"
    ))
    .expect("pinned RedisTimeSeries command source must be valid JSON")
}

fn coverage() -> CoverageLedger {
    serde_json::from_str(include_str!("fixtures/redis-timeseries-coverage.json"))
        .expect("RedisTimeSeries coverage ledger must be valid JSON")
}

#[test]
fn timeseries_commands_are_pinned_and_mapped_exactly_once() {
    let source = source();
    let coverage = coverage();

    assert_eq!(
        source.source.repository,
        "https://github.com/RedisTimeSeries/RedisTimeSeries"
    );
    assert_eq!(source.source.release, TIMESERIES_RELEASE);
    assert_eq!(source.source.module, "timeseries");
    assert_eq!(source.source.module_version, TIMESERIES_MODULE_VERSION);
    assert!(!source.source.extracted_from.is_empty());
    assert!(!source.source.generator.trim().is_empty());
    assert_eq!(source.command_count, COMMAND_COUNT);
    assert_eq!(source.commands.len(), COMMAND_COUNT);
    assert_eq!(coverage.module, source.source.module);
    assert_eq!(coverage.release, source.source.release);
    assert_eq!(coverage.command_count, source.command_count);
    assert_eq!(coverage.commands.len(), source.commands.len());

    let source_names = source
        .commands
        .iter()
        .map(|command| command.name.as_str())
        .collect::<Vec<_>>();
    let mut sorted = source_names.clone();
    sorted.sort_unstable();
    assert_eq!(source_names, sorted, "source commands must stay sorted");
    assert_eq!(
        source_names.iter().copied().collect::<BTreeSet<_>>().len(),
        source_names.len(),
        "source command names must be unique"
    );
    let coverage_names = coverage
        .commands
        .iter()
        .map(|command| command.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(source_names, coverage_names);

    for (source, covered) in source.commands.iter().zip(&coverage.commands) {
        assert_eq!(covered.name, source.name);
        assert_eq!(covered.disposition, "typed", "{}", covered.name);
        assert!(!covered.tools.is_empty(), "{}", covered.name);
        assert!(!covered.rationale.trim().is_empty(), "{}", covered.name);
        assert!(source.flags.iter().any(|flag| flag == "module"));

        // Our access tier can be stricter than the server flag, never looser.
        if source.flags.iter().any(|flag| flag == "readonly") {
            assert_eq!(covered.access, "read_only", "{}", covered.name);
        } else {
            assert!(
                matches!(covered.access.as_str(), "read_write" | "full"),
                "{} must classify a write flag as mutating",
                covered.name
            );
        }

        let keyless = source.first_key == 0;
        if keyless {
            assert_eq!(covered.cluster_behavior, "node_local", "{}", covered.name);
        } else {
            assert_eq!(covered.cluster_behavior, "key_routed", "{}", covered.name);
        }
        if covered.name == "TS.MADD" {
            assert_eq!(
                (source.first_key, source.last_key, source.key_step),
                (1, -1, 3)
            );
        }
    }
}

#[test]
fn timeseries_tools_have_catalog_and_classification_evidence() {
    let catalog = tool_catalog()
        .iter()
        .map(|tool| (tool.name, tool))
        .collect::<BTreeMap<_, _>>();

    struct RejectingExecutor;

    #[async_trait::async_trait]
    impl redis_mcp::RedisExecutor for RejectingExecutor {
        async fn execute(&self, _command: RedisCommand) -> Result<RedisValue, RedisError> {
            unreachable!("classification never executes")
        }
    }

    let engine = RedisInvocationEngine::builder(RejectingExecutor)
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();

    for covered in coverage().commands {
        for tool_name in &covered.tools {
            let tool = catalog
                .get(tool_name.as_str())
                .unwrap_or_else(|| panic!("{} references missing tool {tool_name}", covered.name));
            assert_eq!(
                covered.access,
                tool.required_access.as_str(),
                "{} disagrees with {tool_name} access",
                covered.name
            );
            assert_eq!(
                tool.required_module(),
                Some(RedisModule::TimeSeries),
                "{tool_name}"
            );
            assert!(
                tool.capability_requirements()
                    .required_commands()
                    .contains(&covered.name.as_str()),
                "{} maps to {tool_name} without capability evidence",
                covered.name
            );
        }

        let metadata = engine
            .classify(&NativeRedisInvocation::new(covered.name.as_bytes()))
            .unwrap_or_else(|error| panic!("{} must classify: {error}", covered.name));
        assert!(metadata.is_classified(), "{}", covered.name);
        assert_eq!(
            metadata.required_access().as_str(),
            covered.access,
            "{} native classification disagrees with the ledger",
            covered.name
        );
        assert_eq!(
            metadata.required_module(),
            Some(RedisModule::TimeSeries),
            "{}",
            covered.name
        );
        if covered.name == "TS.DEL" {
            assert_eq!(
                metadata.minimum_module_version(),
                Some(RedisVersion::new(1, 6, 0))
            );
        }
    }
}
