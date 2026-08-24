use std::collections::{BTreeMap, BTreeSet};

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Scorecard {
    schema_version: u64,
    captured_at: String,
    baselines: Baselines,
    status_definitions: BTreeMap<String, String>,
    completion_gate: CompletionGate,
    current_gate: CurrentGate,
    contract_scorecard: ContractScorecard,
    capabilities: Vec<Capability>,
}

#[derive(Debug, Deserialize)]
struct Baselines {
    redis_mcp: Baseline,
    redisctl: Baseline,
}

#[derive(Debug, Deserialize)]
struct Baseline {
    repository: String,
    revision: String,
    #[serde(default)]
    release: Option<String>,
    tool_count: usize,
    tools: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CompletionGate {
    redis_mcp: String,
    library_catalog: String,
    contract: ContractGate,
    strategic_capabilities: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct ContractGate {
    minimum_library_score: u8,
    must_not_trail_redis_mcp: bool,
    minimum_leading_dimensions: usize,
}

#[derive(Debug, Deserialize)]
struct CurrentGate {
    met: bool,
    blockers: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct ContractScorecard {
    scale: BTreeMap<String, String>,
    dimensions: Vec<ContractDimension>,
}

#[derive(Debug, Deserialize)]
struct ContractDimension {
    id: String,
    library_score: u8,
    redis_mcp_score: u8,
    library_evidence: Vec<String>,
    redis_mcp_evidence: Vec<String>,
    target: String,
}

#[derive(Debug, Deserialize)]
struct Capability {
    id: String,
    redis_mcp_tools: Vec<String>,
    redisctl_tools: Vec<String>,
    library_tools: Vec<String>,
    #[serde(default)]
    replacement_library_tools: Vec<String>,
    disposition: String,
    #[serde(default)]
    issue: Option<u64>,
    #[serde(default)]
    quality_issues: Vec<u64>,
    reason: String,
}

fn scorecard() -> Scorecard {
    serde_json::from_str(include_str!("../../../docs/surface-comparison.json"))
        .expect("surface comparison must be valid JSON")
}

fn occurrences<'a>(tools: impl Iterator<Item = &'a String>) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for tool in tools {
        *counts.entry(tool.clone()).or_default() += 1;
    }
    counts
}

fn assert_pinned_baseline(name: &str, baseline: &Baseline, expected_count: usize) {
    assert_eq!(baseline.tool_count, expected_count, "{name} declared count");
    assert_eq!(baseline.tools.len(), expected_count, "{name} tools");
    assert!(
        baseline.repository.starts_with("https://github.com/"),
        "{name}"
    );
    assert_eq!(baseline.revision.len(), 40, "{name} revision");
    assert!(
        baseline
            .revision
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit()),
        "{name} revision"
    );
    let counts = occurrences(baseline.tools.iter());
    assert!(
        counts.values().all(|count| *count == 1),
        "{name} baseline contains duplicate tools"
    );
    assert!(
        baseline.tools.iter().all(|tool| !tool.trim().is_empty()),
        "{}",
        name
    );
}

fn assert_exact_mapping(name: &str, baseline: &Baseline, mapped: impl Iterator<Item = String>) {
    let mapped = occurrences(mapped.collect::<Vec<_>>().iter());
    let baseline = occurrences(baseline.tools.iter());
    assert_eq!(mapped, baseline, "{name} mapping must be exact and unique");
}

#[test]
fn comparison_baselines_are_pinned_and_completely_mapped() {
    let scorecard = scorecard();
    assert_eq!(scorecard.schema_version, 1);
    assert_eq!(scorecard.captured_at, "2026-08-11");
    assert_pinned_baseline("redis/mcp-redis", &scorecard.baselines.redis_mcp, 53);
    assert_eq!(
        scorecard.baselines.redis_mcp.release.as_deref(),
        Some("0.5.1")
    );
    assert_pinned_baseline("redisctl", &scorecard.baselines.redisctl, 132);

    assert_exact_mapping(
        "redis/mcp-redis",
        &scorecard.baselines.redis_mcp,
        scorecard
            .capabilities
            .iter()
            .flat_map(|capability| capability.redis_mcp_tools.iter().cloned()),
    );
    assert_exact_mapping(
        "redisctl",
        &scorecard.baselines.redisctl,
        scorecard
            .capabilities
            .iter()
            .flat_map(|capability| capability.redisctl_tools.iter().cloned()),
    );
}

#[test]
fn library_catalog_and_dispositions_cannot_drift() {
    let scorecard = scorecard();
    let catalog = redis_mcp::tool_catalog()
        .iter()
        .map(|tool| tool.name.to_string())
        .collect::<BTreeSet<_>>();
    let mapped = occurrences(
        scorecard
            .capabilities
            .iter()
            .flat_map(|capability| capability.library_tools.iter()),
    );
    assert!(
        mapped.values().all(|count| *count == 1),
        "library tools must appear once"
    );
    assert_eq!(mapped.into_keys().collect::<BTreeSet<_>>(), catalog);

    assert_eq!(
        scorecard
            .status_definitions
            .keys()
            .map(String::as_str)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["excluded", "implemented", "planned", "superseded"])
    );
    assert!(
        scorecard
            .status_definitions
            .values()
            .all(|description| !description.trim().is_empty())
    );

    let mut capability_ids = BTreeSet::new();
    for capability in &scorecard.capabilities {
        assert!(capability_ids.insert(&capability.id), "{}", capability.id);
        assert!(!capability.reason.trim().is_empty(), "{}", capability.id);
        assert!(capability.quality_issues.iter().all(|issue| *issue > 0));
        match capability.disposition.as_str() {
            "implemented" => {
                assert!(!capability.library_tools.is_empty(), "{}", capability.id);
                assert!(capability.issue.is_none(), "{}", capability.id);
            }
            "planned" => {
                assert!(capability.library_tools.is_empty(), "{}", capability.id);
                assert!(
                    capability.issue.is_some_and(|issue| issue > 0),
                    "{}",
                    capability.id
                );
            }
            "superseded" => {
                assert!(capability.library_tools.is_empty(), "{}", capability.id);
                assert!(
                    !capability.replacement_library_tools.is_empty(),
                    "{}",
                    capability.id
                );
                assert!(capability.issue.is_none(), "{}", capability.id);
            }
            "excluded" => {
                assert!(capability.library_tools.is_empty(), "{}", capability.id);
                assert!(capability.issue.is_none(), "{}", capability.id);
            }
            disposition => panic!("unknown disposition {disposition} for {}", capability.id),
        }
    }
}

#[test]
fn completion_gate_is_objective_and_self_consistent() {
    let scorecard = scorecard();
    let gate = &scorecard.completion_gate.contract;
    assert!(!scorecard.completion_gate.redis_mcp.trim().is_empty());
    assert!(!scorecard.completion_gate.library_catalog.trim().is_empty());
    assert!(gate.minimum_library_score <= 3);
    assert_eq!(
        scorecard
            .contract_scorecard
            .scale
            .keys()
            .cloned()
            .collect::<Vec<_>>(),
        ["0", "1", "2", "3"]
    );
    assert!(
        scorecard
            .contract_scorecard
            .scale
            .values()
            .all(|description| !description.trim().is_empty())
    );

    let mut dimensions = BTreeSet::new();
    let mut leading_dimensions = 0;
    let mut contract_met = true;
    for dimension in &scorecard.contract_scorecard.dimensions {
        assert!(dimensions.insert(&dimension.id), "{}", dimension.id);
        assert!(dimension.library_score <= 3, "{}", dimension.id);
        assert!(dimension.redis_mcp_score <= 3, "{}", dimension.id);
        assert!(!dimension.library_evidence.is_empty(), "{}", dimension.id);
        assert!(!dimension.redis_mcp_evidence.is_empty(), "{}", dimension.id);
        assert!(!dimension.target.trim().is_empty(), "{}", dimension.id);
        contract_met &= dimension.library_score >= gate.minimum_library_score;
        if gate.must_not_trail_redis_mcp {
            contract_met &= dimension.library_score >= dimension.redis_mcp_score;
        }
        if dimension.library_score > dimension.redis_mcp_score {
            leading_dimensions += 1;
        }
    }
    contract_met &= leading_dimensions >= gate.minimum_leading_dimensions;

    let capability_by_id = scorecard
        .capabilities
        .iter()
        .map(|capability| (capability.id.as_str(), capability))
        .collect::<BTreeMap<_, _>>();
    let strategic_met = scorecard
        .completion_gate
        .strategic_capabilities
        .iter()
        .all(|id| {
            capability_by_id
                .get(id.as_str())
                .is_some_and(|capability| capability.disposition == "implemented")
        });
    let redis_mcp_met = scorecard.capabilities.iter().all(|capability| {
        capability.redis_mcp_tools.is_empty() || capability.disposition != "planned"
    });
    assert_eq!(
        scorecard.current_gate.met,
        contract_met && strategic_met && redis_mcp_met
    );

    let planned_issues = scorecard
        .capabilities
        .iter()
        .filter_map(|capability| capability.issue)
        .collect::<BTreeSet<_>>();
    let blockers = scorecard
        .current_gate
        .blockers
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    assert!(planned_issues.is_subset(&blockers));
    let quality_issues = scorecard
        .capabilities
        .iter()
        .flat_map(|capability| capability.quality_issues.iter().copied())
        .collect::<BTreeSet<_>>();
    assert!(quality_issues.is_subset(&blockers));
}
