//! Redis target capabilities and version-aware tool availability.

use std::{collections::BTreeMap, fmt, str::FromStr, time::Duration};

use crate::{
    AccessMode, RedisCommand, RedisError, RedisErrorKind, RedisExecutor, RedisModule, RedisValue,
    ToolDeploymentRequirement, ToolMetadata, tool_catalog,
};

/// Default total time allowed for direct target capability discovery.
pub const DEFAULT_CAPABILITY_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(5);

/// A semantic Redis or module version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RedisVersion {
    major: u64,
    minor: u64,
    patch: u64,
}

impl RedisVersion {
    pub const fn new(major: u64, minor: u64, patch: u64) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    pub const fn major(self) -> u64 {
        self.major
    }

    pub const fn minor(self) -> u64 {
        self.minor
    }

    pub const fn patch(self) -> u64 {
        self.patch
    }

    fn from_module_integer(version: i64) -> Option<Self> {
        let version = u64::try_from(version).ok()?;
        Some(Self::new(
            version / 10_000,
            (version % 10_000) / 100,
            version % 100,
        ))
    }
}

impl fmt::Display for RedisVersion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

impl FromStr for RedisVersion {
    type Err = RedisVersionParseError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let mut components = value.split('.');
        let major = parse_version_component(components.next(), value)?;
        let minor = parse_version_component(components.next(), value)?;
        let patch = match components.next() {
            Some(component) => parse_version_component(Some(component), value)?,
            None => 0,
        };
        Ok(Self::new(major, minor, patch))
    }
}

fn parse_version_component(
    component: Option<&str>,
    original: &str,
) -> Result<u64, RedisVersionParseError> {
    let component = component.ok_or_else(|| RedisVersionParseError(original.to_string()))?;
    let digits = component
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    if digits.is_empty() {
        return Err(RedisVersionParseError(original.to_string()));
    }
    digits
        .parse()
        .map_err(|_| RedisVersionParseError(original.to_string()))
}

/// A version string that could not be interpreted as `major.minor[.patch]`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisVersionParseError(String);

impl fmt::Display for RedisVersionParseError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid Redis version: {}", self.0)
    }
}

impl std::error::Error for RedisVersionParseError {}

/// Whether a capability is known to be present on the target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum CapabilityStatus {
    Available,
    Unavailable,
    #[default]
    Unknown,
}

impl CapabilityStatus {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Available => "available",
            Self::Unavailable => "unavailable",
            Self::Unknown => "unknown",
        }
    }
}

/// Deployment mode reported by a Redis target or fixed-target adapter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum RedisDeployment {
    Standalone,
    Cluster,
    #[default]
    Unknown,
}

/// How a router handles tools that are known not to work on its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum UnavailableToolPolicy {
    /// Keep the stable tool surface and return a capability-specific error.
    #[default]
    Advertise,
    /// Remove known-unavailable tools from `tools/list` and reject direct calls.
    Hide,
}

impl RedisDeployment {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Standalone => "standalone",
            Self::Cluster => "cluster",
            Self::Unknown => "unknown",
        }
    }
}

/// Known state and optional version of one Redis module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RedisModuleCapability {
    status: CapabilityStatus,
    version: Option<RedisVersion>,
}

impl RedisModuleCapability {
    pub const fn available(version: Option<RedisVersion>) -> Self {
        Self {
            status: CapabilityStatus::Available,
            version,
        }
    }

    pub const fn unavailable() -> Self {
        Self {
            status: CapabilityStatus::Unavailable,
            version: None,
        }
    }

    pub const fn unknown() -> Self {
        Self {
            status: CapabilityStatus::Unknown,
            version: None,
        }
    }

    pub const fn status(self) -> CapabilityStatus {
        self.status
    }

    pub const fn version(self) -> Option<RedisVersion> {
        self.version
    }
}

/// An authoritative or partially known snapshot of one Redis target.
///
/// Missing entries remain `Unknown`, which preserves compatibility for custom
/// executors that cannot or should not introspect their targets.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RedisCapabilities {
    redis_version: Option<RedisVersion>,
    deployment: RedisDeployment,
    modules: BTreeMap<RedisModule, RedisModuleCapability>,
    commands: BTreeMap<String, CapabilityStatus>,
}

impl RedisCapabilities {
    /// Construct a conservative snapshot in which every capability is unknown.
    pub fn unknown() -> Self {
        Self::default()
    }

    pub fn redis_version(&self) -> Option<RedisVersion> {
        self.redis_version
    }

    pub fn deployment(&self) -> RedisDeployment {
        self.deployment
    }

    pub fn module(&self, module: RedisModule) -> RedisModuleCapability {
        self.modules.get(&module).copied().unwrap_or_default()
    }

    pub fn command(&self, command: &str) -> CapabilityStatus {
        self.commands
            .get(&normalize_command(command))
            .copied()
            .unwrap_or_default()
    }

    pub fn with_redis_version(mut self, version: RedisVersion) -> Self {
        self.redis_version = Some(version);
        self
    }

    pub fn with_deployment(mut self, deployment: RedisDeployment) -> Self {
        self.deployment = deployment;
        self
    }

    pub fn with_module(mut self, module: RedisModule, capability: RedisModuleCapability) -> Self {
        self.modules.insert(module, capability);
        self
    }

    pub fn with_command(mut self, command: impl AsRef<str>, status: CapabilityStatus) -> Self {
        self.commands
            .insert(normalize_command(command.as_ref()), status);
        self
    }

    /// Replace command knowledge with an authoritative inventory of available
    /// command names. Every command required by this library but absent from
    /// the inventory becomes unavailable.
    pub fn with_command_inventory<I, S>(mut self, commands: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        for command in required_command_names() {
            self.commands
                .insert(normalize_command(command), CapabilityStatus::Unavailable);
        }
        for command in commands {
            self.commands.insert(
                normalize_command(command.as_ref()),
                CapabilityStatus::Available,
            );
        }
        self
    }

    /// Replace module knowledge with an authoritative inventory of available
    /// modules and their optional versions. Supported modules absent from the
    /// inventory become unavailable.
    pub fn with_module_inventory<I>(mut self, modules: I) -> Self
    where
        I: IntoIterator<Item = (RedisModule, Option<RedisVersion>)>,
    {
        for module in [RedisModule::Json, RedisModule::Search] {
            self.modules
                .insert(module, RedisModuleCapability::unavailable());
        }
        for (module, version) in modules {
            self.modules
                .insert(module, RedisModuleCapability::available(version));
        }
        self
    }

    /// Evaluate the known requirements for a catalog tool.
    pub fn tool_status(&self, tool: ToolMetadata) -> CapabilityStatus {
        match self.check_tool(tool) {
            Ok(status) => status,
            Err(_) => CapabilityStatus::Unavailable,
        }
    }

    pub(crate) fn check_tool(&self, tool: ToolMetadata) -> Result<CapabilityStatus, RedisError> {
        let requirements = tool.capability_requirements();
        let mut status = CapabilityStatus::Available;

        let deployment_matches = match requirements.deployment() {
            ToolDeploymentRequirement::Any => true,
            ToolDeploymentRequirement::Standalone => self.deployment != RedisDeployment::Cluster,
            ToolDeploymentRequirement::Cluster => self.deployment != RedisDeployment::Standalone,
        };
        if !deployment_matches {
            return Err(RedisError::new(
                RedisErrorKind::CapabilityUnavailable,
                format!(
                    "{} requires a {} Redis target; configured target is {}",
                    tool.name,
                    requirements.deployment().as_str(),
                    self.deployment.as_str()
                ),
            )
            .with_code("DEPLOYMENT_UNAVAILABLE"));
        }
        if self.deployment == RedisDeployment::Unknown
            && requirements.deployment() != ToolDeploymentRequirement::Any
        {
            status = CapabilityStatus::Unknown;
        }

        if let Some(minimum) = requirements.minimum_redis_version() {
            match self.redis_version {
                Some(actual) if actual < minimum => {
                    return Err(RedisError::new(
                        RedisErrorKind::CapabilityUnavailable,
                        format!(
                            "{} requires Redis {minimum} or newer; target reports {actual}",
                            tool.name
                        ),
                    )
                    .with_code("REDIS_VERSION_UNAVAILABLE"));
                }
                Some(_) => {}
                None => status = CapabilityStatus::Unknown,
            }
        }

        if let Some(module) = requirements.required_module() {
            let capability = self.module(module);
            match capability.status() {
                CapabilityStatus::Unavailable => {
                    return Err(RedisError::new(
                        RedisErrorKind::ModuleUnavailable,
                        format!("{} is unavailable on the configured Redis target", module),
                    )
                    .with_code("MODULE_UNAVAILABLE"));
                }
                CapabilityStatus::Unknown => status = CapabilityStatus::Unknown,
                CapabilityStatus::Available => {}
            }
            if let Some(minimum) = requirements.minimum_module_version() {
                match capability.version() {
                    Some(actual) if actual < minimum => {
                        return Err(RedisError::new(
                            RedisErrorKind::ModuleUnavailable,
                            format!(
                                "{} requires {} {minimum} or newer; target reports {actual}",
                                tool.name, module
                            ),
                        )
                        .with_code("MODULE_VERSION_UNAVAILABLE"));
                    }
                    Some(_) => {}
                    None => status = CapabilityStatus::Unknown,
                }
            }
        }

        for command in requirements.required_commands() {
            match self.command(command) {
                CapabilityStatus::Unavailable => {
                    return Err(RedisError::new(
                        RedisErrorKind::CapabilityUnavailable,
                        format!(
                            "{} requires Redis command {command}, which is unavailable on the configured target",
                            tool.name
                        ),
                    )
                    .with_code("COMMAND_UNAVAILABLE"));
                }
                CapabilityStatus::Unknown => status = CapabilityStatus::Unknown,
                CapabilityStatus::Available => {}
            }
        }

        Ok(status)
    }
}

fn normalize_command(command: &str) -> String {
    command.trim().to_ascii_uppercase()
}

pub(crate) async fn discover_capabilities(
    executor: &dyn RedisExecutor,
    timeout: Duration,
    deployment_hint: RedisDeployment,
) -> Result<RedisCapabilities, RedisError> {
    if timeout.is_zero() {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "capability discovery timeout must be greater than zero",
        )
        .with_code("ZERO_CAPABILITY_DISCOVERY_TIMEOUT"));
    }

    tokio::time::timeout(
        timeout,
        discover_capabilities_inner(executor, deployment_hint),
    )
    .await
    .map_err(|_| {
        RedisError::new(
            RedisErrorKind::Timeout,
            format!(
                "Redis capability discovery timed out after {} ms",
                timeout.as_millis()
            ),
        )
        .with_code("CAPABILITY_DISCOVERY_TIMEOUT")
    })?
}

async fn discover_capabilities_inner(
    executor: &dyn RedisExecutor,
    deployment_hint: RedisDeployment,
) -> Result<RedisCapabilities, RedisError> {
    let mut info = RedisCommand::new("redis_capability_discovery", AccessMode::ReadOnly, "INFO");
    info.arg("server");
    let info = executor.execute(info).await?;
    let mut capabilities = parse_server_info(&info).unwrap_or_default();
    if capabilities.deployment == RedisDeployment::Unknown {
        capabilities.deployment = deployment_hint;
    }
    if deployment_hint == RedisDeployment::Cluster {
        capabilities.deployment = RedisDeployment::Cluster;
    }

    let module_command =
        RedisCommand::new("redis_capability_discovery", AccessMode::ReadOnly, "MODULE");
    let mut module_command = module_command;
    module_command.arg("LIST");
    if let Ok(modules) = executor.execute(module_command).await {
        for module in [RedisModule::Json, RedisModule::Search] {
            capabilities
                .modules
                .insert(module, RedisModuleCapability::unavailable());
        }
        for (module, version) in parse_modules(&modules) {
            capabilities
                .modules
                .insert(module, RedisModuleCapability::available(version));
        }
    }

    let commands = required_command_names();
    let mut command_info = RedisCommand::new(
        "redis_capability_discovery",
        AccessMode::ReadOnly,
        "COMMAND",
    );
    command_info.arg("INFO").args(commands.iter().copied());
    if let Ok(value) = executor.execute(command_info).await {
        for (command, status) in parse_command_info(&value, &commands) {
            capabilities.commands.insert(command, status);
        }
    }

    Ok(capabilities)
}

fn parse_server_info(value: &RedisValue) -> Option<RedisCapabilities> {
    if let RedisValue::Map(entries) = value {
        let discovered = entries
            .iter()
            .filter_map(|(_, value)| parse_server_info(value))
            .collect::<Vec<_>>();
        if !discovered.is_empty() {
            return Some(RedisCapabilities {
                redis_version: discovered
                    .iter()
                    .filter_map(RedisCapabilities::redis_version)
                    .min(),
                deployment: if discovered
                    .iter()
                    .any(|capabilities| capabilities.deployment() == RedisDeployment::Cluster)
                {
                    RedisDeployment::Cluster
                } else {
                    discovered
                        .first()
                        .map(RedisCapabilities::deployment)
                        .unwrap_or_default()
                },
                ..RedisCapabilities::default()
            });
        }
    }
    let text = value_text(value)?;
    let mut capabilities = RedisCapabilities::unknown();
    for line in text.lines() {
        let Some((key, value)) = line.trim_end_matches('\r').split_once(':') else {
            continue;
        };
        match key {
            "redis_version" => capabilities.redis_version = value.parse().ok(),
            "redis_mode" => {
                capabilities.deployment = match value.to_ascii_lowercase().as_str() {
                    "cluster" => RedisDeployment::Cluster,
                    "standalone" => RedisDeployment::Standalone,
                    _ => RedisDeployment::Unknown,
                }
            }
            _ => {}
        }
    }
    Some(capabilities)
}

fn parse_modules(value: &RedisValue) -> Vec<(RedisModule, Option<RedisVersion>)> {
    if let RedisValue::Map(entries) = value
        && !entries
            .iter()
            .any(|(key, _)| value_text(key).is_some_and(|key| key == "name"))
    {
        let snapshots = entries
            .iter()
            .map(|(_, value)| parse_modules(value).into_iter().collect::<BTreeMap<_, _>>())
            .collect::<Vec<_>>();
        let Some(first) = snapshots.first() else {
            return Vec::new();
        };
        return first
            .iter()
            .filter(|(module, _)| {
                snapshots
                    .iter()
                    .all(|snapshot| snapshot.contains_key(module))
            })
            .map(|(module, first_version)| {
                let version = snapshots
                    .iter()
                    .map(|snapshot| snapshot[module])
                    .try_fold(*first_version, |minimum, version| {
                        Some(match (minimum, version) {
                            (Some(left), Some(right)) => Some(left.min(right)),
                            _ => None,
                        })
                    })
                    .flatten();
                (*module, version)
            })
            .collect();
    }
    let (RedisValue::Array(entries) | RedisValue::Set(entries)) = value else {
        return Vec::new();
    };
    entries
        .iter()
        .filter_map(|entry| {
            let pairs = value_pairs(entry)?;
            let name = pairs
                .iter()
                .find(|(key, _)| value_text(key).is_some_and(|key| key == "name"))
                .and_then(|(_, value)| value_text(value))?;
            let module = match name.to_ascii_lowercase().as_str() {
                "rejson" | "redisjson" | "json" => RedisModule::Json,
                "search" | "redisearch" | "ft" => RedisModule::Search,
                _ => return None,
            };
            let version = pairs
                .iter()
                .find(|(key, _)| value_text(key).is_some_and(|key| key == "ver"))
                .and_then(|(_, value)| match value {
                    RedisValue::Integer(version) => RedisVersion::from_module_integer(*version),
                    _ => value_text(value).and_then(|version| version.parse().ok()),
                });
            Some((module, version))
        })
        .collect()
}

fn value_pairs(value: &RedisValue) -> Option<Vec<(&RedisValue, &RedisValue)>> {
    match value {
        RedisValue::Map(pairs) => Some(pairs.iter().map(|(key, value)| (key, value)).collect()),
        RedisValue::Array(values) if values.len() % 2 == 0 => Some(
            values
                .chunks_exact(2)
                .map(|pair| (&pair[0], &pair[1]))
                .collect(),
        ),
        _ => None,
    }
}

fn parse_command_info(
    value: &RedisValue,
    commands: &[&'static str],
) -> Vec<(String, CapabilityStatus)> {
    match value {
        RedisValue::Array(entries) => commands
            .iter()
            .zip(entries)
            .map(|(command, entry)| {
                (
                    normalize_command(command),
                    if matches!(entry, RedisValue::Nil) {
                        CapabilityStatus::Unavailable
                    } else {
                        CapabilityStatus::Available
                    },
                )
            })
            .collect(),
        RedisValue::Map(entries)
            if entries.iter().any(|(key, _)| {
                value_text(key).is_some_and(|key| {
                    commands
                        .iter()
                        .any(|command| command.eq_ignore_ascii_case(key))
                })
            }) =>
        {
            let mut status = commands
                .iter()
                .map(|command| (normalize_command(command), CapabilityStatus::Unavailable))
                .collect::<BTreeMap<_, _>>();
            for (key, value) in entries {
                if let Some(key) = value_text(key) {
                    let key = normalize_command(key);
                    if status.contains_key(&key) && !matches!(value, RedisValue::Nil) {
                        status.insert(key, CapabilityStatus::Available);
                    }
                }
            }
            status.into_iter().collect()
        }
        RedisValue::Map(entries) => {
            let snapshots = entries
                .iter()
                .map(|(_, value)| {
                    parse_command_info(value, commands)
                        .into_iter()
                        .collect::<BTreeMap<_, _>>()
                })
                .collect::<Vec<_>>();
            commands
                .iter()
                .map(|command| {
                    let command = normalize_command(command);
                    let status = if snapshots.iter().any(|snapshot| {
                        snapshot.get(&command) == Some(&CapabilityStatus::Unavailable)
                    }) {
                        CapabilityStatus::Unavailable
                    } else if !snapshots.is_empty()
                        && snapshots.iter().all(|snapshot| {
                            snapshot.get(&command) == Some(&CapabilityStatus::Available)
                        })
                    {
                        CapabilityStatus::Available
                    } else {
                        CapabilityStatus::Unknown
                    };
                    (command, status)
                })
                .collect()
        }
        _ => Vec::new(),
    }
}

fn value_text(value: &RedisValue) -> Option<&str> {
    match value {
        RedisValue::BulkString(value) | RedisValue::BigNumber(value) => {
            std::str::from_utf8(value).ok()
        }
        RedisValue::SimpleString(value) | RedisValue::VerbatimString { text: value, .. } => {
            Some(value)
        }
        _ => None,
    }
}

fn required_command_names() -> Vec<&'static str> {
    let mut commands = tool_catalog()
        .iter()
        .flat_map(|tool| tool.capability_requirements().required_commands())
        .copied()
        .collect::<Vec<_>>();
    commands.sort_unstable();
    commands.dedup();
    commands
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;

    #[test]
    fn versions_parse_release_suffixes_and_compare_semantically() {
        assert_eq!("8.2.1".parse(), Ok(RedisVersion::new(8, 2, 1)));
        assert_eq!("8.4-rc1".parse(), Ok(RedisVersion::new(8, 4, 0)));
        assert!(RedisVersion::new(8, 2, 0) > RedisVersion::new(7, 4, 9));
        assert!("not-a-version".parse::<RedisVersion>().is_err());
    }

    #[test]
    fn module_integer_versions_follow_redis_encoding() {
        assert_eq!(
            RedisVersion::from_module_integer(20803),
            Some(RedisVersion::new(2, 8, 3))
        );
    }

    #[derive(Clone, Copy)]
    struct SlowDiscovery;

    #[async_trait]
    impl RedisExecutor for SlowDiscovery {
        async fn execute(&self, _command: RedisCommand) -> Result<RedisValue, RedisError> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(RedisValue::Nil)
        }
    }

    #[tokio::test]
    async fn discovery_has_one_bounded_total_timeout() {
        let error = discover_capabilities(
            &SlowDiscovery,
            Duration::from_millis(5),
            RedisDeployment::Standalone,
        )
        .await
        .expect_err("slow discovery should time out");
        assert_eq!(error.kind(), RedisErrorKind::Timeout);
        assert_eq!(error.code(), Some("CAPABILITY_DISCOVERY_TIMEOUT"));
    }

    #[test]
    fn cluster_info_uses_the_oldest_reported_node_version() {
        let info = RedisValue::Map(vec![
            (
                RedisValue::BulkString(b"node-a".to_vec()),
                RedisValue::BulkString(
                    b"# Server\r\nredis_version:8.2.1\r\nredis_mode:cluster\r\n".to_vec(),
                ),
            ),
            (
                RedisValue::BulkString(b"node-b".to_vec()),
                RedisValue::BulkString(
                    b"# Server\r\nredis_version:7.4.9\r\nredis_mode:cluster\r\n".to_vec(),
                ),
            ),
        ]);
        let capabilities = parse_server_info(&info).expect("parse cluster INFO map");
        assert_eq!(
            capabilities.redis_version(),
            Some(RedisVersion::new(7, 4, 9))
        );
        assert_eq!(capabilities.deployment(), RedisDeployment::Cluster);
    }

    fn module_entry(name: &str, version: i64) -> RedisValue {
        RedisValue::Array(vec![
            RedisValue::BulkString(b"name".to_vec()),
            RedisValue::BulkString(name.as_bytes().to_vec()),
            RedisValue::BulkString(b"ver".to_vec()),
            RedisValue::Integer(version),
        ])
    }

    #[test]
    fn cluster_module_discovery_requires_every_node_and_uses_oldest_version() {
        let mixed = RedisValue::Map(vec![
            (
                RedisValue::BulkString(b"node-a".to_vec()),
                RedisValue::Array(vec![module_entry("ReJSON", 20803)]),
            ),
            (
                RedisValue::BulkString(b"node-b".to_vec()),
                RedisValue::Array(Vec::new()),
            ),
        ]);
        assert!(parse_modules(&mixed).is_empty());

        let complete = RedisValue::Map(vec![
            (
                RedisValue::BulkString(b"node-a".to_vec()),
                RedisValue::Array(vec![module_entry("ReJSON", 20803)]),
            ),
            (
                RedisValue::BulkString(b"node-b".to_vec()),
                RedisValue::Array(vec![module_entry("ReJSON", 20600)]),
            ),
        ]);
        assert_eq!(
            parse_modules(&complete),
            vec![(RedisModule::Json, Some(RedisVersion::new(2, 6, 0)))]
        );
    }

    #[test]
    fn cluster_command_discovery_requires_every_node() {
        let response = RedisValue::Map(vec![
            (
                RedisValue::BulkString(b"node-a".to_vec()),
                RedisValue::Array(vec![RedisValue::Array(vec![])]),
            ),
            (
                RedisValue::BulkString(b"node-b".to_vec()),
                RedisValue::Array(vec![RedisValue::Nil]),
            ),
        ]);
        assert_eq!(
            parse_command_info(&response, &["GET"]),
            vec![("GET".to_string(), CapabilityStatus::Unavailable)]
        );
    }
}
