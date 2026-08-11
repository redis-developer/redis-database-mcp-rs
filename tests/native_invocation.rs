use std::{
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use redis_mcp::{
    AccessMode, CapabilityStatus, NativeRedisInvocation, OutputBudget, RawCommandPolicy,
    RedisCapabilities, RedisCommand, RedisError, RedisErrorKind, RedisExecutor,
    RedisInvocationEngine, RedisMcp, RedisModule, RedisModuleCapability, RedisOutputLimitDimension,
    RedisValue, RedisVersion,
};

#[derive(Clone)]
struct FakeExecutor {
    response: Result<RedisValue, RedisError>,
    delay: Duration,
    commands: Arc<Mutex<Vec<RedisCommand>>>,
}

impl FakeExecutor {
    fn returning(value: RedisValue) -> Self {
        Self {
            response: Ok(value),
            delay: Duration::ZERO,
            commands: Arc::default(),
        }
    }

    fn failing(error: RedisError) -> Self {
        Self {
            response: Err(error),
            delay: Duration::ZERO,
            commands: Arc::default(),
        }
    }

    fn delayed(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    fn command_count(&self) -> usize {
        self.commands.lock().expect("command lock").len()
    }

    fn last_command(&self) -> RedisCommand {
        self.commands
            .lock()
            .expect("command lock")
            .last()
            .expect("recorded command")
            .clone()
    }
}

#[async_trait]
impl RedisExecutor for FakeExecutor {
    async fn execute(&self, command: RedisCommand) -> Result<RedisValue, RedisError> {
        self.commands.lock().expect("command lock").push(command);
        if !self.delay.is_zero() {
            tokio::time::sleep(self.delay).await;
        }
        self.response.clone()
    }
}

fn engine(executor: FakeExecutor, access: AccessMode) -> RedisInvocationEngine {
    RedisInvocationEngine::builder(executor)
        .access(access)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build()
}

#[test]
fn one_shared_executor_can_back_mcp_and_native_frontends() {
    let executor: Arc<dyn RedisExecutor> = Arc::new(FakeExecutor::returning(RedisValue::Okay));
    let _router = RedisMcp::builder(executor.clone()).build();
    let _native = RedisInvocationEngine::builder(executor)
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .build();
}

#[tokio::test]
async fn classified_policy_enforces_read_write_and_destructive_access() {
    let executor = FakeExecutor::returning(RedisValue::Okay);
    let read_only = engine(executor.clone(), AccessMode::ReadOnly);

    read_only
        .invoke(NativeRedisInvocation::new("GET").arg("key"))
        .await
        .expect("GET is read-only");
    let set = read_only
        .invoke(NativeRedisInvocation::new("SET").arg("key").arg("value"))
        .await
        .expect_err("SET requires read-write access");
    assert_eq!(set.kind(), RedisErrorKind::Authorization);
    assert_eq!(set.code(), Some("COMMAND_ACCESS_DENIED"));
    assert_eq!(executor.command_count(), 1, "denied commands never execute");

    engine(executor.clone(), AccessMode::ReadWrite)
        .invoke(NativeRedisInvocation::new("SET").arg("key").arg("value"))
        .await
        .expect("SET is an ordinary write");
    let delete = engine(executor.clone(), AccessMode::ReadWrite)
        .invoke(NativeRedisInvocation::new("DEL").arg("key"))
        .await
        .expect_err("DEL requires full access");
    assert_eq!(delete.kind(), RedisErrorKind::Authorization);
    engine(executor, AccessMode::Full)
        .invoke(NativeRedisInvocation::new("DEL").arg("key"))
        .await
        .expect("full access permits DEL");
}

#[tokio::test]
async fn raw_policy_cannot_be_bypassed() {
    let executor = FakeExecutor::returning(RedisValue::Okay);
    let disabled = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::Full)
        .build();
    let error = disabled
        .invoke(NativeRedisInvocation::new("GET").arg("key"))
        .await
        .expect_err("disabled native invocation");
    assert_eq!(error.code(), Some("RAW_COMMANDS_DISABLED"));

    let classified = engine(executor.clone(), AccessMode::Full);
    let error = classified
        .invoke(NativeRedisInvocation::new("FUTURE.COMMAND"))
        .await
        .expect_err("classified mode fails closed");
    assert_eq!(error.code(), Some("COMMAND_UNCLASSIFIED"));

    let unrestricted_read_write = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::ReadWrite)
        .raw_command_policy(RawCommandPolicy::Unrestricted)
        .build();
    let error = unrestricted_read_write
        .invoke(NativeRedisInvocation::new("FUTURE.COMMAND"))
        .await
        .expect_err("unknown commands require full access");
    assert_eq!(error.code(), Some("COMMAND_ACCESS_DENIED"));

    let unrestricted_full = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Unrestricted)
        .build();
    unrestricted_full
        .invoke(NativeRedisInvocation::new("FUTURE.COMMAND"))
        .await
        .expect("explicit unrestricted full-access command");
    let auth = unrestricted_full
        .invoke(
            NativeRedisInvocation::new("AUTH")
                .arg("default")
                .arg("secret"),
        )
        .await
        .expect_err("session commands remain hard-blocked");
    assert_eq!(auth.code(), Some("SESSION_COMMAND_UNSUPPORTED"));
}

#[tokio::test]
async fn binary_argv_and_resp_values_round_trip_exactly() {
    let executor = FakeExecutor::returning(RedisValue::BulkString(vec![0xff, 0x00]));
    let engine = engine(executor.clone(), AccessMode::ReadOnly);
    let response = engine
        .invoke_with_metadata(NativeRedisInvocation::new(b"echo".to_vec()).arg(vec![0xff, 0x00]))
        .await
        .expect("binary ECHO");
    assert_eq!(response.metadata().name(), "ECHO");
    assert_eq!(response.metadata().required_access(), AccessMode::ReadOnly);
    assert_eq!(response.value(), &RedisValue::BulkString(vec![0xff, 0x00]));
    assert_eq!(executor.last_command().arguments(), &[vec![0xff, 0x00]]);
}

#[tokio::test]
async fn capability_and_version_failures_happen_before_execution() {
    let executor = FakeExecutor::returning(RedisValue::Nil);
    let unavailable = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .capabilities(
            RedisCapabilities::unknown().with_command("GET", CapabilityStatus::Unavailable),
        )
        .build();
    assert_eq!(
        unavailable
            .invoke(NativeRedisInvocation::new("GET").arg("key"))
            .await
            .expect_err("known unavailable command")
            .code(),
        Some("COMMAND_UNAVAILABLE")
    );

    let old_redis = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .capabilities(RedisCapabilities::unknown().with_redis_version(RedisVersion::new(6, 0, 0)))
        .build();
    assert_eq!(
        old_redis
            .invoke(NativeRedisInvocation::new("GETDEL").arg("key"))
            .await
            .expect_err("GETDEL requires Redis 6.2")
            .code(),
        Some("REDIS_VERSION_UNAVAILABLE")
    );

    let no_json = RedisInvocationEngine::builder(executor.clone())
        .access(AccessMode::Full)
        .raw_command_policy(RawCommandPolicy::Classified)
        .capabilities(
            RedisCapabilities::unknown()
                .with_module(RedisModule::Json, RedisModuleCapability::unavailable()),
        )
        .build();
    assert_eq!(
        no_json
            .invoke(NativeRedisInvocation::new("JSON.GET").arg("doc"))
            .await
            .expect_err("RedisJSON is known unavailable")
            .kind(),
        RedisErrorKind::ModuleUnavailable
    );
    assert_eq!(executor.command_count(), 0);
}

#[tokio::test]
async fn timeout_and_executor_errors_use_the_shared_redacted_taxonomy() {
    let delayed = FakeExecutor::returning(RedisValue::Okay).delayed(Duration::from_millis(50));
    let timeout = RedisInvocationEngine::builder(delayed)
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .command_timeout(Duration::from_millis(1))
        .build()
        .invoke(NativeRedisInvocation::new("PING"))
        .await
        .expect_err("executor timeout");
    assert_eq!(timeout.kind(), RedisErrorKind::Timeout);
    assert_eq!(timeout.code(), Some("COMMAND_TIMEOUT"));

    let secret = "native-secret-value";
    let failing = FakeExecutor::failing(
        RedisError::new(
            RedisErrorKind::Authorization,
            format!("NOPERM attempted argument {secret}"),
        )
        .with_code("NOPERM"),
    );
    let error = engine(failing, AccessMode::ReadWrite)
        .invoke(NativeRedisInvocation::new("SET").arg("key").arg(secret))
        .await
        .expect_err("executor denial");
    assert_eq!(error.kind(), RedisErrorKind::Authorization);
    assert_eq!(error.code(), Some("NOPERM"));
    assert!(!error.to_string().contains(secret));
}

#[tokio::test]
async fn native_responses_enforce_collection_and_encoded_byte_budgets() {
    let collection =
        RedisInvocationEngine::builder(FakeExecutor::returning(RedisValue::Array(vec![
            RedisValue::Integer(1),
            RedisValue::Integer(2),
        ])))
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .output_budget(OutputBudget::new(1_024, 1))
        .build()
        .invoke(NativeRedisInvocation::new("SMEMBERS").arg("set"))
        .await
        .expect_err("collection limit");
    assert_eq!(collection.kind(), RedisErrorKind::OutputLimit);
    let collection = collection.output_limit().expect("collection limit details");
    assert_eq!(
        collection.dimension(),
        RedisOutputLimitDimension::CollectionEntries
    );
    assert_eq!(collection.actual(), 2);
    assert_eq!(collection.limit(), 1);

    let encoded =
        RedisInvocationEngine::builder(FakeExecutor::returning(RedisValue::BulkString(vec![
            0xff;
            16
        ])))
        .access(AccessMode::ReadOnly)
        .raw_command_policy(RawCommandPolicy::Classified)
        .output_budget(OutputBudget::new(16, 100))
        .build()
        .invoke(NativeRedisInvocation::new("ECHO").arg("value"))
        .await
        .expect_err("encoded byte limit");
    let encoded = encoded.output_limit().expect("encoded limit details");
    assert_eq!(encoded.dimension(), RedisOutputLimitDimension::EncodedBytes);
    assert!(encoded.actual() > encoded.limit());
}
