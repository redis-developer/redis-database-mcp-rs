//! Connection configuration shared by the direct redis-tower adapters.

use crate::{RedisError, RedisErrorKind};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_core::{ConnectionConfig, ProtocolVersion, RedisConnection};
use std::time::Duration;

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Retain the redis-rs URL protocol option at the application boundary.
/// The tower URL parser consumes transport/authentication, not protocol query options.
#[derive(Clone)]
pub(crate) struct Target {
    pub url: String,
    pub config: ConnectionConfig,
}

impl Target {
    pub fn parse(input: &str) -> Result<Self, RedisError> {
        let mut url = url::Url::parse(input).map_err(|_| invalid_url())?;
        let mut protocol = ProtocolVersion::Resp2;
        let mut database = None;
        for (name, value) in url.query_pairs() {
            match name.as_ref() {
                "protocol" => {
                    protocol = match value.as_ref() {
                        "resp2" | "2" => ProtocolVersion::Resp2,
                        "resp3" | "3" => ProtocolVersion::Resp3,
                        _ => return Err(invalid_url()),
                    }
                }
                "db" => database = Some(value.parse::<u16>().map_err(|_| invalid_url())?),
                _ => {}
            }
        }
        url.set_query(None);
        url.set_fragment(None);
        if url.scheme() == "redis+unix" {
            // redis-tower names the same Unix transport `unix`.
            url.set_scheme("unix").map_err(|_| invalid_url())?;
        }
        if url.scheme() == "unix"
            && let Some(database) = database
        {
            url.set_query(Some(&format!("db={database}")));
        }
        let url = url.to_string();
        redis_tower_core::parse_redis_url(&url).map_err(|_| invalid_url())?;
        Ok(Self {
            url,
            config: ConnectionConfig::default()
                .with_protocol(protocol)
                .with_connect_timeout(Some(CONNECT_TIMEOUT)),
        })
    }

    pub async fn connect(&self) -> Result<RedisConnection, RedisError> {
        tokio::time::timeout(
            CONNECT_TIMEOUT,
            RedisConnection::connect_url_with_config(&self.url, &self.config),
        )
        .await
        .map_err(|_| RedisError::new(RedisErrorKind::Timeout, "Redis connection setup timed out"))?
        .map_err(RedisError::from)
    }

    pub async fn exclusive_cluster(
        &self,
    ) -> Result<redis_tower_cluster::ClusterConnection, RedisError> {
        let parsed = redis_tower_core::parse_redis_url(&self.url).map_err(RedisError::from)?;
        if parsed.unix || parsed.database.is_some_and(|db| db != 0) {
            return Err(invalid_url());
        }
        let mut builder = redis_tower_cluster::ClusterConnection::builder(format!(
            "{}:{}",
            parsed.host, parsed.port
        ))
        .connection_config(self.config.clone())
        .max_redirects(0);
        if let Some(password) = parsed.password {
            let credentials = match parsed.username {
                Some(user) if !user.is_empty() => {
                    redis_tower::credentials::StaticCredentials::new(user, password)
                }
                _ => redis_tower::credentials::StaticCredentials::password(password),
            };
            builder = builder.credentials(credentials);
        }
        if parsed.tls {
            builder = builder.tls(redis_tower_core::tls::TlsConfig::default_rustls());
        }
        tokio::time::timeout(CONNECT_TIMEOUT, builder.connect())
            .await
            .map_err(|_| {
                RedisError::new(
                    RedisErrorKind::Timeout,
                    "Redis Cluster connection setup timed out",
                )
            })?
            .map_err(RedisError::from)
    }

    pub fn cluster_builder(
        &self,
    ) -> Result<redis_tower_cluster::MultiplexedClusterClientBuilder, RedisError> {
        let parsed = redis_tower_core::parse_redis_url(&self.url).map_err(RedisError::from)?;
        if parsed.unix || parsed.database.is_some_and(|db| db != 0) {
            return Err(invalid_url());
        }
        let mut builder =
            MultiplexedClusterClient::builder(format!("{}:{}", parsed.host, parsed.port))
                .connection_config(self.config.clone());
        if let Some(password) = parsed.password {
            let credentials = match parsed.username {
                Some(user) if !user.is_empty() => {
                    redis_tower::credentials::StaticCredentials::new(user, password)
                }
                _ => redis_tower::credentials::StaticCredentials::password(password),
            };
            builder = builder.credentials(credentials);
        }
        if parsed.tls {
            builder = builder.tls(redis_tower_core::tls::TlsConfig::default_rustls());
        }
        Ok(builder)
    }
}

pub(crate) async fn connect_cluster(
    targets: &[Target],
) -> Result<MultiplexedClusterClient, RedisError> {
    let mut last = None;
    for target in targets {
        let attempt = target.cluster_builder()?.connect();
        match tokio::time::timeout(CONNECT_TIMEOUT, attempt).await {
            Ok(Ok(client)) => return Ok(client),
            Ok(Err(error)) => last = Some(RedisError::from(error)),
            Err(_) => {
                last = Some(RedisError::new(
                    RedisErrorKind::Timeout,
                    "Redis Cluster connection setup timed out",
                ))
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        RedisError::new(
            RedisErrorKind::InvalidRequest,
            "at least one Redis Cluster seed URL is required",
        )
        .with_code("EMPTY_CLUSTER_SEEDS")
    }))
}

fn invalid_url() -> RedisError {
    RedisError::new(
        RedisErrorKind::InvalidRequest,
        "invalid Redis target URL or protocol option",
    )
    .with_code("INVALID_REDIS_URL")
}
