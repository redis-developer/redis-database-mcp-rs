//! Connection configuration shared by the direct redis-tower adapters.

use crate::{RedisError, RedisErrorKind};
use redis_tower::{commands::RawCommand, reconnect::ConnectionFactory};
use redis_tower_cluster::MultiplexedClusterClient;
use redis_tower_core::{
    ConnectionConfig, ProtocolVersion, RedisConnection, RedisError as TowerError,
};
use std::{future::Future, pin::Pin, time::Duration};

pub(crate) const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Retain redis-rs protocol and authenticated Unix URL options at the
/// application boundary while using redis-tower for the transport.
#[derive(Clone)]
pub(crate) struct Target {
    pub url: String,
    pub config: ConnectionConfig,
    unix_setup: Option<UnixSetup>,
}

#[derive(Clone)]
struct UnixSetup {
    username: Option<String>,
    password: Option<String>,
    database: Option<u16>,
}

impl Target {
    pub fn parse(input: &str) -> Result<Self, RedisError> {
        let mut url = url::Url::parse(input).map_err(|_| invalid_url())?;
        let mut protocol = ProtocolVersion::Resp2;
        let mut database = None;
        let mut username = None;
        let mut password = None;
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
                "user" => username = Some(value.into_owned()),
                "pass" => password = Some(value.into_owned()),
                _ => {}
            }
        }
        let unix = matches!(url.scheme(), "unix" | "redis+unix" | "valkey+unix");
        if unix && username.is_some() && password.is_none() {
            return Err(invalid_url());
        }
        url.set_query(None);
        url.set_fragment(None);
        if matches!(url.scheme(), "redis+unix" | "valkey+unix") {
            // redis-tower names the same Unix transport `unix`.
            url.set_scheme("unix").map_err(|_| invalid_url())?;
        }
        let url = url.to_string();
        redis_tower_core::parse_redis_url(&url).map_err(|_| invalid_url())?;
        Ok(Self {
            url,
            config: ConnectionConfig::default()
                .with_protocol(protocol)
                .with_connect_timeout(Some(CONNECT_TIMEOUT)),
            unix_setup: unix.then_some(UnixSetup {
                username,
                password,
                database,
            }),
        })
    }

    pub async fn connect(&self) -> Result<RedisConnection, RedisError> {
        self.connect_tower().await.map_err(RedisError::from)
    }

    async fn connect_tower(&self) -> Result<RedisConnection, TowerError> {
        tokio::time::timeout(CONNECT_TIMEOUT, self.connect_unbounded())
            .await
            .map_err(|_| TowerError::ConnectTimeout)?
    }

    async fn connect_unbounded(&self) -> Result<RedisConnection, TowerError> {
        let Some(setup) = &self.unix_setup else {
            return RedisConnection::connect_url_with_config(&self.url, &self.config).await;
        };

        // redis-tower's Unix URL parser owns transport and database parsing,
        // but redis-rs also accepted query-based ACL credentials. Establish a
        // RESP2 connection first, replay the full legacy setup, then negotiate
        // the requested protocol. This path is also used for reconnection.
        let initial = self.config.clone().with_protocol(ProtocolVersion::Resp2);
        let mut connection = RedisConnection::connect_url_with_config(&self.url, &initial).await?;
        if let Some(password) = &setup.password {
            let mut auth = RawCommand::new("AUTH");
            if let Some(username) = &setup.username {
                auth = auth.arg(username);
            }
            connection.execute(auth.arg(password)).await?;
        }
        if let Some(database) = setup.database {
            connection
                .execute(RawCommand::new("SELECT").arg(database.to_string()))
                .await?;
        }
        connection
            .negotiate_protocol(self.config.protocol())
            .await?;
        Ok(connection)
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

impl ConnectionFactory for Target {
    fn connect(&self) -> Pin<Box<dyn Future<Output = Result<RedisConnection, TowerError>> + Send>> {
        let target = self.clone();
        Box::pin(async move { target.connect_tower().await })
    }
}

pub(crate) async fn connect_cluster(
    targets: &[Target],
) -> Result<MultiplexedClusterClient, RedisError> {
    connect_cluster_with_timeout(targets, CONNECT_TIMEOUT).await
}

async fn connect_cluster_with_timeout(
    targets: &[Target],
    timeout: Duration,
) -> Result<MultiplexedClusterClient, RedisError> {
    if targets.is_empty() {
        return Err(RedisError::new(
            RedisErrorKind::InvalidRequest,
            "at least one Redis Cluster seed URL is required",
        )
        .with_code("EMPTY_CLUSTER_SEEDS"));
    }

    tokio::time::timeout(timeout, async {
        let mut last = None;
        for target in targets {
            match target.cluster_builder()?.connect().await {
                Ok(client) => return Ok(client),
                Err(error) => last = Some(RedisError::from(error)),
            }
        }
        Err(last.expect("non-empty seed list attempted at least once"))
    })
    .await
    .map_err(|_| {
        RedisError::new(
            RedisErrorKind::Timeout,
            "Redis Cluster connection setup timed out",
        )
    })?
}

fn invalid_url() -> RedisError {
    RedisError::new(
        RedisErrorKind::InvalidRequest,
        "invalid Redis target URL or protocol option",
    )
    .with_code("INVALID_REDIS_URL")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[tokio::test]
    async fn authenticated_unix_aliases_replay_acl_setup() {
        let directory = tempfile::tempdir().expect("create Redis test directory");
        let socket = directory.path().join("redis.sock");
        let acl_file = directory.path().join("users.acl");
        std::fs::write(
            &acl_file,
            "user default on nopass ~* +@all\nuser agent on >secret ~* +@all\n",
        )
        .expect("write Redis ACL file");
        let server = redis_server_wrapper::RedisServer::new()
            .auto_port()
            .bind("127.0.0.1")
            .dir(directory.path())
            .unixsocket(&socket)
            .unixsocketperm(700)
            .acl_file(&acl_file)
            .no_stack_modules()
            .start()
            .await;
        let _server = match server {
            Ok(server) => server,
            Err(redis_server_wrapper::Error::BinaryNotFound { binary }) => {
                eprintln!("skipping local Unix transport test: {binary} is not on PATH");
                return;
            }
            Err(error) => panic!("start isolated Redis: {error}"),
        };

        for scheme in ["redis+unix", "valkey+unix"] {
            let url = format!(
                "{scheme}://{}?user=agent&pass=secret&db=1&protocol=resp3",
                socket.display()
            );
            let target = Target::parse(&url).expect("parse authenticated Unix target");
            assert!(target.url.starts_with("unix:"));
            let mut connection = target.connect().await.expect("connect with ACL identity");
            let identity: String = connection
                .execute(RawCommand::new("ACL").arg("WHOAMI").query())
                .await
                .expect("query authenticated identity");
            assert_eq!(identity, "agent");
            assert!(connection.is_resp3());

            let wrong_url = format!(
                "{scheme}://{}?user=agent&pass=wrong&protocol=resp3",
                socket.display()
            );
            let result = Target::parse(&wrong_url)
                .expect("parse bad credentials")
                .connect()
                .await;
            let error = match result {
                Ok(_) => panic!("wrong ACL password must fail"),
                Err(error) => error,
            };
            assert_eq!(error.kind(), RedisErrorKind::Authentication);
            assert_eq!(error.code(), Some("WRONGPASS"));
        }
    }

    #[tokio::test]
    async fn cluster_seed_attempts_share_one_total_timeout() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stalled cluster seed");
        let address = listener.local_addr().expect("read listener address");
        let acceptor = tokio::spawn(async move {
            let mut connections = Vec::new();
            while let Ok((connection, _)) = listener.accept().await {
                connections.push(connection);
            }
        });
        let target = Target::parse(&format!("redis://{address}/")).expect("parse seed");
        let started = tokio::time::Instant::now();
        let error =
            connect_cluster_with_timeout(&[target.clone(), target], Duration::from_millis(100))
                .await
                .expect_err("stalled seeds must time out");
        let elapsed = started.elapsed();
        acceptor.abort();

        assert_eq!(error.kind(), RedisErrorKind::Timeout);
        assert!(
            elapsed < Duration::from_millis(175),
            "seed attempts exceeded one total timeout: {elapsed:?}"
        );
    }
}
