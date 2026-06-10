//! Server configuration: a TOML file (`--config <path>` or `KAIROSD_CONFIG`)
//! with every existing `KAIROSD_*` environment variable still honored as an
//! override, so env-only deployments keep working unchanged.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_telnet")]
    pub telnet_listen: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    /// `compat` (bit-identical to Java) or `fast` (vector kernels).
    #[serde(default = "default_query_mode")]
    pub query_mode: String,
    #[serde(default)]
    pub datastore: DatastoreConfig,
    #[serde(default)]
    pub parquet: ParquetConfig,
    #[serde(default)]
    pub limits: LimitsConfig,
    #[serde(default)]
    pub rollups: RollupsConfig,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatastoreConfig {
    /// `memory` or `cassandra`.
    #[serde(default = "default_backend")]
    pub backend: String,
    #[serde(default = "default_cassandra_node")]
    pub cassandra_node: String,
    #[serde(default = "default_keyspace")]
    pub cassandra_keyspace: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ParquetConfig {
    /// Enables the cold tier when set.
    pub dir: Option<String>,
    /// Hourly auto-compaction moves points older than this; 0 disables.
    #[serde(default)]
    pub compact_older_than_ms: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LimitsConfig {
    /// Maximum raw points one query may scan; 0 = unlimited.
    #[serde(default)]
    pub max_query_points: u64,
    /// Per-query wall-clock budget; 0 = unlimited.
    #[serde(default)]
    pub query_timeout_ms: u64,
    /// Concurrent datapoint queries; 0 = unlimited.
    #[serde(default)]
    pub max_concurrent_queries: u32,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RollupsConfig {
    /// Node identity for lease-based task assignment; defaults to
    /// hostname-pid.
    #[serde(default)]
    pub node_id: String,
    /// How often nodes re-read the shared task list.
    #[serde(default = "default_refresh")]
    pub refresh_seconds: u64,
}

fn default_listen() -> String { "0.0.0.0:8080".into() }
fn default_telnet() -> String { "0.0.0.0:4242".into() }
fn default_data_dir() -> String { "kairosd-data".into() }
fn default_query_mode() -> String { "compat".into() }
fn default_backend() -> String { "memory".into() }
fn default_cassandra_node() -> String { "127.0.0.1:9042".into() }
fn default_keyspace() -> String { "kairosdb".into() }
fn default_refresh() -> u64 { 30 }

impl Default for DatastoreConfig {
    fn default() -> Self {
        DatastoreConfig {
            backend: default_backend(),
            cassandra_node: default_cassandra_node(),
            cassandra_keyspace: default_keyspace(),
        }
    }
}

impl Default for RollupsConfig {
    fn default() -> Self {
        RollupsConfig { node_id: String::new(), refresh_seconds: default_refresh() }
    }
}

impl Config {
    /// File (if any) first, then environment overrides on top.
    pub fn load() -> Config {
        let path = std::env::args()
            .skip_while(|a| a != "--config")
            .nth(1)
            .or_else(|| std::env::var("KAIROSD_CONFIG").ok());
        let mut config = match path {
            Some(path) => {
                let content = std::fs::read_to_string(&path)
                    .unwrap_or_else(|e| panic!("cannot read config {path}: {e}"));
                toml::from_str(&content)
                    .unwrap_or_else(|e| panic!("invalid config {path}: {e}"))
            }
            None => Config::load_defaults(),
        };
        config.apply_env();
        if config.rollups.node_id.is_empty() {
            let host = std::fs::read_to_string("/etc/hostname")
                .map(|h| h.trim().to_string())
                .unwrap_or_else(|_| "node".to_string());
            config.rollups.node_id = format!("{host}-{}", std::process::id());
        }
        config
    }

    fn load_defaults() -> Config {
        toml::from_str("").expect("defaults are valid")
    }

    fn apply_env(&mut self) {
        let env = |k: &str| std::env::var(k).ok();
        if let Some(v) = env("KAIROSD_LISTEN") { self.listen = v; }
        if let Some(v) = env("KAIROSD_TELNET_LISTEN") { self.telnet_listen = v; }
        if let Some(v) = env("KAIROSD_DATA_DIR") { self.data_dir = v; }
        if let Some(v) = env("KAIROSD_QUERY_MODE") { self.query_mode = v; }
        if let Some(v) = env("KAIROSD_DATASTORE") { self.datastore.backend = v; }
        if let Some(v) = env("KAIROSD_CASSANDRA_NODE") { self.datastore.cassandra_node = v; }
        if let Some(v) = env("KAIROSD_CASSANDRA_KEYSPACE") { self.datastore.cassandra_keyspace = v; }
        if let Some(v) = env("KAIROSD_PARQUET_DIR") { self.parquet.dir = Some(v); }
        if let Some(v) = env("KAIROSD_COMPACT_OLDER_THAN_MS") {
            self.parquet.compact_older_than_ms = v.parse().expect("KAIROSD_COMPACT_OLDER_THAN_MS: ms");
        }
        if let Some(v) = env("KAIROSD_MAX_QUERY_POINTS") {
            self.limits.max_query_points = v.parse().expect("KAIROSD_MAX_QUERY_POINTS: count");
        }
        if let Some(v) = env("KAIROSD_QUERY_TIMEOUT_MS") {
            self.limits.query_timeout_ms = v.parse().expect("KAIROSD_QUERY_TIMEOUT_MS: ms");
        }
        if let Some(v) = env("KAIROSD_MAX_CONCURRENT_QUERIES") {
            self.limits.max_concurrent_queries = v.parse().expect("KAIROSD_MAX_CONCURRENT_QUERIES: count");
        }
        if let Some(v) = env("KAIROSD_NODE_ID") { self.rollups.node_id = v; }
        if let Some(v) = env("KAIROSD_ROLLUP_REFRESH_SECONDS") {
            self.rollups.refresh_seconds = v.parse().expect("KAIROSD_ROLLUP_REFRESH_SECONDS: s");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_config() {
        let config: Config = toml::from_str(
            r#"
            listen = "127.0.0.1:9999"
            query_mode = "fast"
            [datastore]
            backend = "cassandra"
            cassandra_node = "db:9042"
            [parquet]
            dir = "/data/parquet"
            compact_older_than_ms = 604800000
            [limits]
            max_query_points = 10000000
            query_timeout_ms = 30000
            max_concurrent_queries = 16
            [rollups]
            node_id = "node-a"
            refresh_seconds = 10
            "#,
        )
        .unwrap();
        assert_eq!(config.listen, "127.0.0.1:9999");
        assert_eq!(config.datastore.backend, "cassandra");
        assert_eq!(config.limits.max_query_points, 10_000_000);
        assert_eq!(config.rollups.node_id, "node-a");
        assert_eq!(config.telnet_listen, "0.0.0.0:4242"); // default survives
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(toml::from_str::<Config>("nope = 1").is_err());
    }
}
