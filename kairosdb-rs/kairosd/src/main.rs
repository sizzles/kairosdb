//! KairosDB-rs server: wire-compatible `/api/v1` REST endpoints from
//! `org.kairosdb.core.http.rest.MetricsResource`.
//!
//! Backend selection via environment:
//! - `KAIROSD_DATASTORE`: `memory` (default) or `cassandra`
//! - `KAIROSD_CASSANDRA_NODE`: CQL contact point (default `127.0.0.1:9042`)
//! - `KAIROSD_CASSANDRA_KEYSPACE`: keyspace name (default `kairosdb`)
//! - `KAIROSD_LISTEN`: bind address (default `0.0.0.0:8080`)

mod api;
mod store;

use std::sync::Arc;

use kairos_store::cassandra_store::{CassandraConfig, CassandraDatastore};
use kairos_store::memory::MemoryDatastore;
use store::AnyDatastore;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let backend = std::env::var("KAIROSD_DATASTORE").unwrap_or_else(|_| "memory".to_string());
    let store = match backend.as_str() {
        "memory" => AnyDatastore::Memory(MemoryDatastore::new()),
        "cassandra" => {
            let mut config = CassandraConfig::default();
            if let Ok(node) = std::env::var("KAIROSD_CASSANDRA_NODE") {
                config.node = node;
            }
            if let Ok(keyspace) = std::env::var("KAIROSD_CASSANDRA_KEYSPACE") {
                config.keyspace = keyspace;
            }
            tracing::info!("connecting to cassandra at {}", config.node);
            let datastore = CassandraDatastore::connect(&config)
                .await
                .unwrap_or_else(|e| panic!("cassandra connect failed: {e}"));
            AnyDatastore::Cassandra(datastore)
        }
        other => panic!("unknown KAIROSD_DATASTORE: {other}"),
    };
    tracing::info!("datastore backend: {backend}");

    let app = api::router(Arc::new(store));

    let addr = std::env::var("KAIROSD_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {addr}: {e}"));
    tracing::info!("kairosd listening on {addr}");
    axum::serve(listener, app).await.expect("server failed");
}
