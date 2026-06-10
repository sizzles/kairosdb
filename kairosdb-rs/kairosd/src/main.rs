//! KairosDB-rs server: wire-compatible `/api/v1` REST endpoints from
//! `org.kairosdb.core.http.rest.MetricsResource` and `RollUpResource`.
//!
//! Configuration via environment:
//! - `KAIROSD_DATASTORE`: `memory` (default) or `cassandra`
//! - `KAIROSD_CASSANDRA_NODE`: CQL contact point (default `127.0.0.1:9042`)
//! - `KAIROSD_CASSANDRA_KEYSPACE`: keyspace name (default `kairosdb`)
//! - `KAIROSD_DATA_DIR`: WAL + rollup task storage (default `./kairosd-data`,
//!   `none` disables the WAL and rollup persistence)
//! - `KAIROSD_LISTEN`: bind address (default `0.0.0.0:8080`)
//! - `KAIROSD_TELNET_LISTEN`: telnet bind address (default `0.0.0.0:4242`,
//!   `none` disables)
//! - `KAIROSD_QUERY_MODE`: `compat` (default, bit-identical to Java) or
//!   `fast` (vectorized sum/avg/dev kernels)
//! - `KAIROSD_PARQUET_DIR`: enables the Parquet cold tier at this path
//! - `KAIROSD_COMPACT_OLDER_THAN_MS`: auto-compact points older than this
//!   every hour (requires the parquet tier)

mod api;
mod features;
mod metrics;
mod ingest;
mod rollup;
mod store;
mod telnet;

use std::sync::Arc;

use kairos_store::cassandra_store::{CassandraConfig, CassandraDatastore};
use kairos_store::memory::MemoryDatastore;
use kairos_store::parquet_store::ParquetStore;
use kairos_store::tiered::TieredDatastore;
use kairos_store::wal::Wal;

use api::AppState;
use ingest::Ingest;
use rollup::RollupManager;
use store::AnyDatastore;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();
    metrics::mark_start();

    let backend = std::env::var("KAIROSD_DATASTORE").unwrap_or_else(|_| "memory".to_string());
    let parquet = std::env::var("KAIROSD_PARQUET_DIR").ok().map(|dir| {
        Arc::new(ParquetStore::open(&dir).unwrap_or_else(|e| panic!("parquet open failed: {e}")))
    });
    let store = match backend.as_str() {
        "memory" => {
            let hot = MemoryDatastore::new();
            match &parquet {
                Some(cold) => AnyDatastore::TieredMemory(TieredDatastore::new(hot, cold.clone())),
                None => AnyDatastore::Memory(hot),
            }
        }
        "cassandra" => {
            let mut config = CassandraConfig::default();
            if let Ok(node) = std::env::var("KAIROSD_CASSANDRA_NODE") {
                config.node = node;
            }
            if let Ok(keyspace) = std::env::var("KAIROSD_CASSANDRA_KEYSPACE") {
                config.keyspace = keyspace;
            }
            tracing::info!("connecting to cassandra at {}", config.node);
            let hot = CassandraDatastore::connect(&config)
                .await
                .unwrap_or_else(|e| panic!("cassandra connect failed: {e}"));
            match &parquet {
                Some(cold) => {
                    AnyDatastore::TieredCassandra(TieredDatastore::new(hot, cold.clone()))
                }
                None => AnyDatastore::Cassandra(hot),
            }
        }
        other => panic!("unknown KAIROSD_DATASTORE: {other}"),
    };
    tracing::info!(
        "datastore backend: {backend}{}",
        if parquet.is_some() { " + parquet cold tier" } else { "" }
    );
    let store = Arc::new(store);

    if let Ok(older_than) = std::env::var("KAIROSD_COMPACT_OLDER_THAN_MS") {
        let older_than: i64 = older_than.parse().expect("KAIROSD_COMPACT_OLDER_THAN_MS: ms");
        assert!(store.is_tiered(), "auto-compaction requires KAIROSD_PARQUET_DIR");
        let compact_store = store.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(3600));
            loop {
                tick.tick().await;
                let cutoff = chrono::Utc::now().timestamp_millis() - older_than;
                match compact_store.compact(cutoff).await {
                    Ok(moved) if moved > 0 => tracing::info!("compacted {moved} points to parquet"),
                    Ok(_) => {}
                    Err(e) => tracing::error!("compaction failed: {e}"),
                }
            }
        });
    }

    let data_dir = std::env::var("KAIROSD_DATA_DIR").unwrap_or_else(|_| "kairosd-data".to_string());
    let (wal, rollup_file) = if data_dir == "none" {
        tracing::warn!("KAIROSD_DATA_DIR=none: WAL durability and rollup persistence disabled");
        (None, None)
    } else {
        let dir = std::path::PathBuf::from(&data_dir);
        let wal = Wal::open(dir.join("wal")).unwrap_or_else(|e| panic!("wal open failed: {e}"));
        (Some(Arc::new(wal)), Some(dir.join("rollups.json")))
    };

    let fast_math = match std::env::var("KAIROSD_QUERY_MODE").as_deref() {
        Ok("fast") => true,
        Ok("compat") | Err(_) => false,
        Ok(other) => panic!("unknown KAIROSD_QUERY_MODE: {other}"),
    };
    if fast_math {
        tracing::info!("query mode: fast (vectorized sum/avg/dev; last-ulp divergence from Java)");
    }

    let ingest = Ingest::start(wal, store.clone())
        .await
        .unwrap_or_else(|e| panic!("ingest start (wal replay) failed: {e}"));
    let rollups = RollupManager::start(store.clone(), ingest.clone(), rollup_file, fast_math);

    let telnet_addr =
        std::env::var("KAIROSD_TELNET_LISTEN").unwrap_or_else(|_| "0.0.0.0:4242".to_string());
    if telnet_addr != "none" {
        let telnet_listener = tokio::net::TcpListener::bind(&telnet_addr)
            .await
            .unwrap_or_else(|e| panic!("cannot bind telnet {telnet_addr}: {e}"));
        tracing::info!("telnet listening on {telnet_addr}");
        tokio::spawn(telnet::serve(telnet_listener, ingest.clone()));
    }

    let app = api::router(AppState { store, ingest, rollups, fast_math });

    let addr = std::env::var("KAIROSD_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {addr}: {e}"));
    tracing::info!("kairosd listening on {addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await
        .expect("server failed");
    tracing::info!("shutdown complete");
}

/// SIGINT/SIGTERM stop accepting connections; the WAL fsync loop has
/// already made queued ingest durable, so replay covers the rest.
async fn shutdown_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! {
            _ = ctrl_c => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    ctrl_c.await.expect("install ctrl-c handler");
    tracing::info!("shutdown signal received");
}
