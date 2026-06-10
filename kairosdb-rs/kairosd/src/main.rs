//! KairosDB-rs server: wire-compatible `/api/v1` REST endpoints from
//! `org.kairosdb.core.http.rest.MetricsResource` and `RollUpResource`.
//!
//! Configuration: a TOML file via `--config <path>` or `KAIROSD_CONFIG`,
//! with `KAIROSD_*` environment variables overriding individual settings
//! (see `config.rs` for the full schema and variable names).

mod api;
mod config;
mod features;
mod guard;
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

    let cfg = config::Config::load();
    let parquet = cfg.parquet.dir.as_ref().map(|dir| {
        Arc::new(ParquetStore::open(dir).unwrap_or_else(|e| panic!("parquet open failed: {e}")))
    });
    let store = match cfg.datastore.backend.as_str() {
        "memory" => {
            let hot = MemoryDatastore::new();
            match &parquet {
                Some(cold) => AnyDatastore::TieredMemory(TieredDatastore::new(hot, cold.clone())),
                None => AnyDatastore::Memory(hot),
            }
        }
        "cassandra" => {
            let cassandra = CassandraConfig {
                node: cfg.datastore.cassandra_node.clone(),
                keyspace: cfg.datastore.cassandra_keyspace.clone(),
                ..CassandraConfig::default()
            };
            tracing::info!("connecting to cassandra at {}", cassandra.node);
            let hot = CassandraDatastore::connect(&cassandra)
                .await
                .unwrap_or_else(|e| panic!("cassandra connect failed: {e}"));
            match &parquet {
                Some(cold) => {
                    AnyDatastore::TieredCassandra(TieredDatastore::new(hot, cold.clone()))
                }
                None => AnyDatastore::Cassandra(hot),
            }
        }
        other => panic!("unknown datastore backend: {other}"),
    };
    tracing::info!(
        "datastore backend: {}{}, node id: {}",
        cfg.datastore.backend,
        if parquet.is_some() { " + parquet cold tier" } else { "" },
        cfg.rollups.node_id,
    );
    let store = Arc::new(store);

    if cfg.parquet.compact_older_than_ms > 0 {
        let older_than = cfg.parquet.compact_older_than_ms;
        assert!(store.is_tiered(), "auto-compaction requires parquet.dir");
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

    let data_dir = cfg.data_dir.clone();
    let (wal, rollup_file) = if data_dir == "none" {
        tracing::warn!("data_dir=none: WAL durability disabled");
        (None, None)
    } else {
        let dir = std::path::PathBuf::from(&data_dir);
        let wal = Wal::open(dir.join("wal")).unwrap_or_else(|e| panic!("wal open failed: {e}"));
        (Some(Arc::new(wal)), Some(dir.join("rollups.json")))
    };

    let fast_math = match cfg.query_mode.as_str() {
        "fast" => true,
        "compat" => false,
        other => panic!("unknown query_mode: {other}"),
    };
    if fast_math {
        tracing::info!("query mode: fast (vectorized sum/avg/dev; last-ulp divergence from Java)");
    }

    let ingest = Ingest::start(wal, store.clone())
        .await
        .unwrap_or_else(|e| panic!("ingest start (wal replay) failed: {e}"));
    let rollups = RollupManager::start(
        store.clone(),
        ingest.clone(),
        rollup_file,
        fast_math,
        cfg.rollups.node_id.clone(),
        cfg.rollups.refresh_seconds,
    )
    .await;

    let telnet_addr = cfg.telnet_listen.clone();
    if telnet_addr != "none" {
        let telnet_listener = tokio::net::TcpListener::bind(&telnet_addr)
            .await
            .unwrap_or_else(|e| panic!("cannot bind telnet {telnet_addr}: {e}"));
        tracing::info!("telnet listening on {telnet_addr}");
        tokio::spawn(telnet::serve(telnet_listener, ingest.clone()));
    }

    let guard = Arc::new(guard::QueryGuard::new(&cfg.limits));
    let app = api::router(AppState { store, ingest, rollups, guard, fast_math });

    let addr = cfg.listen.clone();
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
