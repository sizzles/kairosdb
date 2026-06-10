//! KairosDB-rs server: wire-compatible `/api/v1` REST endpoints from
//! `org.kairosdb.core.http.rest.MetricsResource`, currently backed by the
//! in-memory datastore.

mod api;

use std::sync::Arc;

use kairos_store::memory::MemoryDatastore;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let store = Arc::new(MemoryDatastore::new());
    let app = api::router(store);

    let addr = std::env::var("KAIROSD_LISTEN").unwrap_or_else(|_| "0.0.0.0:8080".to_string());
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .unwrap_or_else(|e| panic!("cannot bind {addr}: {e}"));
    tracing::info!("kairosd listening on {addr}");
    axum::serve(listener, app).await.expect("server failed");
}
