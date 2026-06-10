//! Internal counters and the Prometheus text exposition endpoint
//! (`GET /metrics`) — the observability piece of the control plane.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

pub static DATAPOINTS_INGESTED: AtomicU64 = AtomicU64::new(0);
pub static INGEST_REQUESTS: AtomicU64 = AtomicU64::new(0);
pub static QUERIES: AtomicU64 = AtomicU64::new(0);
pub static QUERY_ERRORS: AtomicU64 = AtomicU64::new(0);
pub static QUERY_SAMPLE_POINTS: AtomicU64 = AtomicU64::new(0);
pub static QUERY_MILLIS: AtomicU64 = AtomicU64::new(0);
pub static COLUMNAR_QUERIES: AtomicU64 = AtomicU64::new(0);
pub static COMPACTIONS: AtomicU64 = AtomicU64::new(0);
pub static POINTS_COMPACTED: AtomicU64 = AtomicU64::new(0);
pub static WAL_REPLAYED_SETS: AtomicU64 = AtomicU64::new(0);

static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();

pub fn mark_start() {
    let _ = START.set(Instant::now());
}

pub fn inc(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

pub fn add(counter: &AtomicU64, n: u64) {
    counter.fetch_add(n, Ordering::Relaxed);
}

/// Prometheus text exposition format.
pub fn render() -> String {
    let uptime = START
        .get()
        .map(|s| s.elapsed().as_secs())
        .unwrap_or_default();
    let counters: &[(&str, &str, &AtomicU64)] = &[
        ("kairosd_datapoints_ingested_total", "Data points accepted into the ingest pipeline", &DATAPOINTS_INGESTED),
        ("kairosd_ingest_requests_total", "Ingest API/telnet requests accepted", &INGEST_REQUESTS),
        ("kairosd_queries_total", "Datapoint queries served", &QUERIES),
        ("kairosd_query_errors_total", "Datapoint queries that failed", &QUERY_ERRORS),
        ("kairosd_query_sample_points_total", "Raw points scanned by queries", &QUERY_SAMPLE_POINTS),
        ("kairosd_query_milliseconds_total", "Wall time spent serving queries", &QUERY_MILLIS),
        ("kairosd_columnar_queries_total", "Queries served by the columnar parquet path", &COLUMNAR_QUERIES),
        ("kairosd_compactions_total", "Compaction runs", &COMPACTIONS),
        ("kairosd_points_compacted_total", "Points moved to the parquet tier", &POINTS_COMPACTED),
        ("kairosd_wal_replayed_sets_total", "Datapoint sets replayed from the WAL at startup", &WAL_REPLAYED_SETS),
    ];
    let mut out = String::with_capacity(1024);
    for (name, help, counter) in counters {
        out.push_str(&format!(
            "# HELP {name} {help}\n# TYPE {name} counter\n{name} {}\n",
            counter.load(Ordering::Relaxed)
        ));
    }
    out.push_str(&format!(
        "# HELP kairosd_uptime_seconds Seconds since process start\n# TYPE kairosd_uptime_seconds gauge\nkairosd_uptime_seconds {uptime}\n"
    ));
    out
}
