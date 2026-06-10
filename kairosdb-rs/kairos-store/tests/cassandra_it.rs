//! Integration test against a live Cassandra/Scylla node.
//!
//! Skipped unless `KAIROS_TEST_CASSANDRA` is set to a contact point, e.g.:
//! `KAIROS_TEST_CASSANDRA=127.0.0.1:9042 cargo test -p kairos-store --test cassandra_it`

use std::collections::HashMap;

use kairos_core::{DataPointSet, Value};
use kairos_store::cassandra_store::{CassandraConfig, CassandraDatastore};
use kairos_store::{Datastore, DatastoreQuery};

fn test_node() -> Option<String> {
    std::env::var("KAIROS_TEST_CASSANDRA").ok()
}

#[tokio::test]
async fn write_query_delete_roundtrip() {
    let Some(node) = test_node() else {
        eprintln!("KAIROS_TEST_CASSANDRA not set; skipping");
        return;
    };
    let store = CassandraDatastore::connect(&CassandraConfig {
        node,
        keyspace: "kairosdb_rs_it".to_string(),
        ..CassandraConfig::default()
    })
    .await
    .expect("connect");

    // Timestamps straddle a 3-week row boundary to exercise multi-row reads.
    let row_width = 1_814_400_000i64;
    let t0 = row_width - 1_000;
    let t1 = row_width + 1_000;

    store
        .write(
            DataPointSet::new("it.price.settle")
                .tag("root", "CL")
                .tag("contract", "2026-12")
                .point(t0, 61.5)
                .point(t1, 62.5)
                .point(t1 + 1_000, 63i64),
        )
        .await
        .expect("write");
    store
        .write(
            DataPointSet::new("it.price.settle")
                .tag("root", "NG")
                .tag("contract", "2026-12")
                .point(t0, 2.9),
        )
        .await
        .expect("write");

    let all = store
        .query(&DatastoreQuery {
            metric: "it.price.settle".to_string(),
            start_time_ms: 0,
            end_time_ms: t1 + 10_000,
            tags: HashMap::new(),
            limit: None,
        })
        .await
        .expect("query");
    assert_eq!(all.len(), 2, "expected two series, got {all:?}");

    let cl = all
        .iter()
        .find(|s| s.tags["root"] == "CL")
        .expect("CL series");
    assert_eq!(cl.points.len(), 3);
    assert_eq!(cl.points[0].value, Value::Double(61.5));
    assert_eq!(cl.points[0].timestamp_ms, t0);
    // Long and double points for one series live in separate row keys
    // (distinct data_type) but merge into one ordered series.
    assert_eq!(cl.points[2].value, Value::Long(63));

    let names = store.metric_names(Some("it.")).await.expect("names");
    assert!(names.contains(&"it.price.settle".to_string()));
    assert!(store
        .tag_values()
        .await
        .expect("tag values")
        .contains(&"2026-12".to_string()));

    // Time-bounded query only sees the second row window.
    let bounded = store
        .query(&DatastoreQuery {
            metric: "it.price.settle".to_string(),
            start_time_ms: row_width,
            end_time_ms: t1 + 10_000,
            tags: HashMap::from([("root".to_string(), vec!["CL".to_string()])]),
            limit: None,
        })
        .await
        .expect("bounded query");
    assert_eq!(bounded.len(), 1);
    assert_eq!(bounded[0].points.len(), 2);

    // Delete the CL series and confirm it is gone.
    store
        .delete(&DatastoreQuery {
            metric: "it.price.settle".to_string(),
            start_time_ms: 0,
            end_time_ms: t1 + 10_000,
            tags: HashMap::from([("root".to_string(), vec!["CL".to_string()])]),
            limit: None,
        })
        .await
        .expect("delete");
    let after = store
        .query(&DatastoreQuery {
            metric: "it.price.settle".to_string(),
            start_time_ms: 0,
            end_time_ms: t1 + 10_000,
            tags: HashMap::from([("root".to_string(), vec!["CL".to_string()])]),
            limit: None,
        })
        .await
        .expect("query after delete");
    assert!(after.is_empty(), "expected empty after delete, got {after:?}");
}
