//! Storage abstraction for the KairosDB Rust port, mirroring
//! `org.kairosdb.core.datastore.Datastore`.

pub mod cassandra;
pub mod cassandra_store;
pub mod memory;
pub mod parquet_store;
pub mod tiered;
pub mod wal;

use std::collections::HashMap;

use kairos_core::{ColumnSeries, DataPoint, DataPointSet, Tags};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("datastore error: {0}")]
    Datastore(String),
    #[error(transparent)]
    Core(#[from] kairos_core::Error),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Query against a single metric, the equivalent of
/// `DatastoreMetricQuery`. Tag filters with multiple values are ORed,
/// distinct tags are ANDed, matching Java semantics.
#[derive(Debug, Clone)]
pub struct DatastoreQuery {
    pub metric: String,
    pub start_time_ms: i64,
    pub end_time_ms: i64,
    pub tags: HashMap<String, Vec<String>>,
    pub limit: Option<usize>,
    /// When true, `limit` keeps the most recent points instead of the
    /// earliest. Returned points are always in ascending time order.
    pub descending: bool,
}

/// Applies `limit` to an ascending series honoring the query order: keep the
/// earliest N points ascending, or the latest N descending.
pub(crate) fn apply_limit(points: &mut Vec<DataPoint>, query: &DatastoreQuery) {
    if let Some(limit) = query.limit {
        if query.descending {
            if points.len() > limit {
                points.drain(..points.len() - limit);
            }
        } else {
            points.truncate(limit);
        }
    }
}

/// One stored series: a distinct tag set and its points in time order.
#[derive(Debug, Clone)]
pub struct SeriesData {
    pub tags: Tags,
    pub points: Vec<DataPoint>,
}

pub trait Datastore: Send + Sync {
    fn write(&self, set: DataPointSet) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Returns every distinct series matching the query. Grouping and
    /// aggregation happen above the datastore, as in Java.
    fn query(
        &self,
        query: &DatastoreQuery,
    ) -> impl std::future::Future<Output = Result<Vec<SeriesData>>> + Send;

    fn delete(&self, query: &DatastoreQuery)
        -> impl std::future::Future<Output = Result<()>> + Send;

    /// Delete with an explicit millisecond write timestamp. Cassandra
    /// tombstones default to microsecond client timestamps (as in the Java
    /// driver), which would shadow all later millisecond-stamped writes;
    /// compaction uses this variant so corrections and backfills into
    /// compacted ranges remain writable. Defaults to plain `delete`.
    fn delete_at(
        &self,
        query: &DatastoreQuery,
        _timestamp_ms: i64,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        self.delete(query)
    }

    fn metric_names(
        &self,
        prefix: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    /// Removes index entries for fully-emptied ranges (after compaction
    /// deletes the data), so emptiness checks stay cheap. Writes re-create
    /// index entries, so this is always safe. Default: no-op.
    fn purge_index(
        &self,
        _query: &DatastoreQuery,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }

    /// Columnar scan, when this store can serve the whole range as
    /// (timestamps, values) arrays — the zero-materialization Parquet path.
    /// `None` means the caller must use the row form `query`.
    fn query_columns(
        &self,
        _query: &DatastoreQuery,
    ) -> impl std::future::Future<Output = Result<Option<Vec<ColumnSeries>>>> + Send {
        async { Ok(None) }
    }

    fn tag_names(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    fn tag_values(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    // --- service key store (Java `ServiceKeyStore` / `service_index`) ---
    // Shared key/value metadata used for rollup tasks and leases. Backends
    // without shared storage keep it in memory (single-node semantics).

    fn service_set(
        &self,
        _service: &str,
        _service_key: &str,
        _key: &str,
        _value: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Err(Error::Datastore("service index not supported".into())) }
    }

    fn service_get(
        &self,
        _service: &str,
        _service_key: &str,
        _key: &str,
    ) -> impl std::future::Future<Output = Result<Option<String>>> + Send {
        async { Err(Error::Datastore("service index not supported".into())) }
    }

    fn service_list_keys(
        &self,
        _service: &str,
        _service_key: &str,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send {
        async { Err(Error::Datastore("service index not supported".into())) }
    }

    fn service_delete(
        &self,
        _service: &str,
        _service_key: &str,
        _key: &str,
    ) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Err(Error::Datastore("service index not supported".into())) }
    }
}

pub(crate) fn tags_match(tags: &Tags, filter: &HashMap<String, Vec<String>>) -> bool {
    filter.iter().all(|(key, allowed)| {
        allowed.is_empty() || tags.get(key).is_some_and(|v| allowed.iter().any(|a| a == v))
    })
}
