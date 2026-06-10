//! Storage abstraction for the KairosDB Rust port, mirroring
//! `org.kairosdb.core.datastore.Datastore`.

pub mod cassandra;
pub mod cassandra_store;
pub mod memory;

use std::collections::HashMap;

use kairos_core::{DataPoint, DataPointSet, Tags};

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

    fn metric_names(
        &self,
        prefix: Option<&str>,
    ) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    fn tag_names(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;

    fn tag_values(&self) -> impl std::future::Future<Output = Result<Vec<String>>> + Send;
}

pub(crate) fn tags_match(tags: &Tags, filter: &HashMap<String, Vec<String>>) -> bool {
    filter.iter().all(|(key, allowed)| {
        allowed.is_empty() || tags.get(key).is_some_and(|v| allowed.iter().any(|a| a == v))
    })
}
