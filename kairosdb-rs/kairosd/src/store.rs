//! Datastore wiring: backend selection without a DI framework.

use kairos_core::DataPointSet;
use kairos_store::cassandra_store::CassandraDatastore;
use kairos_store::memory::MemoryDatastore;
use kairos_store::tiered::TieredDatastore;
use kairos_store::{Datastore, DatastoreQuery, Error, Result, SeriesData};

pub enum AnyDatastore {
    Memory(MemoryDatastore),
    Cassandra(CassandraDatastore),
    TieredMemory(TieredDatastore<MemoryDatastore>),
    TieredCassandra(TieredDatastore<CassandraDatastore>),
}

impl AnyDatastore {
    /// Moves points older than `cutoff_ms` into the Parquet cold tier.
    pub async fn compact(&self, cutoff_ms: i64) -> Result<usize> {
        match self {
            AnyDatastore::TieredMemory(s) => s.compact(cutoff_ms).await,
            AnyDatastore::TieredCassandra(s) => s.compact(cutoff_ms).await,
            _ => Err(Error::Datastore(
                "compaction requires the parquet tier (set KAIROSD_PARQUET_DIR)".into(),
            )),
        }
    }

    pub fn is_tiered(&self) -> bool {
        matches!(
            self,
            AnyDatastore::TieredMemory(_) | AnyDatastore::TieredCassandra(_)
        )
    }
}

impl Datastore for AnyDatastore {
    async fn write(&self, set: DataPointSet) -> Result<()> {
        match self {
            AnyDatastore::Memory(s) => s.write(set).await,
            AnyDatastore::Cassandra(s) => s.write(set).await,
            AnyDatastore::TieredMemory(s) => s.write(set).await,
            AnyDatastore::TieredCassandra(s) => s.write(set).await,
        }
    }

    async fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        match self {
            AnyDatastore::Memory(s) => s.query(query).await,
            AnyDatastore::Cassandra(s) => s.query(query).await,
            AnyDatastore::TieredMemory(s) => s.query(query).await,
            AnyDatastore::TieredCassandra(s) => s.query(query).await,
        }
    }

    async fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        match self {
            AnyDatastore::Memory(s) => s.delete(query).await,
            AnyDatastore::Cassandra(s) => s.delete(query).await,
            AnyDatastore::TieredMemory(s) => s.delete(query).await,
            AnyDatastore::TieredCassandra(s) => s.delete(query).await,
        }
    }

    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.metric_names(prefix).await,
            AnyDatastore::Cassandra(s) => s.metric_names(prefix).await,
            AnyDatastore::TieredMemory(s) => s.metric_names(prefix).await,
            AnyDatastore::TieredCassandra(s) => s.metric_names(prefix).await,
        }
    }

    async fn tag_names(&self) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.tag_names().await,
            AnyDatastore::Cassandra(s) => s.tag_names().await,
            AnyDatastore::TieredMemory(s) => s.tag_names().await,
            AnyDatastore::TieredCassandra(s) => s.tag_names().await,
        }
    }

    async fn tag_values(&self) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.tag_values().await,
            AnyDatastore::Cassandra(s) => s.tag_values().await,
            AnyDatastore::TieredMemory(s) => s.tag_values().await,
            AnyDatastore::TieredCassandra(s) => s.tag_values().await,
        }
    }
}
