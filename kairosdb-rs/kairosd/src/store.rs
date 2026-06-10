//! Datastore wiring: backend selection without a DI framework.

use kairos_core::DataPointSet;
use kairos_store::cassandra_store::CassandraDatastore;
use kairos_store::memory::MemoryDatastore;
use kairos_store::{Datastore, DatastoreQuery, Result, SeriesData};

pub enum AnyDatastore {
    Memory(MemoryDatastore),
    Cassandra(CassandraDatastore),
}

impl Datastore for AnyDatastore {
    async fn write(&self, set: DataPointSet) -> Result<()> {
        match self {
            AnyDatastore::Memory(s) => s.write(set).await,
            AnyDatastore::Cassandra(s) => s.write(set).await,
        }
    }

    async fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        match self {
            AnyDatastore::Memory(s) => s.query(query).await,
            AnyDatastore::Cassandra(s) => s.query(query).await,
        }
    }

    async fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        match self {
            AnyDatastore::Memory(s) => s.delete(query).await,
            AnyDatastore::Cassandra(s) => s.delete(query).await,
        }
    }

    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.metric_names(prefix).await,
            AnyDatastore::Cassandra(s) => s.metric_names(prefix).await,
        }
    }

    async fn tag_names(&self) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.tag_names().await,
            AnyDatastore::Cassandra(s) => s.tag_names().await,
        }
    }

    async fn tag_values(&self) -> Result<Vec<String>> {
        match self {
            AnyDatastore::Memory(s) => s.tag_values().await,
            AnyDatastore::Cassandra(s) => s.tag_values().await,
        }
    }
}
