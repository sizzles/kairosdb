//! Data points and metric containers, mirroring `org.kairosdb.core.DataPoint`
//! and `DataPointSet`.

use std::collections::BTreeMap;

use crate::value::Value;

/// Tags are kept sorted, like the Java `ImmutableSortedMap` — the Cassandra
/// row-key tag string depends on sorted iteration order.
pub type Tags = BTreeMap<String, String>;

#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub timestamp_ms: i64,
    pub value: Value,
}

impl DataPoint {
    pub fn new(timestamp_ms: i64, value: impl Into<Value>) -> Self {
        DataPoint {
            timestamp_ms,
            value: value.into(),
        }
    }
}

/// A named series of points sharing one tag set — the unit of ingestion.
#[derive(Debug, Clone, PartialEq)]
pub struct DataPointSet {
    pub name: String,
    pub tags: Tags,
    pub points: Vec<DataPoint>,
    /// Time-to-live in seconds; 0 means never expire (Java default).
    pub ttl: u32,
}

impl DataPointSet {
    pub fn new(name: impl Into<String>) -> Self {
        DataPointSet {
            name: name.into(),
            tags: Tags::new(),
            points: Vec::new(),
            ttl: 0,
        }
    }

    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    pub fn point(mut self, timestamp_ms: i64, value: impl Into<Value>) -> Self {
        self.points.push(DataPoint::new(timestamp_ms, value));
        self
    }
}
