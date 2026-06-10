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

/// One series in columnar form: parallel timestamp/value arrays, sorted by
/// timestamp. The shape Parquet scans produce and the vector kernels
/// consume; numeric-only (longs are widened to f64, as aggregators do).
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnSeries {
    pub tags: Tags,
    pub timestamps: Vec<i64>,
    pub values: Vec<f64>,
}

impl ColumnSeries {
    /// Materializes row form (values become doubles).
    pub fn to_points(&self) -> Vec<DataPoint> {
        self.timestamps
            .iter()
            .zip(&self.values)
            .map(|(ts, v)| DataPoint::new(*ts, *v))
            .collect()
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
