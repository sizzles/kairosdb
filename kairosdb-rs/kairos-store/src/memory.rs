//! In-memory datastore: the dev/test backend (replaces the Java H2 module
//! for development purposes) and the reference implementation of the
//! `Datastore` trait.

use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

use kairos_core::{DataPoint, DataPointSet, Tags, Value};

use crate::{tags_match, Datastore, DatastoreQuery, Result, SeriesData};

#[derive(Default)]
pub struct MemoryDatastore {
    // metric name -> distinct tag set -> timestamp -> value
    data: RwLock<HashMap<String, HashMap<Tags, BTreeMap<i64, Value>>>>,
}

impl MemoryDatastore {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Datastore for MemoryDatastore {
    async fn write(&self, set: DataPointSet) -> Result<()> {
        let mut data = self.data.write().expect("lock poisoned");
        let series = data
            .entry(set.name)
            .or_default()
            .entry(set.tags)
            .or_default();
        for point in set.points {
            series.insert(point.timestamp_ms, point.value);
        }
        Ok(())
    }

    async fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        let data = self.data.read().expect("lock poisoned");
        let Some(metric) = data.get(&query.metric) else {
            return Ok(Vec::new());
        };
        let mut results = Vec::new();
        for (tags, points) in metric {
            if !tags_match(tags, &query.tags) {
                continue;
            }
            let mut series_points: Vec<DataPoint> = points
                .range(query.start_time_ms..=query.end_time_ms)
                .map(|(ts, value)| DataPoint::new(*ts, value.clone()))
                .collect();
            if let Some(limit) = query.limit {
                series_points.truncate(limit);
            }
            if !series_points.is_empty() {
                results.push(SeriesData {
                    tags: tags.clone(),
                    points: series_points,
                });
            }
        }
        // Deterministic output order for tests and stable API responses.
        results.sort_by(|a, b| a.tags.cmp(&b.tags));
        Ok(results)
    }

    async fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        let mut data = self.data.write().expect("lock poisoned");
        if let Some(metric) = data.get_mut(&query.metric) {
            for (tags, points) in metric.iter_mut() {
                if tags_match(tags, &query.tags) {
                    points.retain(|ts, _| *ts < query.start_time_ms || *ts > query.end_time_ms);
                }
            }
            metric.retain(|_, points| !points.is_empty());
        }
        Ok(())
    }

    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let data = self.data.read().expect("lock poisoned");
        let mut names: Vec<String> = data
            .keys()
            .filter(|name| prefix.is_none_or(|p| name.starts_with(p)))
            .cloned()
            .collect();
        names.sort();
        Ok(names)
    }

    async fn tag_names(&self) -> Result<Vec<String>> {
        let data = self.data.read().expect("lock poisoned");
        let mut names: Vec<String> = data
            .values()
            .flat_map(|m| m.keys())
            .flat_map(|tags| tags.keys().cloned())
            .collect();
        names.sort();
        names.dedup();
        Ok(names)
    }

    async fn tag_values(&self) -> Result<Vec<String>> {
        let data = self.data.read().expect("lock poisoned");
        let mut values: Vec<String> = data
            .values()
            .flat_map(|m| m.keys())
            .flat_map(|tags| tags.values().cloned())
            .collect();
        values.sort();
        values.dedup();
        Ok(values)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn write_query_roundtrip() {
        let store = MemoryDatastore::new();
        store
            .write(
                DataPointSet::new("price.settle")
                    .tag("root", "CL")
                    .tag("contract", "2026-12")
                    .point(1_000, 62.5)
                    .point(2_000, 63.1),
            )
            .await
            .unwrap();
        store
            .write(
                DataPointSet::new("price.settle")
                    .tag("root", "NG")
                    .tag("contract", "2026-12")
                    .point(1_500, 2.9),
            )
            .await
            .unwrap();

        let all = store
            .query(&DatastoreQuery {
                metric: "price.settle".into(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                tags: HashMap::new(),
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(all.len(), 2);

        let cl_only = store
            .query(&DatastoreQuery {
                metric: "price.settle".into(),
                start_time_ms: 0,
                end_time_ms: 10_000,
                tags: HashMap::from([("root".to_string(), vec!["CL".to_string()])]),
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(cl_only.len(), 1);
        assert_eq!(cl_only[0].points.len(), 2);
    }
}
