//! Tiered datastore: a hot store (memory or Cassandra) in front of the
//! Parquet cold tier. Writes land in the hot store; `compact` moves closed
//! time windows into Parquet; queries merge both tiers (hot wins on
//! timestamp collisions, since corrections land there).

use std::collections::BTreeMap;
use std::sync::Arc;

use kairos_core::{DataPoint, DataPointSet, Tags, Value};

use crate::parquet_store::ParquetStore;
use crate::{Datastore, DatastoreQuery, Result, SeriesData};

pub struct TieredDatastore<H: Datastore> {
    hot: H,
    cold: Arc<ParquetStore>,
}

impl<H: Datastore> TieredDatastore<H> {
    pub fn new(hot: H, cold: Arc<ParquetStore>) -> Self {
        TieredDatastore { hot, cold }
    }

    pub fn hot(&self) -> &H {
        &self.hot
    }

    pub fn cold(&self) -> &Arc<ParquetStore> {
        &self.cold
    }

    /// Moves all points older than `cutoff_ms` from the hot store into
    /// Parquet partitions (merging with any existing partition data), then
    /// deletes them from the hot store. Returns the number of points moved.
    pub async fn compact(&self, cutoff_ms: i64) -> Result<usize> {
        let mut moved = 0usize;
        for metric in self.hot.metric_names(None).await? {
            let query = DatastoreQuery {
                metric: metric.clone(),
                start_time_ms: 0,
                end_time_ms: cutoff_ms - 1,
                tags: Default::default(),
                limit: None,
                descending: false,
            };
            let series = self.hot.query(&query).await?;
            if series.is_empty() {
                continue;
            }

            // Split per partition window; non-numeric points stay hot.
            let mut windows: BTreeMap<i64, BTreeMap<Tags, Vec<DataPoint>>> = BTreeMap::new();
            for s in &series {
                for p in &s.points {
                    if !matches!(p.value, Value::Long(_) | Value::Double(_)) {
                        continue;
                    }
                    windows
                        .entry(self.cold.window_start(p.timestamp_ms))
                        .or_default()
                        .entry(s.tags.clone())
                        .or_default()
                        .push(p.clone());
                }
            }

            for (window, by_tags) in windows {
                // Merge with whatever the partition already holds.
                let mut merged: BTreeMap<Tags, BTreeMap<i64, Value>> = BTreeMap::new();
                for existing in self.cold.read_partition(&metric, window)? {
                    let slot = merged.entry(existing.tags).or_default();
                    for p in existing.points {
                        slot.insert(p.timestamp_ms, p.value);
                    }
                }
                for (tags, points) in by_tags {
                    let slot = merged.entry(tags).or_default();
                    for p in points {
                        moved += 1;
                        slot.insert(p.timestamp_ms, p.value);
                    }
                }
                let partition: Vec<SeriesData> = merged
                    .into_iter()
                    .map(|(tags, points)| SeriesData {
                        tags,
                        points: points
                            .into_iter()
                            .map(|(ts, value)| DataPoint { timestamp_ms: ts, value })
                            .collect(),
                    })
                    .collect();
                self.cold.write_partition(&metric, window, &partition)?;
            }

            // Millisecond-stamped tombstones keep compacted ranges
            // writable for corrections (see Datastore::delete_at).
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock before epoch")
                .as_millis() as i64;
            self.hot.delete_at(&query, now_ms).await?;
            // Drop orphaned index entries for fully-compacted windows so
            // the hot-emptiness check on reads stays cheap.
            let mut purge = query.clone();
            purge.end_time_ms = self
                .cold
                .window_start(cutoff_ms.saturating_sub(1))
                .saturating_sub(1);
            if purge.end_time_ms > purge.start_time_ms {
                self.hot.purge_index(&purge).await?;
            }
        }
        Ok(moved)
    }
}

fn merge_series(cold: Vec<SeriesData>, hot: Vec<SeriesData>) -> Vec<SeriesData> {
    // The common steady states — everything compacted, or nothing yet —
    // need no merging at all.
    if hot.is_empty() {
        return cold;
    }
    if cold.is_empty() {
        return hot;
    }
    let mut merged: BTreeMap<Tags, BTreeMap<i64, Value>> = BTreeMap::new();
    // Cold first so hot overwrites on collision.
    for tier in [cold, hot] {
        for s in tier {
            let slot = merged.entry(s.tags).or_default();
            for p in s.points {
                slot.insert(p.timestamp_ms, p.value);
            }
        }
    }
    merged
        .into_iter()
        .map(|(tags, points)| SeriesData {
            tags,
            points: points
                .into_iter()
                .map(|(ts, value)| DataPoint { timestamp_ms: ts, value })
                .collect(),
        })
        .collect()
}

impl<H: Datastore> Datastore for TieredDatastore<H> {
    async fn write(&self, set: DataPointSet) -> Result<()> {
        self.hot.write(set).await
    }

    async fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        let cold = self.cold.query(query)?;
        let hot = self.hot.query(query).await?;
        let mut merged = merge_series(cold, hot);
        for s in &mut merged {
            crate::apply_limit(&mut s.points, query);
        }
        merged.retain(|s| !s.points.is_empty());
        Ok(merged)
    }

    async fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        self.cold.delete(query)?;
        self.hot.delete(query).await
    }

    /// Columnar fast path: only when the hot tier has nothing in range, so
    /// the result comes purely from Parquet (otherwise tier merging needs
    /// rows and the caller falls back).
    async fn query_columns(
        &self,
        query: &DatastoreQuery,
    ) -> Result<Option<Vec<kairos_core::ColumnSeries>>> {
        if !self.hot.query(query).await?.is_empty() {
            return Ok(None);
        }
        let cols = self.cold.scan_columns(query)?;
        if cols.is_empty() {
            return Ok(None);
        }
        Ok(Some(cols))
    }

    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>> {
        let mut names = self.hot.metric_names(prefix).await?;
        for name in self.cold.metric_names()? {
            if prefix.is_none_or(|p| name.starts_with(p)) && !names.contains(&name) {
                names.push(name);
            }
        }
        names.sort();
        Ok(names)
    }

    async fn tag_names(&self) -> Result<Vec<String>> {
        self.hot.tag_names().await
    }

    async fn tag_values(&self) -> Result<Vec<String>> {
        self.hot.tag_values().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryDatastore;
    use std::collections::HashMap;

    fn tiered() -> (TieredDatastore<MemoryDatastore>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "kairos-tiered-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let cold = Arc::new(ParquetStore::open(&dir).unwrap());
        (TieredDatastore::new(MemoryDatastore::new(), cold), dir)
    }

    fn q(metric: &str, start: i64, end: i64) -> DatastoreQuery {
        DatastoreQuery {
            metric: metric.to_string(),
            start_time_ms: start,
            end_time_ms: end,
            tags: HashMap::new(),
            limit: None,
            descending: false,
        }
    }

    #[tokio::test]
    async fn compact_moves_old_points_and_queries_merge_tiers() {
        let (store, dir) = tiered();
        store
            .write(
                DataPointSet::new("m")
                    .tag("h", "a")
                    .point(1_000, 1.0)
                    .point(2_000, 2.0)
                    .point(900_000_000, 3.0), // stays hot
            )
            .await
            .unwrap();

        let moved = store.compact(500_000_000).await.unwrap();
        assert_eq!(moved, 2);

        // Old points now come from parquet only.
        assert_eq!(store.cold().query(&q("m", 0, 10_000)).unwrap()[0].points.len(), 2);
        assert!(store.hot().query(&q("m", 0, 10_000)).await.unwrap().is_empty());

        // A spanning query merges both tiers seamlessly.
        let all = store.query(&q("m", 0, 1_000_000_000)).await.unwrap();
        assert_eq!(all.len(), 1);
        let values: Vec<i64> = all[0].points.iter().map(|p| p.timestamp_ms).collect();
        assert_eq!(values, vec![1_000, 2_000, 900_000_000]);

        // Compacting again is a no-op.
        assert_eq!(store.compact(500_000_000).await.unwrap(), 0);

        // Re-compaction merges new old data into the existing partition.
        store
            .write(DataPointSet::new("m").tag("h", "a").point(1_500, 9.0))
            .await
            .unwrap();
        assert_eq!(store.compact(500_000_000).await.unwrap(), 1);
        let cold = store.cold().query(&q("m", 0, 10_000)).unwrap();
        assert_eq!(cold[0].points.len(), 3);

        assert_eq!(store.metric_names(None).await.unwrap(), vec!["m"]);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn hot_overwrites_cold_on_collision_and_delete_spans_tiers() {
        let (store, dir) = tiered();
        store
            .write(DataPointSet::new("m").tag("h", "a").point(1_000, 1.0))
            .await
            .unwrap();
        store.compact(i64::MAX).await.unwrap();
        // A correction arrives for an already-compacted timestamp.
        store
            .write(DataPointSet::new("m").tag("h", "a").point(1_000, 99.0))
            .await
            .unwrap();
        let merged = store.query(&q("m", 0, 10_000)).await.unwrap();
        assert_eq!(merged[0].points[0].value, Value::Double(99.0));

        store.delete(&q("m", 0, 10_000)).await.unwrap();
        assert!(store.query(&q("m", 0, 10_000)).await.unwrap().is_empty());
        let _ = std::fs::remove_dir_all(dir);
    }
}
