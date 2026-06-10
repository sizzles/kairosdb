//! Parquet cold tier: immutable, time-partitioned columnar storage for
//! historical data — the analytical layer from the port proposal.
//!
//! Layout: `<dir>/<urlencoded metric>/<window_start_ms>.parquet`, one file
//! per (metric, time window). Rows are sorted by (series, timestamp) and
//! carry the canonical tag string, the timestamp, and one of two nullable
//! value columns (long/double), so numeric type round-trips exactly. Row
//! groups carry min/max statistics; scans prune files by window and row
//! groups by timestamp range. Text/custom values stay in the hot store.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::{
    Array, DictionaryArray, Float64Array, Int64Array, RecordBatch, StringArray,
};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use parquet::arrow::arrow_reader::{ArrowReaderOptions, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use kairos_core::{DataPoint, Tags, Value};

use crate::{tags_match, DatastoreQuery, Error, Result, SeriesData};

/// Window width for partition files; matches the Cassandra row width so a
/// compacted window maps to whole hot-store rows.
pub const DEFAULT_WINDOW_MS: i64 = 1_814_400_000;

pub struct ParquetStore {
    dir: PathBuf,
    window_ms: i64,
}

fn pq_err(context: &str, e: impl std::fmt::Display) -> Error {
    Error::Datastore(format!("parquet {context}: {e}"))
}

/// Canonical tag-string form (the row-key tag format) used as the series
/// identifier column.
fn tag_string(tags: &Tags) -> String {
    let mut out = String::new();
    for (k, v) in tags {
        out.push_str(k);
        out.push('=');
        out.push_str(v);
        out.push(':');
    }
    out
}

fn parse_tag_string(s: &str) -> Tags {
    let mut tags = Tags::new();
    for pair in s.split(':').filter(|p| !p.is_empty()) {
        if let Some((k, v)) = pair.split_once('=') {
            tags.insert(k.to_string(), v.to_string());
        }
    }
    tags
}

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(
            "series",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        ),
        Field::new("ts", DataType::Int64, false),
        Field::new("long_value", DataType::Int64, true),
        Field::new("double_value", DataType::Float64, true),
    ]))
}

/// Typed views over one record batch's columns.
struct BatchColumns<'a> {
    series: &'a DictionaryArray<Int32Type>,
    series_values: &'a StringArray,
    ts: &'a Int64Array,
    longs: &'a Int64Array,
    doubles: &'a Float64Array,
}

impl<'a> BatchColumns<'a> {
    fn new(batch: &'a RecordBatch) -> Result<Self> {
        let series = batch
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>()
            .ok_or_else(|| pq_err("series column", "not a dictionary"))?;
        Ok(BatchColumns {
            series,
            series_values: series
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| pq_err("series column", "not utf8"))?,
            ts: batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| pq_err("ts column", "not int64"))?,
            longs: batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| pq_err("long column", "not int64"))?,
            doubles: batch
                .column(3)
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| pq_err("double column", "not float64"))?,
        })
    }

}

/// Row groups whose ts statistics overlap [start, end].
fn prune_row_groups<T: parquet::file::reader::ChunkReader>(
    builder: &ParquetRecordBatchReaderBuilder<T>,
    start_ms: i64,
    end_ms: i64,
) -> Vec<usize> {
    builder
        .metadata()
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, rg)| {
            let Some(stats) = rg.column(1).statistics() else { return true };
            let min = stats
                .min_bytes_opt()
                .map(|b| i64::from_le_bytes(b.try_into().unwrap_or([0; 8])));
            let max = stats
                .max_bytes_opt()
                .map(|b| i64::from_le_bytes(b.try_into().unwrap_or([0; 8])));
            match (min, max) {
                (Some(min), Some(max)) => max >= start_ms && min <= end_ms,
                _ => true,
            }
        })
        .map(|(i, _)| i)
        .collect()
}

impl ParquetStore {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| pq_err("create dir", e))?;
        Ok(ParquetStore {
            dir,
            window_ms: DEFAULT_WINDOW_MS,
        })
    }

    fn metric_dir(&self, metric: &str) -> PathBuf {
        // Percent-encode path separators and dots conservatively.
        let safe: String = metric
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                    c.to_string()
                } else {
                    format!("%{:02X}", c as u32)
                }
            })
            .collect();
        self.dir.join(safe)
    }

    pub fn window_start(&self, ts_ms: i64) -> i64 {
        ts_ms - ts_ms.rem_euclid(self.window_ms)
    }

    fn partition_path(&self, metric: &str, window_start: i64) -> PathBuf {
        self.metric_dir(metric).join(format!("{window_start}.parquet"))
    }

    /// Writes (or replaces) one partition from per-series points. Series are
    /// written contiguously, sorted by timestamp, so readers reconstruct
    /// them with a single pass.
    pub fn write_partition(
        &self,
        metric: &str,
        window_start: i64,
        series: &[SeriesData],
    ) -> Result<()> {
        let mut series_col: Vec<String> = Vec::new();
        let mut ts_col: Vec<i64> = Vec::new();
        let mut long_col: Vec<Option<i64>> = Vec::new();
        let mut double_col: Vec<Option<f64>> = Vec::new();

        let mut sorted: Vec<&SeriesData> = series.iter().collect();
        sorted.sort_by_key(|s| tag_string(&s.tags));
        for s in sorted {
            let key = tag_string(&s.tags);
            for p in &s.points {
                match p.value {
                    Value::Long(v) => {
                        long_col.push(Some(v));
                        double_col.push(None);
                    }
                    Value::Double(v) => {
                        long_col.push(None);
                        double_col.push(Some(v));
                    }
                    // The cold tier is numeric-only by design.
                    _ => continue,
                }
                series_col.push(key.clone());
                ts_col.push(p.timestamp_ms);
            }
        }
        if ts_col.is_empty() {
            return Ok(());
        }

        let dict: DictionaryArray<Int32Type> =
            series_col.iter().map(String::as_str).collect();
        let batch = RecordBatch::try_new(
            schema(),
            vec![
                Arc::new(dict),
                Arc::new(Int64Array::from(ts_col)),
                Arc::new(Int64Array::from(long_col)),
                Arc::new(Float64Array::from(double_col)),
            ],
        )
        .map_err(|e| pq_err("build batch", e))?;

        fs::create_dir_all(self.metric_dir(metric)).map_err(|e| pq_err("create metric dir", e))?;
        let path = self.partition_path(metric, window_start);
        let tmp = path.with_extension("parquet.tmp");
        let file = fs::File::create(&tmp).map_err(|e| pq_err("create file", e))?;
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(64 * 1024)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema(), Some(props))
            .map_err(|e| pq_err("open writer", e))?;
        writer.write(&batch).map_err(|e| pq_err("write", e))?;
        writer.close().map_err(|e| pq_err("close", e))?;
        fs::rename(&tmp, &path).map_err(|e| pq_err("rename", e))?;
        Ok(())
    }

    /// Reads one whole partition back as per-series data (used by
    /// compaction merges and partial deletes).
    pub fn read_partition(&self, metric: &str, window_start: i64) -> Result<Vec<SeriesData>> {
        let path = self.partition_path(metric, window_start);
        if !path.exists() {
            return Ok(Vec::new());
        }
        self.scan_file(&path, i64::MIN, i64::MAX, &Default::default())
    }

    /// Partition window starts overlapping a time range, oldest first.
    pub fn partitions(&self, metric: &str, start_ms: i64, end_ms: i64) -> Result<Vec<i64>> {
        let dir = self.metric_dir(metric);
        if !dir.exists() {
            return Ok(Vec::new());
        }
        let mut windows = Vec::new();
        for entry in fs::read_dir(&dir).map_err(|e| pq_err("read metric dir", e))? {
            let name = entry.map_err(|e| pq_err("dir entry", e))?.file_name();
            let Some(window) = name
                .to_string_lossy()
                .strip_suffix(".parquet")
                .and_then(|s| s.parse::<i64>().ok())
            else {
                continue;
            };
            if window <= end_ms && window + self.window_ms > start_ms {
                windows.push(window);
            }
        }
        windows.sort_unstable();
        Ok(windows)
    }

    /// Time-range scan with tag filtering: prunes partitions by window and
    /// row groups by timestamp statistics. Windows are disjoint in time and
    /// visited oldest-first, so per-series points append already sorted —
    /// no per-point map insertion.
    pub fn query(&self, query: &DatastoreQuery) -> Result<Vec<SeriesData>> {
        let mut merged: BTreeMap<Tags, Vec<DataPoint>> = BTreeMap::new();
        for window in self.partitions(&query.metric, query.start_time_ms, query.end_time_ms)? {
            let path = self.partition_path(&query.metric, window);
            for series in
                self.scan_file(&path, query.start_time_ms, query.end_time_ms, &query.tags)?
            {
                merged.entry(series.tags).or_default().extend(series.points);
            }
        }
        Ok(merged
            .into_iter()
            .filter(|(_, points)| !points.is_empty())
            .map(|(tags, points)| SeriesData { tags, points })
            .collect())
    }

    fn scan_file(
        &self,
        path: &Path,
        start_ms: i64,
        end_ms: i64,
        tag_filter: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<Vec<SeriesData>> {
        // Row form is derived from the columnar scan, preserving the
        // long/double distinction via a parallel pass.
        self.scan_file_rows(path, start_ms, end_ms, tag_filter)
    }

    /// Columnar time-range scan: per-series (timestamps, values) arrays
    /// fed directly to the vector kernels — no row materialization. Longs
    /// widen to f64, exactly as the aggregators' `as_f64` view does.
    pub fn scan_columns(&self, query: &DatastoreQuery) -> Result<Vec<kairos_core::ColumnSeries>> {
        let mut merged: BTreeMap<Tags, (Vec<i64>, Vec<f64>)> = BTreeMap::new();
        for window in self.partitions(&query.metric, query.start_time_ms, query.end_time_ms)? {
            let path = self.partition_path(&query.metric, window);
            let file = fs::File::open(&path).map_err(|e| pq_err("open", e))?;
            let builder = ParquetRecordBatchReaderBuilder::try_new_with_options(
                file,
                ArrowReaderOptions::new(),
            )
            .map_err(|e| pq_err("reader", e))?;
            let pruned = prune_row_groups(&builder, query.start_time_ms, query.end_time_ms);
            let reader = builder
                .with_row_groups(pruned)
                .build()
                .map_err(|e| pq_err("build reader", e))?;

            // Rows are series-contiguous: track the dictionary key id and
            // only do string/tag work when it changes; points accumulate
            // into a run that flushes on series change.
            let mut run: Option<(Tags, bool, Vec<i64>, Vec<f64>)> = None;
            for batch in reader {
                let batch = batch.map_err(|e| pq_err("read batch", e))?;
                let cols = BatchColumns::new(&batch)?;
                let mut current_id: Option<usize> = None; // ids are per-batch
                for row in 0..batch.num_rows() {
                    let id = cols.series.key(row).expect("non-null key");
                    if current_id != Some(id) {
                        current_id = Some(id);
                        let key = cols.series_values.value(id);
                        let changed = run
                            .as_ref()
                            .is_none_or(|(tags, ..)| tag_string(tags) != key);
                        if changed {
                            if let Some((tags, keep, ts, vals)) = run.take() {
                                if keep && !ts.is_empty() {
                                    let slot = merged.entry(tags).or_default();
                                    slot.0.extend(ts);
                                    slot.1.extend(vals);
                                }
                            }
                            let tags = parse_tag_string(key);
                            let keep = tags_match(&tags, &query.tags);
                            run = Some((tags, keep, Vec::new(), Vec::new()));
                        }
                    }
                    let (_, keep, ts_acc, val_acc) = run.as_mut().expect("set above");
                    if !*keep {
                        continue;
                    }
                    let t = cols.ts.value(row);
                    if t < query.start_time_ms || t > query.end_time_ms {
                        continue;
                    }
                    ts_acc.push(t);
                    val_acc.push(if cols.longs.is_valid(row) {
                        cols.longs.value(row) as f64
                    } else {
                        cols.doubles.value(row)
                    });
                }
            }
            if let Some((tags, keep, ts, vals)) = run.take() {
                if keep && !ts.is_empty() {
                    let slot = merged.entry(tags).or_default();
                    slot.0.extend(ts);
                    slot.1.extend(vals);
                }
            }
        }
        Ok(merged
            .into_iter()
            .filter(|(_, (ts, _))| !ts.is_empty())
            .map(|(tags, (timestamps, values))| kairos_core::ColumnSeries {
                tags,
                timestamps,
                values,
            })
            .collect())
    }

    fn scan_file_rows(
        &self,
        path: &Path,
        start_ms: i64,
        end_ms: i64,
        tag_filter: &std::collections::HashMap<String, Vec<String>>,
    ) -> Result<Vec<SeriesData>> {
        let file = fs::File::open(path).map_err(|e| pq_err("open", e))?;
        let builder =
            ParquetRecordBatchReaderBuilder::try_new_with_options(file, ArrowReaderOptions::new())
                .map_err(|e| pq_err("reader", e))?;
        let pruned = prune_row_groups(&builder, start_ms, end_ms);
        let reader = builder
            .with_row_groups(pruned)
            .build()
            .map_err(|e| pq_err("build reader", e))?;

        let mut out: Vec<SeriesData> = Vec::new();
        let mut skip_series = false;
        let mut current_key: Option<String> = None;
        for batch in reader {
            let batch = batch.map_err(|e| pq_err("read batch", e))?;
            let cols = BatchColumns::new(&batch)?;
            let mut current_id: Option<usize> = None; // ids are per-batch
            for row in 0..batch.num_rows() {
                let id = cols.series.key(row).expect("non-null key");
                if current_id != Some(id) {
                    current_id = Some(id);
                    let key = cols.series_values.value(id);
                    if current_key.as_deref() != Some(key) {
                        current_key = Some(key.to_string());
                        let tags = parse_tag_string(key);
                        skip_series = !tags_match(&tags, tag_filter);
                        if !skip_series {
                            out.push(SeriesData { tags, points: Vec::new() });
                        }
                    }
                }
                let t = cols.ts.value(row);
                if skip_series || t < start_ms || t > end_ms {
                    continue;
                }
                let value = if cols.longs.is_valid(row) {
                    Value::Long(cols.longs.value(row))
                } else {
                    Value::Double(cols.doubles.value(row))
                };
                out.last_mut()
                    .expect("series pushed above")
                    .points
                    .push(DataPoint { timestamp_ms: t, value });
            }
        }
        out.retain(|s| !s.points.is_empty());
        Ok(out)
    }

    /// Deletes the query's time range. Fully covered partitions are removed;
    /// partially covered ones are rewritten without the deleted rows.
    pub fn delete(&self, query: &DatastoreQuery) -> Result<()> {
        for window in self.partitions(&query.metric, query.start_time_ms, query.end_time_ms)? {
            let path = self.partition_path(&query.metric, window);
            let fully_covered = query.start_time_ms <= window
                && query.end_time_ms >= window + self.window_ms - 1
                && query.tags.is_empty();
            if fully_covered {
                fs::remove_file(&path).map_err(|e| pq_err("remove partition", e))?;
                continue;
            }
            let mut remaining = Vec::new();
            for mut series in self.read_partition(&query.metric, window)? {
                if tags_match(&series.tags, &query.tags) {
                    series.points.retain(|p| {
                        p.timestamp_ms < query.start_time_ms || p.timestamp_ms > query.end_time_ms
                    });
                }
                if !series.points.is_empty() {
                    remaining.push(series);
                }
            }
            if remaining.is_empty() {
                fs::remove_file(&path).map_err(|e| pq_err("remove partition", e))?;
            } else {
                self.write_partition(&query.metric, window, &remaining)?;
            }
        }
        // Drop empty metric dirs so metric_names stays accurate.
        let dir = self.metric_dir(&query.metric);
        if dir.exists() && fs::read_dir(&dir).map(|mut d| d.next().is_none()).unwrap_or(false) {
            let _ = fs::remove_dir(&dir);
        }
        Ok(())
    }

    pub fn metric_names(&self) -> Result<Vec<String>> {
        let mut names = Vec::new();
        for entry in fs::read_dir(&self.dir).map_err(|e| pq_err("read dir", e))? {
            let entry = entry.map_err(|e| pq_err("dir entry", e))?;
            if entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                let name = entry.file_name().to_string_lossy().to_string();
                // Reverse the conservative percent-encoding.
                let mut decoded = String::new();
                let mut chars = name.chars();
                while let Some(c) = chars.next() {
                    if c == '%' {
                        let hex: String = chars.by_ref().take(2).collect();
                        if let Ok(code) = u32::from_str_radix(&hex, 16) {
                            if let Some(ch) = char::from_u32(code) {
                                decoded.push(ch);
                                continue;
                            }
                        }
                        decoded.push('%');
                        decoded.push_str(&hex);
                    } else {
                        decoded.push(c);
                    }
                }
                names.push(decoded);
            }
        }
        names.sort();
        Ok(names)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn store() -> (ParquetStore, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "kairos-pq-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        (ParquetStore::open(&dir).unwrap(), dir)
    }

    fn series(tags: &[(&str, &str)], pts: &[(i64, f64)]) -> SeriesData {
        SeriesData {
            tags: tags.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            points: pts.iter().map(|(t, v)| DataPoint::new(*t, *v)).collect(),
        }
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

    #[test]
    fn roundtrip_with_mixed_types_and_filtering() {
        let (store, dir) = store();
        let mut s1 = series(&[("root", "CL")], &[(1_000, 61.5), (2_000, 62.5)]);
        s1.points.push(DataPoint::new(3_000, 42i64));
        let s2 = series(&[("root", "NG")], &[(1_500, 2.9)]);
        store.write_partition("price", 0, &[s1, s2]).unwrap();

        let all = store.query(&q("price", 0, 10_000)).unwrap();
        assert_eq!(all.len(), 2);
        let cl = all.iter().find(|s| s.tags["root"] == "CL").unwrap();
        assert_eq!(cl.points.len(), 3);
        assert_eq!(cl.points[2].value, Value::Long(42));

        let mut filtered = q("price", 0, 10_000);
        filtered.tags.insert("root".into(), vec!["NG".into()]);
        let ng = store.query(&filtered).unwrap();
        assert_eq!(ng.len(), 1);
        assert_eq!(ng[0].points[0].value, Value::Double(2.9));

        // Time bounds trim within the partition.
        let bounded = store.query(&q("price", 1_500, 2_500)).unwrap();
        let cl = bounded.iter().find(|s| s.tags["root"] == "CL").unwrap();
        assert_eq!(cl.points.len(), 1);

        assert_eq!(store.metric_names().unwrap(), vec!["price"]);
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn partition_pruning_by_window() {
        let (store, dir) = store();
        let w = DEFAULT_WINDOW_MS;
        store
            .write_partition("m", 0, &[series(&[], &[(10, 1.0)])])
            .unwrap();
        store
            .write_partition("m", w, &[series(&[], &[(w + 10, 2.0)])])
            .unwrap();

        assert_eq!(store.partitions("m", 0, w - 1).unwrap(), vec![0]);
        assert_eq!(store.partitions("m", w, 2 * w).unwrap(), vec![w]);
        assert_eq!(store.partitions("m", 0, 2 * w).unwrap(), vec![0, w]);

        let old = store.query(&q("m", 0, w - 1)).unwrap();
        assert_eq!(old[0].points[0].value, Value::Double(1.0));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn delete_rewrites_partial_and_removes_full() {
        let (store, dir) = store();
        store
            .write_partition("m", 0, &[series(&[("h", "a")], &[(1_000, 1.0), (2_000, 2.0)])])
            .unwrap();

        // Partial delete rewrites the file.
        store.delete(&q("m", 0, 1_500)).unwrap();
        let rest = store.query(&q("m", 0, 10_000)).unwrap();
        assert_eq!(rest[0].points.len(), 1);
        assert_eq!(rest[0].points[0].timestamp_ms, 2_000);

        // Full-range delete removes partition and metric dir.
        store.delete(&q("m", 0, DEFAULT_WINDOW_MS)).unwrap();
        assert!(store.query(&q("m", 0, 10_000)).unwrap().is_empty());
        assert!(store.metric_names().unwrap().is_empty());
        let _ = fs::remove_dir_all(dir);
    }
}
