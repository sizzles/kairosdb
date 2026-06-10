//! Wire-compatible query model: the JSON accepted by
//! `POST /api/v1/datapoints/query` in the Java implementation, and the
//! query execution pipeline (grouping → merge → aggregator chain).

use std::collections::{BTreeMap, HashMap};

use kairos_core::time::add_units;
use kairos_core::{DataPoint, Sampling, Tags, TimeUnit};
use serde::Deserialize;

use crate::{aggregators, Error, Result};

#[derive(Debug, Deserialize)]
pub struct QueryRequest {
    pub start_absolute: Option<i64>,
    pub end_absolute: Option<i64>,
    pub start_relative: Option<RelativeTime>,
    pub end_relative: Option<RelativeTime>,
    #[serde(default)]
    pub metrics: Vec<MetricQuery>,
}

#[derive(Debug, Clone, Copy, Deserialize)]
pub struct RelativeTime {
    /// Java accepts both `"value": 1` and `"value": "1"`.
    #[serde(deserialize_with = "lenient_i64")]
    pub value: i64,
    pub unit: TimeUnit,
}

fn lenient_i64<'de, D: serde::Deserializer<'de>>(de: D) -> std::result::Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(i64),
        Str(String),
    }
    match NumOrStr::deserialize(de)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

impl RelativeTime {
    fn before(&self, now_ms: i64) -> i64 {
        add_units(now_ms, self.unit, -self.value)
    }
}

impl QueryRequest {
    /// Resolve the query window, matching Java `QueryParser`: start is
    /// required, end defaults to now.
    pub fn resolve_time_range(&self, now_ms: i64) -> Result<(i64, i64)> {
        let start = match (self.start_absolute, &self.start_relative) {
            (Some(abs), _) => abs,
            (None, Some(rel)) => rel.before(now_ms),
            (None, None) => {
                return Err(Error::InvalidQuery(
                    "query must specify start_absolute or start_relative".into(),
                ))
            }
        };
        let end = match (self.end_absolute, &self.end_relative) {
            (Some(abs), _) => abs,
            (None, Some(rel)) => rel.before(now_ms),
            (None, None) => now_ms,
        };
        Ok((start, end))
    }
}

#[derive(Debug, Deserialize)]
pub struct MetricQuery {
    pub name: String,
    #[serde(default)]
    pub tags: HashMap<String, OneOrMany>,
    pub limit: Option<usize>,
    /// `asc` (default) or `desc`. Descending affects which points a `limit`
    /// keeps (the most recent) and the response ordering.
    pub order: Option<String>,
    #[serde(default)]
    pub aggregators: Vec<AggregatorSpec>,
    #[serde(default)]
    pub group_by: Vec<GroupBySpec>,
}

impl MetricQuery {
    pub fn descending(&self) -> bool {
        self.order.as_deref().is_some_and(|o| o.eq_ignore_ascii_case("desc"))
    }
}

impl MetricQuery {
    pub fn tag_filter(&self) -> HashMap<String, Vec<String>> {
        self.tags
            .iter()
            .map(|(k, v)| (k.clone(), v.clone().into_vec()))
            .collect()
    }
}

/// Java accepts both `"tags": {"host": "a"}` and `"tags": {"host": ["a","b"]}`.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum OneOrMany {
    One(String),
    Many(Vec<String>),
}

impl OneOrMany {
    pub fn into_vec(self) -> Vec<String> {
        match self {
            OneOrMany::One(v) => vec![v],
            OneOrMany::Many(v) => v,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct AggregatorSpec {
    pub name: String,
    pub sampling: Option<Sampling>,
    pub align_sampling: Option<bool>,
    pub align_start_time: Option<bool>,
    pub align_end_time: Option<bool>,
    pub factor: Option<f64>,
    pub percentile: Option<f64>,
    pub divisor: Option<f64>,
    pub size: Option<i64>,
    pub unit: Option<TimeUnit>,
    pub time_unit: Option<TimeUnit>,
    pub filter_op: Option<String>,
    pub threshold: Option<f64>,
    pub trim: Option<String>,
    pub pad_value: Option<i64>,
    pub thresholds: Option<Vec<ThresholdSpec>>,
    /// `score`'s threshold order: `ascending` (default) or `descending`.
    pub order: Option<String>,
    /// `save_as` target metric and extra tags.
    pub metric_name: Option<String>,
    pub tags: Option<BTreeMap<String, String>>,
    pub ttl: Option<u32>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThresholdSpec {
    pub value: f64,
    pub boundary: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct GroupBySpec {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
    /// `time` group-by: `{"value": 1, "unit": "days"}`; `value` group-by: a
    /// plain number.
    pub range_size: Option<serde_json::Value>,
    pub group_count: Option<i64>,
    pub bins: Option<Vec<f64>>,
}

/// One stored series fed into query execution.
#[derive(Debug, Clone)]
pub struct SeriesInput {
    pub tags: Tags,
    pub points: Vec<DataPoint>,
}

/// One aggregated output group.
#[derive(Debug)]
pub struct GroupResult {
    /// Grouping tag values (empty when not grouping by tag).
    pub group: BTreeMap<String, String>,
    /// Union of tag values across the merged series, as in Java responses.
    pub tags: BTreeMap<String, Vec<String>>,
    /// Response `group_by` entries (tag/time/value/bin), excluding the
    /// always-present `type` entry.
    pub group_by_entries: Vec<serde_json::Value>,
    pub points: Vec<DataPoint>,
}

/// Point-level grouper: assigns each point a group id, the analogue of
/// `GroupBy.getGroupId`.
enum PointGrouper {
    /// `TimeGroupBy`: bucket index modulo `group_count`, anchored at the
    /// query start. Months use calendar math; other units use Java's fixed
    /// conversion (a "year" is 52 weeks).
    Time {
        range_value: i64,
        range_unit: TimeUnit,
        group_count: i64,
        start_ms: i64,
    },
    /// `ValueGroupBy`: value truncated to an integer, divided by range size.
    Value { range_size: i64 },
    /// `BinGroupBy`: index of the half-open bin containing the value.
    Bin { bins: Vec<f64> },
}

impl PointGrouper {
    fn from_spec(spec: &GroupBySpec, query_start_ms: i64) -> Result<Option<PointGrouper>> {
        match spec.name.as_str() {
            "tag" => Ok(None), // handled at the series level
            "time" => {
                let range = spec.range_size.as_ref().ok_or_else(|| {
                    Error::InvalidQuery("time group_by requires range_size".into())
                })?;
                let sampling: Sampling = serde_json::from_value(range.clone())
                    .map_err(|e| Error::InvalidQuery(format!("invalid range_size: {e}")))?;
                Ok(Some(PointGrouper::Time {
                    range_value: sampling.value,
                    range_unit: sampling.unit,
                    group_count: spec.group_count.ok_or_else(|| {
                        Error::InvalidQuery("time group_by requires group_count".into())
                    })?,
                    start_ms: query_start_ms,
                }))
            }
            "value" => {
                let range_size = spec
                    .range_size
                    .as_ref()
                    .and_then(serde_json::Value::as_i64)
                    .filter(|v| *v > 0)
                    .ok_or_else(|| {
                        Error::InvalidQuery("value group_by requires a numeric range_size".into())
                    })?;
                Ok(Some(PointGrouper::Value { range_size }))
            }
            "bin" => Ok(Some(PointGrouper::Bin {
                bins: spec.bins.clone().filter(|b| !b.is_empty()).ok_or_else(|| {
                    Error::InvalidQuery("bin group_by requires bins".into())
                })?,
            })),
            other => Err(Error::InvalidQuery(format!("unknown group_by: {other}"))),
        }
    }

    fn group_id(&self, point: &DataPoint) -> i32 {
        match self {
            PointGrouper::Time {
                range_value,
                range_unit,
                group_count,
                start_ms,
            } => {
                if *range_unit == TimeUnit::Months {
                    let months = kairos_core::time::unit_difference(
                        point.timestamp_ms,
                        *start_ms,
                        TimeUnit::Months,
                    );
                    (months % group_count) as i32
                } else {
                    let range_ms = java_group_size_millis(*range_value, *range_unit);
                    (((point.timestamp_ms - start_ms) / range_ms) % group_count) as i32
                }
            }
            PointGrouper::Value { range_size } => match &point.value {
                kairos_core::Value::Long(v) => (v / range_size) as i32,
                kairos_core::Value::Double(v) => (*v as i32) / *range_size as i32,
                _ => -1,
            },
            PointGrouper::Bin { bins } => {
                let Some(v) = point.value.as_f64() else { return -1 };
                if v < bins[0] {
                    return 0;
                }
                for i in 0..bins.len() - 1 {
                    if v >= bins[i] && v < bins[i + 1] {
                        return (i + 1) as i32;
                    }
                }
                bins.len() as i32
            }
        }
    }

    /// Response entry for this grouper, matching `GroupByResult.toJson`.
    fn result_entry(&self, id: i32) -> serde_json::Value {
        match self {
            PointGrouper::Time {
                range_value,
                range_unit,
                group_count,
                ..
            } => serde_json::json!({
                "name": "time",
                "range_size": {"value": range_value, "unit": time_unit_name(*range_unit)},
                "group_count": group_count,
                "group": {"group_number": id},
            }),
            PointGrouper::Value { range_size } => serde_json::json!({
                "name": "value",
                "range_size": range_size,
                "group": {"group_number": id},
            }),
            PointGrouper::Bin { bins } => serde_json::json!({
                "name": "bin",
                "bins": bins,
                "group": {"bin_number": id},
            }),
        }
    }
}

/// Java `TimeGroupBy.convertGroupSizeToMillis`, fallthrough included: a year
/// is 52 weeks. Also used by the `time_diff` aggregator's unit divisor.
pub(crate) fn java_group_size_millis(value: i64, unit: TimeUnit) -> i64 {
    let mut ms = value;
    let factors: &[(TimeUnit, i64)] = &[
        (TimeUnit::Years, 52),
        (TimeUnit::Weeks, 7),
        (TimeUnit::Days, 24),
        (TimeUnit::Hours, 60),
        (TimeUnit::Minutes, 60),
        (TimeUnit::Seconds, 1000),
    ];
    let mut multiplying = false;
    for (u, factor) in factors {
        if *u == unit {
            multiplying = true;
        }
        if multiplying {
            ms *= factor;
        }
    }
    ms
}

/// Java `TimeUnit.toString()` for response JSON.
fn time_unit_name(unit: TimeUnit) -> &'static str {
    match unit {
        TimeUnit::Milliseconds => "MILLISECONDS",
        TimeUnit::Seconds => "SECONDS",
        TimeUnit::Minutes => "MINUTES",
        TimeUnit::Hours => "HOURS",
        TimeUnit::Days => "DAYS",
        TimeUnit::Weeks => "WEEKS",
        TimeUnit::Months => "MONTHS",
        TimeUnit::Years => "YEARS",
    }
}

/// Executes a metric query over the series returned by the datastore:
/// partition by tag group-by (or merge everything, the Java default), then by
/// any point-level group-bys (time/value/bin), then run the aggregator chain
/// anchored at the query start. Returns the result groups plus any series
/// produced by `save_as` (the caller writes those back).
pub fn execute(
    metric: &MetricQuery,
    series: Vec<SeriesInput>,
    query_start_ms: i64,
    query_end_ms: i64,
) -> Result<(Vec<GroupResult>, Vec<kairos_core::DataPointSet>)> {
    let group_tags: Vec<&String> = metric
        .group_by
        .iter()
        .filter(|g| g.name == "tag")
        .flat_map(|g| g.tags.iter())
        .collect();
    let groupers: Vec<PointGrouper> = metric
        .group_by
        .iter()
        .filter_map(|g| PointGrouper::from_spec(g, query_start_ms).transpose())
        .collect::<Result<_>>()?;

    let mut groups: BTreeMap<Vec<String>, Vec<SeriesInput>> = BTreeMap::new();
    for s in series {
        let key: Vec<String> = group_tags
            .iter()
            .map(|t| s.tags.get(*t).cloned().unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(s);
    }

    let mut results = Vec::new();
    let mut saved: Vec<kairos_core::DataPointSet> = Vec::new();
    for (key, members) in groups {
        let group: BTreeMap<String, String> = group_tags
            .iter()
            .zip(&key)
            .map(|(t, v)| ((*t).clone(), v.clone()))
            .collect();

        let mut tags: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut points = Vec::new();
        for member in members {
            for (k, v) in &member.tags {
                let values = tags.entry(k.clone()).or_default();
                if !values.contains(v) {
                    values.push(v.clone());
                }
            }
            points.extend(member.points);
        }
        points.sort_by_key(|p| p.timestamp_ms);

        // Partition the merged points by the point-level group ids, then
        // aggregate each partition independently.
        let mut partitions: BTreeMap<Vec<i32>, Vec<DataPoint>> = BTreeMap::new();
        if groupers.is_empty() {
            partitions.insert(Vec::new(), points);
        } else {
            for point in points {
                let ids: Vec<i32> = groupers.iter().map(|g| g.group_id(&point)).collect();
                partitions.entry(ids).or_default().push(point);
            }
        }

        for (ids, mut points) in partitions {
            let ctx = crate::QueryContext {
                start_ms: query_start_ms,
                end_ms: query_end_ms,
                source_metric: metric.name.clone(),
                group_tags: group.clone(),
                save_sink: std::sync::Mutex::new(Vec::new()),
            };
            for spec in &metric.aggregators {
                let aggregator = aggregators::build(spec)?;
                points = aggregator.run(&ctx, points);
            }
            saved.extend(ctx.save_sink.into_inner().expect("save sink poisoned"));

            // Java sorts descending before aggregation; we aggregate
            // ascending and reverse the output, which matches the response
            // ordering for the practical cases (raw and most-recent-N).
            if metric.descending() {
                points.reverse();
            }

            let mut group_by_entries = Vec::new();
            if !group_tags.is_empty() {
                group_by_entries.push(serde_json::json!({
                    "name": "tag",
                    "tags": group_tags,
                    "group": group,
                }));
            }
            for (grouper, id) in groupers.iter().zip(&ids) {
                group_by_entries.push(grouper.result_entry(*id));
            }

            results.push(GroupResult {
                group: group.clone(),
                tags: tags.clone(),
                group_by_entries,
                points,
            });
        }
    }
    Ok((results, saved))
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairos_core::Value;

    fn series(tag_pairs: &[(&str, &str)], values: &[(i64, f64)]) -> SeriesInput {
        SeriesInput {
            tags: tag_pairs
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            points: values.iter().map(|(t, v)| DataPoint::new(*t, *v)).collect(),
        }
    }

    fn parse_metric(json: &str) -> MetricQuery {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn parses_java_query_json() {
        let req: QueryRequest = serde_json::from_str(
            r#"{
                "start_absolute": 1000,
                "end_relative": {"value": 1, "unit": "hours"},
                "metrics": [{
                    "name": "sys.cpu",
                    "tags": {"host": ["a", "b"], "dc": "lga"},
                    "aggregators": [
                        {"name": "avg", "sampling": {"value": 5, "unit": "minutes"}}
                    ],
                    "group_by": [{"name": "tag", "tags": ["host"]}]
                }]
            }"#,
        )
        .unwrap();
        let now = 10_000_000;
        assert_eq!(req.resolve_time_range(now).unwrap(), (1000, now - 3_600_000));
        assert_eq!(req.metrics[0].tag_filter()["host"], vec!["a", "b"]);
        assert_eq!(req.metrics[0].tag_filter()["dc"], vec!["lga"]);
    }

    #[test]
    fn merges_all_series_without_group_by() {
        let metric = parse_metric(
            r#"{"name": "m", "aggregators": [
                {"name": "sum", "sampling": {"value": 100, "unit": "milliseconds"},
                 "align_sampling": false}]}"#,
        );
        let (results, _) = execute(
            &metric,
            vec![
                series(&[("host", "a")], &[(1, 1.0)]),
                series(&[("host", "b")], &[(2, 2.0)]),
            ],
            0,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].points, vec![DataPoint::new(1, Value::Double(3.0))]);
        assert_eq!(results[0].tags["host"], vec!["a", "b"]);
    }

    #[test]
    fn group_by_time_buckets_by_day_of_week() {
        // Two-group day-of-week style grouping: 1-day ranges, 2 groups.
        let metric = parse_metric(
            r#"{"name": "m", "group_by": [
                {"name": "time", "group_count": 2,
                 "range_size": {"value": 1, "unit": "days"}}]}"#,
        );
        const DAY: i64 = 86_400_000;
        let (results, _) = execute(
            &metric,
            vec![series(&[], &[(0, 1.0), (100, 2.0), (DAY + 5, 3.0)])],
            0,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].group_by_entries[0]["group"]["group_number"], 0);
        assert_eq!(results[0].points.len(), 2);
        assert_eq!(results[1].group_by_entries[0]["group"]["group_number"], 1);
        assert_eq!(results[1].points.len(), 1);
    }

    #[test]
    fn group_by_value_buckets_by_magnitude() {
        let metric = parse_metric(
            r#"{"name": "m", "group_by": [{"name": "value", "range_size": 10}]}"#,
        );
        let (results, _) = execute(
            &metric,
            vec![series(&[], &[(1, 3.0), (2, 25.0), (3, 7.0)])],
            0,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].group_by_entries[0]["group"]["group_number"], 0);
        assert_eq!(results[0].points.len(), 2);
        assert_eq!(results[1].group_by_entries[0]["group"]["group_number"], 2);
    }

    #[test]
    fn group_by_bin_uses_half_open_bins() {
        let metric = parse_metric(
            r#"{"name": "m", "group_by": [{"name": "bin", "bins": [10, 20]}]}"#,
        );
        let (results, _) = execute(
            &metric,
            vec![series(&[], &[(1, 5.0), (2, 15.0), (3, 25.0)])],
            0,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(results.len(), 3);
        let ids: Vec<_> = results
            .iter()
            .map(|r| r.group_by_entries[0]["group"]["bin_number"].as_i64().unwrap())
            .collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    #[test]
    fn group_by_tag_splits_series() {
        let metric = parse_metric(r#"{"name": "m", "group_by": [{"name": "tag", "tags": ["host"]}]}"#);
        let (results, _) = execute(
            &metric,
            vec![
                series(&[("host", "a")], &[(1, 1.0)]),
                series(&[("host", "b")], &[(2, 2.0)]),
                series(&[("host", "a"), ("dc", "lga")], &[(3, 3.0)]),
            ],
            0,
            i64::MAX,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].group["host"], "a");
        assert_eq!(results[0].points.len(), 2);
        assert_eq!(results[1].group["host"], "b");
    }
}
