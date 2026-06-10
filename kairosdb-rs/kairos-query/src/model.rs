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
    pub value: i64,
    pub unit: TimeUnit,
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
    #[serde(default)]
    pub aggregators: Vec<AggregatorSpec>,
    #[serde(default)]
    pub group_by: Vec<GroupBySpec>,
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
}

#[derive(Debug, Deserialize)]
pub struct GroupBySpec {
    pub name: String,
    #[serde(default)]
    pub tags: Vec<String>,
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
    pub points: Vec<DataPoint>,
}

/// Executes a metric query over the series returned by the datastore:
/// group by tags (or merge everything, the Java default), merge each group
/// in time order, then run the aggregator chain anchored at the query start.
pub fn execute(
    metric: &MetricQuery,
    series: Vec<SeriesInput>,
    query_start_ms: i64,
) -> Result<Vec<GroupResult>> {
    let group_tags: Vec<&String> = metric
        .group_by
        .iter()
        .filter(|g| g.name == "tag")
        .flat_map(|g| g.tags.iter())
        .collect();

    let mut groups: BTreeMap<Vec<String>, Vec<SeriesInput>> = BTreeMap::new();
    for s in series {
        let key: Vec<String> = group_tags
            .iter()
            .map(|t| s.tags.get(*t).cloned().unwrap_or_default())
            .collect();
        groups.entry(key).or_default().push(s);
    }

    let mut results = Vec::new();
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

        for spec in &metric.aggregators {
            let aggregator = aggregators::build(spec)?;
            points = aggregator.run(query_start_ms, points);
        }

        results.push(GroupResult { group, tags, points });
    }
    Ok(results)
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
        let results = execute(
            &metric,
            vec![
                series(&[("host", "a")], &[(1, 1.0)]),
                series(&[("host", "b")], &[(2, 2.0)]),
            ],
            0,
        )
        .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].points, vec![DataPoint::new(1, Value::Double(3.0))]);
        assert_eq!(results[0].tags["host"], vec!["a", "b"]);
    }

    #[test]
    fn group_by_tag_splits_series() {
        let metric = parse_metric(r#"{"name": "m", "group_by": [{"name": "tag", "tags": ["host"]}]}"#);
        let results = execute(
            &metric,
            vec![
                series(&[("host", "a")], &[(1, 1.0)]),
                series(&[("host", "b")], &[(2, 2.0)]),
                series(&[("host", "a"), ("dc", "lga")], &[(3, 3.0)]),
            ],
            0,
        )
        .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].group["host"], "a");
        assert_eq!(results[0].points.len(), 2);
        assert_eq!(results[1].group["host"], "b");
    }
}
