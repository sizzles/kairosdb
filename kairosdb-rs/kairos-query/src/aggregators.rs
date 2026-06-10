//! Built-in aggregators, semantics matched to `org.kairosdb.core.aggregator`.

use kairos_core::{DataPoint, Value};

use crate::{Aggregator, Error, RangeSubAggregator, Result, SeriesAggregator};
use crate::model::AggregatorSpec;

pub struct SumSub;

impl RangeSubAggregator for SumSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let sum: f64 = points.iter().filter_map(|p| p.value.as_f64()).sum();
        vec![DataPoint::new(return_time, sum)]
    }
}

pub struct AvgSub;

impl RangeSubAggregator for AvgSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let numeric: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();
        if numeric.is_empty() {
            return Vec::new();
        }
        let avg = numeric.iter().sum::<f64>() / numeric.len() as f64;
        vec![DataPoint::new(return_time, avg)]
    }
}

pub struct MinSub;

impl RangeSubAggregator for MinSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let min = points
            .iter()
            .filter_map(|p| p.value.as_f64())
            .fold(f64::MAX, f64::min);
        vec![DataPoint::new(return_time, min)]
    }
}

pub struct MaxSub;

impl RangeSubAggregator for MaxSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let max = points
            .iter()
            .filter_map(|p| p.value.as_f64())
            .fold(f64::MIN, f64::max);
        vec![DataPoint::new(return_time, max)]
    }
}

pub struct CountSub;

impl RangeSubAggregator for CountSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, Value::Long(points.len() as i64))]
    }
}

/// Java `FirstAggregator` re-stamps the first point to the range time.
pub struct FirstSub;

impl RangeSubAggregator for FirstSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        points
            .first()
            .map(|p| DataPoint::new(return_time, p.value.clone()))
            .into_iter()
            .collect()
    }
}

/// Java `LastAggregator` keeps the last point's own timestamp unless an
/// align flag was set on the aggregator.
pub struct LastSub {
    pub aligned: bool,
}

impl RangeSubAggregator for LastSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        points
            .last()
            .map(|p| {
                let ts = if self.aligned { return_time } else { p.timestamp_ms };
                DataPoint::new(ts, p.value.clone())
            })
            .into_iter()
            .collect()
    }
}

/// Java `ScaleAggregator`: multiply every point by a factor.
pub struct Scale {
    pub factor: f64,
}

impl SeriesAggregator for Scale {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .into_iter()
            .map(|p| {
                let scaled = p.value.as_f64().map(|v| v * self.factor);
                match scaled {
                    Some(v) => DataPoint::new(p.timestamp_ms, v),
                    None => p,
                }
            })
            .collect()
    }
}

/// Java `DiffAggregator`: difference between each point and its predecessor;
/// emits n-1 points stamped at the later point's time.
pub struct Diff;

impl SeriesAggregator for Diff {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .windows(2)
            .filter_map(|w| {
                let (prev, cur) = (w[0].value.as_f64()?, w[1].value.as_f64()?);
                Some(DataPoint::new(w[1].timestamp_ms, cur - prev))
            })
            .collect()
    }
}

/// Builds an [`Aggregator`] from a parsed wire spec, the analogue of the
/// Guice-registered aggregator factory.
pub fn build(spec: &AggregatorSpec) -> Result<Aggregator> {
    let range = |sub: Box<dyn RangeSubAggregator>| -> Result<Aggregator> {
        let sampling = spec
            .sampling
            .ok_or_else(|| Error::MissingSampling(spec.name.clone()))?;
        Ok(Aggregator::Range {
            sampling,
            // Java's RangeAggregator constructs with alignSampling = true.
            align_sampling: spec.align_sampling.unwrap_or(true),
            align_start_time: spec.align_start_time.unwrap_or(false),
            align_end_time: spec.align_end_time.unwrap_or(false),
            sub,
        })
    };

    match spec.name.as_str() {
        "sum" => range(Box::new(SumSub)),
        "avg" => range(Box::new(AvgSub)),
        "min" => range(Box::new(MinSub)),
        "max" => range(Box::new(MaxSub)),
        "count" => range(Box::new(CountSub)),
        "first" => range(Box::new(FirstSub)),
        "last" => range(Box::new(LastSub {
            aligned: spec.align_start_time.unwrap_or(false)
                || spec.align_end_time.unwrap_or(false),
        })),
        "scale" => Ok(Aggregator::Series(Box::new(Scale {
            factor: spec
                .factor
                .ok_or_else(|| Error::InvalidQuery("scale requires a factor".into()))?,
        }))),
        "diff" => Ok(Aggregator::Series(Box::new(Diff))),
        other => Err(Error::UnknownAggregator(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pts(values: &[(i64, f64)]) -> Vec<DataPoint> {
        values.iter().map(|(t, v)| DataPoint::new(*t, *v)).collect()
    }

    #[test]
    fn avg_counts_numeric_points_only() {
        let mut points = pts(&[(1, 2.0), (2, 4.0)]);
        points.push(DataPoint::new(3, "text"));
        assert_eq!(
            AvgSub.aggregate(1, &points),
            vec![DataPoint::new(1, Value::Double(3.0))]
        );
    }

    #[test]
    fn count_emits_long() {
        assert_eq!(
            CountSub.aggregate(5, &pts(&[(1, 1.0), (2, 2.0)])),
            vec![DataPoint::new(5, Value::Long(2))]
        );
    }

    #[test]
    fn last_keeps_own_timestamp_when_unaligned() {
        let agg = LastSub { aligned: false };
        assert_eq!(
            agg.aggregate(0, &pts(&[(1, 1.0), (9, 2.0)])),
            vec![DataPoint::new(9, Value::Double(2.0))]
        );
    }

    #[test]
    fn diff_emits_n_minus_one_points() {
        assert_eq!(
            Diff.aggregate(pts(&[(1, 10.0), (2, 13.0), (3, 11.0)])),
            vec![
                DataPoint::new(2, Value::Double(3.0)),
                DataPoint::new(3, Value::Double(-2.0)),
            ]
        );
    }
}
