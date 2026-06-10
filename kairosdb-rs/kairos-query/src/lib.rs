//! Query engine: range aggregation and the wire-compatible query model.
//!
//! The aggregation semantics replicate `org.kairosdb.core.aggregator`:
//! ranges are anchored at the query start time (optionally aligned to a
//! sampling boundary), buckets are skipped when empty (non-exhaustive mode),
//! and the output timestamp is the first point's time in the bucket unless
//! `align_start_time`/`align_end_time` is set.

pub mod aggregators;
pub mod model;

use kairos_core::time::{align_range_boundary, RangeCalc};
use kairos_core::{DataPoint, Sampling};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("unknown aggregator: {0}")]
    UnknownAggregator(String),
    #[error("aggregator {0} requires a sampling")]
    MissingSampling(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Aggregates the points within one time range — the Rust analogue of
/// `RangeAggregator.RangeSubAggregator`.
pub trait RangeSubAggregator: Send + Sync {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint>;
}

/// Transforms a whole series — the analogue of non-range aggregators such as
/// `scale` and `diff`.
pub trait SeriesAggregator: Send + Sync {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint>;
}

pub enum Aggregator {
    Range {
        sampling: Sampling,
        align_sampling: bool,
        align_start_time: bool,
        align_end_time: bool,
        sub: Box<dyn RangeSubAggregator>,
    },
    Series(Box<dyn SeriesAggregator>),
}

impl Aggregator {
    /// Runs this aggregator over a sorted series. `query_start_ms` anchors
    /// the range grid, exactly as `RangeAggregator.setStartTime` does.
    pub fn run(&self, query_start_ms: i64, points: Vec<DataPoint>) -> Vec<DataPoint> {
        match self {
            Aggregator::Series(agg) => agg.aggregate(points),
            Aggregator::Range {
                sampling,
                align_sampling,
                align_start_time,
                align_end_time,
                sub,
            } => {
                let anchor = if *align_sampling {
                    align_range_boundary(query_start_ms, sampling.unit)
                } else {
                    query_start_ms
                };
                let calc = RangeCalc::new(anchor, *sampling);
                let mut out = Vec::new();
                let mut bucket: Vec<DataPoint> = Vec::new();
                let mut bucket_end = i64::MIN;
                for point in points {
                    if point.timestamp_ms >= bucket_end {
                        flush_bucket(
                            sub.as_ref(),
                            &calc,
                            &mut bucket,
                            *align_start_time,
                            *align_end_time,
                            &mut out,
                        );
                        bucket_end = calc.end_range(point.timestamp_ms);
                    }
                    bucket.push(point);
                }
                flush_bucket(
                    sub.as_ref(),
                    &calc,
                    &mut bucket,
                    *align_start_time,
                    *align_end_time,
                    &mut out,
                );
                out
            }
        }
    }
}

fn flush_bucket(
    sub: &dyn RangeSubAggregator,
    calc: &RangeCalc,
    bucket: &mut Vec<DataPoint>,
    align_start: bool,
    align_end: bool,
    out: &mut Vec<DataPoint>,
) {
    if bucket.is_empty() {
        return;
    }
    // Java getDataPointTime(): first point's timestamp, or the range
    // start/end when alignment is requested.
    let first_ts = bucket[0].timestamp_ms;
    let return_time = if align_start {
        calc.start_range(first_ts)
    } else if align_end {
        calc.end_range(first_ts)
    } else {
        first_ts
    };
    out.extend(sub.aggregate(return_time, bucket));
    bucket.clear();
}

#[cfg(test)]
mod tests {
    use super::aggregators::SumSub;
    use super::*;
    use kairos_core::{TimeUnit, Value};

    fn pts(values: &[(i64, f64)]) -> Vec<DataPoint> {
        values.iter().map(|(t, v)| DataPoint::new(*t, *v)).collect()
    }

    fn sum_agg(sampling_ms: i64) -> Aggregator {
        Aggregator::Range {
            sampling: Sampling::new(sampling_ms, TimeUnit::Milliseconds),
            align_sampling: false,
            align_start_time: false,
            align_end_time: false,
            sub: Box::new(SumSub),
        }
    }

    #[test]
    fn sums_within_ranges_anchored_at_query_start() {
        let agg = sum_agg(10);
        let result = agg.run(0, pts(&[(1, 1.0), (2, 2.0), (11, 3.0), (25, 4.0)]));
        // Buckets [0,10): 1+2, [10,20): 3, [20,30): 4
        assert_eq!(
            result,
            vec![
                DataPoint::new(1, Value::Double(3.0)),
                DataPoint::new(11, Value::Double(3.0)),
                DataPoint::new(25, Value::Double(4.0)),
            ]
        );
    }

    #[test]
    fn empty_ranges_are_skipped_not_zero_filled() {
        let agg = sum_agg(10);
        let result = agg.run(0, pts(&[(1, 1.0), (95, 2.0)]));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn align_start_time_snaps_output_timestamps() {
        let agg = Aggregator::Range {
            sampling: Sampling::new(10, TimeUnit::Milliseconds),
            align_sampling: false,
            align_start_time: true,
            align_end_time: false,
            sub: Box::new(SumSub),
        };
        let result = agg.run(0, pts(&[(7, 1.0), (13, 2.0)]));
        assert_eq!(result[0].timestamp_ms, 0);
        assert_eq!(result[1].timestamp_ms, 10);
    }
}
