//! Query engine: range aggregation and the wire-compatible query model.
//!
//! The aggregation semantics replicate `org.kairosdb.core.aggregator`:
//! ranges are anchored at the query start time (optionally aligned to a
//! sampling boundary), buckets are skipped when empty unless the aggregator
//! is exhaustive (`pad`, `gaps`), and the output timestamp is the first
//! point's time in the bucket unless alignment is requested.

pub mod aggregators;
pub mod model;

use std::collections::BTreeMap;
use std::sync::Mutex;

use kairos_core::time::{align_range_boundary, RangeCalc};
use kairos_core::{DataPoint, DataPointSet, Sampling};

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

/// Per-execution state handed to aggregators: the query window, the current
/// group's identity (for `save_as` tag selection), and the sink that
/// collects series the caller must write back to the datastore.
pub struct QueryContext {
    pub start_ms: i64,
    pub end_ms: i64,
    pub source_metric: String,
    /// Tag values of the current tag group-by partition.
    pub group_tags: BTreeMap<String, String>,
    /// Series produced by `save_as`, to be ingested by the caller.
    pub save_sink: Mutex<Vec<DataPointSet>>,
}

/// Aggregates the points within one time range — the Rust analogue of
/// `RangeAggregator.RangeSubAggregator`. In exhaustive mode this is also
/// called for empty ranges.
pub trait RangeSubAggregator: Send + Sync {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint>;
}

/// Transforms a whole series — the analogue of non-range aggregators such as
/// `scale` and `rate`.
pub trait SeriesAggregator: Send + Sync {
    fn aggregate(&self, ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint>;
}

pub enum Aggregator {
    Range {
        sampling: Sampling,
        align_sampling: bool,
        align_start_time: bool,
        align_end_time: bool,
        /// Visit every range in the query window, even empty ones
        /// (`pad`/`gaps`, Java's `ExhaustiveRangeDataPointAggregator`).
        exhaustive: bool,
        sub: Box<dyn RangeSubAggregator>,
    },
    Series(Box<dyn SeriesAggregator>),
}

impl Aggregator {
    /// Runs this aggregator over a sorted series. `ctx.start_ms` anchors the
    /// range grid, exactly as `RangeAggregator.setStartTime` does.
    pub fn run(&self, ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        match self {
            Aggregator::Series(agg) => agg.aggregate(ctx, points),
            Aggregator::Range {
                sampling,
                align_sampling,
                align_start_time,
                align_end_time,
                exhaustive,
                sub,
            } => {
                let anchor = if *align_sampling {
                    align_range_boundary(ctx.start_ms, sampling.unit)
                } else {
                    ctx.start_ms
                };
                let calc = RangeCalc::new(anchor, *sampling);
                if *exhaustive {
                    return run_exhaustive(&calc, ctx, sub.as_ref(), *align_start_time, points);
                }
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

/// Java `ExhaustiveRangeDataPointAggregator`: walk every range from the
/// query start until both the query window and the data are exhausted;
/// empty ranges are stamped at the range start.
fn run_exhaustive(
    calc: &RangeCalc,
    ctx: &QueryContext,
    sub: &dyn RangeSubAggregator,
    align_start: bool,
    points: Vec<DataPoint>,
) -> Vec<DataPoint> {
    // Java caps the end at "now" so an open-ended query doesn't pad forever.
    let now_ms = chrono::Utc::now().timestamp_millis();
    let query_end = ctx.end_ms.min(now_ms);

    let mut out = Vec::new();
    let mut idx = 0;
    let mut next_start = ctx.start_ms;
    while next_start < query_end || idx < points.len() {
        let start_range = calc.start_range(next_start);
        let end_range = calc.end_range(next_start);

        let bucket_start = idx;
        while idx < points.len() && points[idx].timestamp_ms < end_range {
            idx += 1;
        }
        let bucket = &points[bucket_start..idx];

        // Java: the data point time is the first point's timestamp unless
        // aligning or the range is empty, in which case it's the range start.
        let first_ts = bucket.first().map_or(i64::MAX, |p| p.timestamp_ms);
        let return_time = if align_start || end_range <= first_ts {
            start_range
        } else {
            first_ts
        };
        out.extend(sub.aggregate(return_time, bucket));
        next_start = end_range;
    }
    out
}

#[cfg(test)]
pub(crate) fn test_context(start_ms: i64, end_ms: i64) -> QueryContext {
    QueryContext {
        start_ms,
        end_ms,
        source_metric: "test.metric".to_string(),
        group_tags: BTreeMap::new(),
        save_sink: Mutex::new(Vec::new()),
    }
}

#[cfg(test)]
mod tests {
    use super::aggregators::{PadSub, SumSub};
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
            exhaustive: false,
            sub: Box::new(SumSub),
        }
    }

    #[test]
    fn sums_within_ranges_anchored_at_query_start() {
        let ctx = test_context(0, 100);
        let result = sum_agg(10).run(&ctx, pts(&[(1, 1.0), (2, 2.0), (11, 3.0), (25, 4.0)]));
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
        let ctx = test_context(0, 100);
        let result = sum_agg(10).run(&ctx, pts(&[(1, 1.0), (95, 2.0)]));
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn align_start_time_snaps_output_timestamps() {
        let agg = Aggregator::Range {
            sampling: Sampling::new(10, TimeUnit::Milliseconds),
            align_sampling: false,
            align_start_time: true,
            align_end_time: false,
            exhaustive: false,
            sub: Box::new(SumSub),
        };
        let ctx = test_context(0, 100);
        let result = agg.run(&ctx, pts(&[(7, 1.0), (13, 2.0)]));
        assert_eq!(result[0].timestamp_ms, 0);
        assert_eq!(result[1].timestamp_ms, 10);
    }

    #[test]
    fn exhaustive_pad_fills_empty_ranges() {
        let agg = Aggregator::Range {
            sampling: Sampling::new(10, TimeUnit::Milliseconds),
            align_sampling: false,
            align_start_time: false,
            align_end_time: false,
            exhaustive: true,
            sub: Box::new(PadSub { pad_value: 0 }),
        };
        let ctx = test_context(0, 40);
        let result = agg.run(&ctx, pts(&[(1, 1.0), (25, 2.0)]));
        // Ranges [0,10): data, [10,20): pad, [20,30): data, [30,40): pad
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].value, Value::Double(1.0));
        assert_eq!(result[1], DataPoint::new(10, Value::Long(0)));
        assert_eq!(result[2].timestamp_ms, 25);
        assert_eq!(result[3], DataPoint::new(30, Value::Long(0)));
    }
}
