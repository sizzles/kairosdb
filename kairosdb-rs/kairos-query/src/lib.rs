//! Query engine: range aggregation and the wire-compatible query model.
//!
//! The aggregation semantics replicate `org.kairosdb.core.aggregator`:
//! ranges are anchored at the query start time (optionally aligned to a
//! sampling boundary), buckets are skipped when empty unless the aggregator
//! is exhaustive (`pad`, `gaps`), and the output timestamp is the first
//! point's time in the bucket unless alignment is requested.

pub mod aggregators;
pub mod columnar;
pub mod model;

use std::collections::BTreeMap;
use std::sync::Mutex;

use chrono_tz::Tz;
use kairos_core::time::{align_range_boundary_tz, RangeCalc};
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
    /// Query-level `time_zone` (Java `TimezoneAware`), default UTC.
    pub tz: Tz,
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

    /// Columnar fast path over the pre-extracted value column (same index
    /// space as `points`). Kernels that benefit override this (and
    /// `wants_columnar`); the default falls back to the row form.
    fn aggregate_columnar(
        &self,
        return_time: i64,
        _values: &[f64],
        points: &[DataPoint],
    ) -> Vec<DataPoint> {
        self.aggregate(return_time, points)
    }

    /// Whether column extraction pays off for this kernel; aggregators that
    /// only pass points through (first/last/pad/...) skip the extra pass.
    fn wants_columnar(&self) -> bool {
        false
    }

    /// Whether this kernel produces results from the f64 value column
    /// identical to its row form (no value-type preservation, no text).
    /// Gates the zero-materialization Parquet scan path.
    fn columnar_safe(&self) -> bool {
        false
    }
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
    /// True when this stage can consume a (timestamps, values) column pair
    /// directly via [`Aggregator::run_columns`].
    pub fn columnar_capable(&self) -> bool {
        match self {
            Aggregator::Range { exhaustive, sub, .. } => !exhaustive && sub.columnar_safe(),
            Aggregator::Series(_) => false,
        }
    }

    /// Runs a columnar-capable Range stage directly over column slices:
    /// bucket boundaries scan the contiguous timestamp array and kernels
    /// consume value slices — no per-point materialization.
    pub fn run_columns(&self, ctx: &QueryContext, ts: &[i64], vals: &[f64]) -> Vec<DataPoint> {
        let Aggregator::Range {
            sampling,
            align_sampling,
            align_start_time,
            align_end_time,
            sub,
            ..
        } = self
        else {
            unreachable!("run_columns requires columnar_capable()");
        };
        let anchor = if *align_sampling {
            align_range_boundary_tz(ctx.start_ms, sampling.unit, ctx.tz)
        } else {
            ctx.start_ms
        };
        let calc = RangeCalc::new_tz(anchor, *sampling, ctx.tz);
        let mut out = Vec::new();
        let mut start = 0;
        while start < ts.len() {
            let end_range = calc.end_range(ts[start]);
            let mut end = start + 1;
            while end < ts.len() && ts[end] < end_range {
                end += 1;
            }
            let return_time = if *align_start_time {
                calc.start_range(ts[start])
            } else if *align_end_time {
                calc.end_range(ts[start])
            } else {
                ts[start]
            };
            out.extend(sub.aggregate_columnar(return_time, &vals[start..end], &[]));
            start = end;
        }
        out
    }

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
                    align_range_boundary_tz(ctx.start_ms, sampling.unit, ctx.tz)
                } else {
                    ctx.start_ms
                };
                let calc = RangeCalc::new_tz(anchor, *sampling, ctx.tz);
                if *exhaustive {
                    return run_exhaustive(&calc, ctx, sub.as_ref(), *align_start_time, points);
                }
                // Buckets are index ranges over the sorted series; extracting
                // columns once removes the per-point enum branch from every
                // kernel's inner loop (and enables the vector kernels).
                let cols = if sub.wants_columnar() {
                    columnar::extract_values(&points)
                } else {
                    None
                };
                let mut out = Vec::new();
                let mut start = 0;
                while start < points.len() {
                    let end_range = calc.end_range(points[start].timestamp_ms);
                    let mut end = start + 1;
                    while end < points.len() && points[end].timestamp_ms < end_range {
                        end += 1;
                    }
                    // Java getDataPointTime(): first point's timestamp, or
                    // the range start/end when alignment is requested.
                    let first_ts = points[start].timestamp_ms;
                    let return_time = if *align_start_time {
                        calc.start_range(first_ts)
                    } else if *align_end_time {
                        calc.end_range(first_ts)
                    } else {
                        first_ts
                    };
                    match &cols {
                        Some(values) => out.extend(sub.aggregate_columnar(
                            return_time,
                            &values[start..end],
                            &points[start..end],
                        )),
                        None => out.extend(sub.aggregate(return_time, &points[start..end])),
                    }
                    start = end;
                }
                out
            }
        }
    }
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
        tz: kairos_core::time::UTC,
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
            sub: Box::new(SumSub { fast: false }),
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
            sub: Box::new(SumSub { fast: false }),
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
