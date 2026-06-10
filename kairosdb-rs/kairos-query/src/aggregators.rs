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

/// Java `StdAggregator`: sample standard deviation via the same running
/// power-sum recurrence (NaN from a single point collapses to 0).
pub struct DevSub;

impl RangeSubAggregator for DevSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let mut count = 0u32;
        let mut average = 0.0;
        let mut pwr_sum_avg = 0.0;
        let mut std_dev = 0.0;
        for point in points {
            let Some(v) = point.value.as_f64() else { continue };
            count += 1;
            average += (v - average) / count as f64;
            pwr_sum_avg += (v * v - pwr_sum_avg) / count as f64;
            std_dev = ((pwr_sum_avg * count as f64 - count as f64 * average * average)
                / (count as f64 - 1.0))
                .sqrt();
        }
        if std_dev.is_nan() {
            std_dev = 0.0;
        }
        vec![DataPoint::new(return_time, std_dev)]
    }
}

/// Java `PercentileAggregator`: sorted values with the Codahale quantile rule
/// `pos = q * (n + 1)`, linear interpolation between neighbors.
///
/// Divergence: Java samples through a 1028-slot `UniformReservoir`; we rank
/// over all points, which is exact rather than approximate.
pub struct PercentileSub {
    pub percentile: f64,
}

impl RangeSubAggregator for PercentileSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let mut values: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();
        values.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN values"));
        let result = if values.is_empty() {
            0.0
        } else {
            let pos = self.percentile * (values.len() + 1) as f64;
            if pos < 1.0 {
                values[0]
            } else if pos >= values.len() as f64 {
                values[values.len() - 1]
            } else {
                let lower = values[pos as usize - 1];
                let upper = values[pos as usize];
                lower + (pos - pos.floor()) * (upper - lower)
            }
        };
        vec![DataPoint::new(return_time, result)]
    }
}

/// Java `RateAggregator`: per-pair rate `(x1-x0)/(y1-y0)` scaled to the
/// sampling duration, stamped at the later point. The sampling duration is
/// calendar-aware for month/year units, anchored at the earlier timestamp.
pub struct Rate {
    pub sampling: kairos_core::Sampling,
}

impl SeriesAggregator for Rate {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .windows(2)
            .filter_map(|w| {
                let (x0, x1) = (w[0].value.as_f64()?, w[1].value.as_f64()?);
                let (y0, y1) = (w[0].timestamp_ms, w[1].timestamp_ms);
                if y1 == y0 {
                    // Java throws here; we drop the pair instead.
                    return None;
                }
                let duration =
                    kairos_core::time::add_units(y0, self.sampling.unit, self.sampling.value) - y0;
                let rate = (x1 - x0) / (y1 - y0) as f64 * duration as f64;
                Some(DataPoint::new(y1, rate))
            })
            .collect()
    }
}

/// Java `SmaAggregator`: simple moving average over the last `size` points;
/// output starts once the window is full, stamped at each point's own time.
pub struct Sma {
    pub size: usize,
}

impl SeriesAggregator for Sma {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        if points.len() < self.size || self.size == 0 {
            return Vec::new();
        }
        points
            .windows(self.size)
            .map(|window| {
                let sum: f64 = window.iter().filter_map(|p| p.value.as_f64()).sum();
                DataPoint::new(
                    window.last().expect("window is non-empty").timestamp_ms,
                    sum / window.len() as f64,
                )
            })
            .collect()
    }
}

/// Java `DivideAggregator`: divide every point by a constant.
pub struct Divide {
    pub divisor: f64,
}

impl SeriesAggregator for Divide {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .into_iter()
            .map(|p| match p.value.as_f64() {
                Some(v) => DataPoint::new(p.timestamp_ms, v / self.divisor),
                None => p,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterOp {
    Lte,
    Lt,
    Gte,
    Gt,
    Equal,
    Ne,
}

impl FilterOp {
    pub fn parse(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "lte" => Ok(FilterOp::Lte),
            "lt" => Ok(FilterOp::Lt),
            "gte" => Ok(FilterOp::Gte),
            "gt" => Ok(FilterOp::Gt),
            "equal" => Ok(FilterOp::Equal),
            "ne" => Ok(FilterOp::Ne),
            other => Err(Error::InvalidQuery(format!("unknown filter_op: {other}"))),
        }
    }
}

/// Java `FilterAggregator`: removes points whose value matches
/// `value <op> threshold`.
pub struct Filter {
    pub op: FilterOp,
    pub threshold: f64,
}

impl SeriesAggregator for Filter {
    fn aggregate(&self, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .into_iter()
            .filter(|p| {
                let Some(v) = p.value.as_f64() else { return true };
                let matches = match self.op {
                    FilterOp::Lte => v <= self.threshold,
                    FilterOp::Lt => v < self.threshold,
                    FilterOp::Gte => v >= self.threshold,
                    FilterOp::Gt => v > self.threshold,
                    FilterOp::Equal => v == self.threshold,
                    FilterOp::Ne => v != self.threshold,
                };
                !matches
            })
            .collect()
    }
}

/// Java `TrimAggregator`: drop the first point, the last, or both.
pub struct Trim {
    pub first: bool,
    pub last: bool,
}

impl SeriesAggregator for Trim {
    fn aggregate(&self, mut points: Vec<DataPoint>) -> Vec<DataPoint> {
        if self.last && !points.is_empty() {
            points.pop();
        }
        if self.first && !points.is_empty() {
            points.remove(0);
        }
        points
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

    let series = |agg: Box<dyn SeriesAggregator>| Ok(Aggregator::Series(agg));

    match spec.name.as_str() {
        "sum" => range(Box::new(SumSub)),
        "avg" => range(Box::new(AvgSub)),
        "min" => range(Box::new(MinSub)),
        "max" => range(Box::new(MaxSub)),
        "count" => range(Box::new(CountSub)),
        "dev" => range(Box::new(DevSub)),
        "percentile" => range(Box::new(PercentileSub {
            percentile: spec
                .percentile
                .ok_or_else(|| Error::InvalidQuery("percentile requires a percentile".into()))?,
        })),
        "first" => range(Box::new(FirstSub)),
        "last" => range(Box::new(LastSub {
            aligned: spec.align_start_time.unwrap_or(false)
                || spec.align_end_time.unwrap_or(false),
        })),
        "scale" => series(Box::new(Scale {
            factor: spec
                .factor
                .ok_or_else(|| Error::InvalidQuery("scale requires a factor".into()))?,
        })),
        "div" => series(Box::new(Divide {
            divisor: match spec.divisor {
                Some(d) if d != 0.0 => d,
                _ => return Err(Error::InvalidQuery("div requires a non-zero divisor".into())),
            },
        })),
        "diff" => series(Box::new(Diff)),
        "rate" => series(Box::new(Rate {
            // Java accepts either a full sampling or a bare unit.
            sampling: spec.sampling.or_else(|| {
                spec.unit.map(|unit| kairos_core::Sampling::new(1, unit))
            })
            .unwrap_or(kairos_core::Sampling::new(1, kairos_core::TimeUnit::Milliseconds)),
        })),
        "sma" => series(Box::new(Sma {
            size: match spec.size {
                Some(s) if s > 0 => s as usize,
                _ => return Err(Error::InvalidQuery("sma requires a positive size".into())),
            },
        })),
        "filter" => series(Box::new(Filter {
            op: FilterOp::parse(
                spec.filter_op
                    .as_deref()
                    .ok_or_else(|| Error::InvalidQuery("filter requires filter_op".into()))?,
            )?,
            threshold: spec.threshold.unwrap_or(0.0),
        })),
        "trim" => {
            let which = spec.trim.as_deref().unwrap_or("both").to_ascii_lowercase();
            series(Box::new(Trim {
                first: which == "first" || which == "both",
                last: which == "last" || which == "both",
            }))
        }
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

    #[test]
    fn dev_is_sample_stddev() {
        let result = DevSub.aggregate(0, &pts(&[(1, 2.0), (2, 4.0), (3, 4.0), (4, 4.0), (5, 5.0), (6, 5.0), (7, 7.0), (8, 9.0)]));
        let Value::Double(dev) = result[0].value else { panic!() };
        assert!((dev - 2.138089935).abs() < 1e-6, "got {dev}");
    }

    #[test]
    fn dev_of_single_point_is_zero() {
        assert_eq!(
            DevSub.aggregate(0, &pts(&[(1, 5.0)])),
            vec![DataPoint::new(0, Value::Double(0.0))]
        );
    }

    #[test]
    fn percentile_uses_n_plus_one_position() {
        let agg = PercentileSub { percentile: 0.5 };
        // pos = 0.5 * 5 = 2.5 -> values[1] + 0.5*(values[2]-values[1])
        let result = agg.aggregate(0, &pts(&[(1, 10.0), (2, 20.0), (3, 30.0), (4, 40.0)]));
        assert_eq!(result[0].value, Value::Double(25.0));
    }

    #[test]
    fn rate_scales_to_sampling_duration() {
        let agg = Rate {
            sampling: kairos_core::Sampling::new(1, kairos_core::TimeUnit::Seconds),
        };
        // +5 over 500ms = 10/second
        let result = agg.aggregate(pts(&[(0, 10.0), (500, 15.0)]));
        assert_eq!(result, vec![DataPoint::new(500, Value::Double(10.0))]);
    }

    #[test]
    fn sma_starts_when_window_full() {
        let agg = Sma { size: 3 };
        let result = agg.aggregate(pts(&[(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]));
        assert_eq!(
            result,
            vec![
                DataPoint::new(3, Value::Double(2.0)),
                DataPoint::new(4, Value::Double(3.0)),
            ]
        );
    }

    #[test]
    fn filter_removes_matching_points() {
        let agg = Filter { op: FilterOp::Gt, threshold: 3.0 };
        let result = agg.aggregate(pts(&[(1, 1.0), (2, 5.0), (3, 3.0)]));
        assert_eq!(result.len(), 2);
        assert_eq!(result[1].value, Value::Double(3.0));
    }

    #[test]
    fn trim_both_drops_endpoints() {
        let agg = Trim { first: true, last: true };
        assert_eq!(
            agg.aggregate(pts(&[(1, 1.0), (2, 2.0), (3, 3.0)])),
            vec![DataPoint::new(2, Value::Double(2.0))]
        );
    }

    #[test]
    fn divide_divides() {
        let agg = Divide { divisor: 4.0 };
        assert_eq!(
            agg.aggregate(pts(&[(1, 10.0)])),
            vec![DataPoint::new(1, Value::Double(2.5))]
        );
    }
}
