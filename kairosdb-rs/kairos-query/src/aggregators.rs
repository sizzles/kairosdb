//! Built-in aggregators, semantics matched to `org.kairosdb.core.aggregator`.

use kairos_core::{DataPoint, DataPointSet, Value};


use crate::{Aggregator, Error, QueryContext, RangeSubAggregator, Result, SeriesAggregator};
use crate::model::AggregatorSpec;

pub struct SumSub {
    pub fast: bool,
}

impl RangeSubAggregator for SumSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let sum: f64 = points.iter().filter_map(|p| p.value.as_f64()).sum();
        vec![DataPoint::new(return_time, sum)]
    }

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, crate::columnar::sum(values, self.fast))]
    }
    fn columnar_safe(&self) -> bool {
        true
    }
}

pub struct AvgSub {
    pub fast: bool,
}

impl RangeSubAggregator for AvgSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let numeric: Vec<f64> = points.iter().filter_map(|p| p.value.as_f64()).collect();
        if numeric.is_empty() {
            return Vec::new();
        }
        let avg = numeric.iter().sum::<f64>() / numeric.len() as f64;
        vec![DataPoint::new(return_time, avg)]
    }

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        if values.is_empty() {
            return Vec::new();
        }
        let avg = crate::columnar::sum(values, self.fast) / values.len() as f64;
        vec![DataPoint::new(return_time, avg)]
    }
    fn columnar_safe(&self) -> bool {
        true
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

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, crate::columnar::min(values))]
    }
    fn columnar_safe(&self) -> bool {
        true
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

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, crate::columnar::max(values))]
    }
    fn columnar_safe(&self) -> bool {
        true
    }
}

pub struct CountSub;

impl RangeSubAggregator for CountSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, Value::Long(points.len() as i64))]
    }

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, Value::Long(values.len() as i64))]
    }

    fn columnar_safe(&self) -> bool {
        true
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
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
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
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
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
pub struct DevSub {
    pub fast: bool,
}

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

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        vec![DataPoint::new(return_time, crate::columnar::dev(values, self.fast))]
    }

    fn columnar_safe(&self) -> bool {
        true
    }

    fn wants_columnar(&self) -> bool {
        // The strict recurrence is sequential either way; the vectorized
        // two-pass beats row + extraction only in fast mode.
        self.fast
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

    fn aggregate_columnar(
        &self,
        return_time: i64,
        values: &[f64],
        _points: &[DataPoint],
    ) -> Vec<DataPoint> {
        let mut sorted = values.to_vec();
        sorted.sort_by(|a, b| a.partial_cmp(b).expect("non-NaN values"));
        let result = if sorted.is_empty() {
            0.0
        } else {
            let pos = self.percentile * (sorted.len() + 1) as f64;
            if pos < 1.0 {
                sorted[0]
            } else if pos >= sorted.len() as f64 {
                sorted[sorted.len() - 1]
            } else {
                let lower = sorted[pos as usize - 1];
                let upper = sorted[pos as usize];
                lower + (pos - pos.floor()) * (upper - lower)
            }
        };
        vec![DataPoint::new(return_time, result)]
    }

    fn columnar_safe(&self) -> bool {
        true
    }

    fn wants_columnar(&self) -> bool {
        true
    }
}

/// Java `RateAggregator`: per-pair rate `(x1-x0)/(y1-y0)` scaled to the
/// sampling duration, stamped at the later point. The sampling duration is
/// calendar-aware for month/year units, anchored at the earlier timestamp.
pub struct Rate {
    pub sampling: kairos_core::Sampling,
}

impl SeriesAggregator for Rate {
    fn aggregate(&self, ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .windows(2)
            .filter_map(|w| {
                let (x0, x1) = (w[0].value.as_f64()?, w[1].value.as_f64()?);
                let (y0, y1) = (w[0].timestamp_ms, w[1].timestamp_ms);
                if y1 == y0 {
                    // Java throws here; we drop the pair instead.
                    return None;
                }
                let duration = kairos_core::time::add_units_tz(
                    y0, self.sampling.unit, self.sampling.value, ctx.tz) - y0;
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
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
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
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
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
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
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
    fn aggregate(&self, _ctx: &QueryContext, mut points: Vec<DataPoint>) -> Vec<DataPoint> {
        if self.last && !points.is_empty() {
            points.pop();
        }
        if self.first && !points.is_empty() {
            points.remove(0);
        }
        points
    }
}

/// Java `PadAggregator`: exhaustive range aggregator that fills empty ranges
/// with a constant long value and passes data through untouched.
pub struct PadSub {
    pub pad_value: i64,
}

impl RangeSubAggregator for PadSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        if points.is_empty() {
            vec![DataPoint::new(return_time, Value::Long(self.pad_value))]
        } else {
            points.to_vec()
        }
    }
}

/// Java `DataGapsMarkingAggregator`: like `pad`, but empty ranges are marked
/// with a null data point.
pub struct GapsSub;

impl RangeSubAggregator for GapsSub {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        if points.is_empty() {
            vec![DataPoint::new(return_time, Value::Null)]
        } else {
            points.to_vec()
        }
    }
}

/// Java `LeastSquaresAggregator`: per range, ordinary least squares over
/// (timestamp, value); emits the fitted line's endpoints. One or two points
/// pass through unchanged. Uses Commons Math `SimpleRegression`'s
/// mean-centered incremental update — the naive normal equations lose
/// precision catastrophically with millisecond-epoch x values.
pub struct LeastSquaresSub;

impl RangeSubAggregator for LeastSquaresSub {
    fn aggregate(&self, _return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        match points.len() {
            0 => Vec::new(),
            1 | 2 => points.to_vec(),
            _ => {
                let (mut xbar, mut ybar) = (0.0f64, 0.0f64);
                let (mut sum_x, mut sum_y) = (0.0f64, 0.0f64);
                let (mut sum_xx, mut sum_xy) = (0.0f64, 0.0f64);
                let mut n = 0.0f64;
                for p in points {
                    let x = p.timestamp_ms as f64;
                    let y = p.value.as_f64().unwrap_or(0.0);
                    if n == 0.0 {
                        xbar = x;
                        ybar = y;
                    } else {
                        let fact1 = 1.0 + n;
                        let fact2 = n / fact1;
                        let dx = x - xbar;
                        let dy = y - ybar;
                        sum_xx += dx * dx * fact2;
                        sum_xy += dx * dy * fact2;
                        xbar += dx / fact1;
                        ybar += dy / fact1;
                    }
                    n += 1.0;
                    sum_x += x;
                    sum_y += y;
                }
                let slope = sum_xy / sum_xx;
                let intercept = (sum_y - slope * sum_x) / n;
                let predict = |ts: i64| intercept + slope * ts as f64;
                let (start, stop) = (points[0].timestamp_ms, points[points.len() - 1].timestamp_ms);
                vec![
                    DataPoint::new(start, predict(start)),
                    DataPoint::new(stop, predict(stop)),
                ]
            }
        }
    }
}

/// Java `SamplerAggregator`: per pair, the *later* value divided by the
/// elapsed time, scaled to one sampling unit; stamped at the later point.
pub struct Sampler {
    pub sampling: kairos_core::Sampling,
}

impl SeriesAggregator for Sampler {
    fn aggregate(&self, ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .windows(2)
            .filter_map(|w| {
                let x1 = w[1].value.as_f64()?;
                let (y0, y1) = (w[0].timestamp_ms, w[1].timestamp_ms);
                if y1 == y0 {
                    // Java throws here; we drop the pair instead.
                    return None;
                }
                let duration = kairos_core::time::add_units_tz(
                    y0, self.sampling.unit, self.sampling.value, ctx.tz) - y0;
                Some(DataPoint::new(y1, x1 / (y1 - y0) as f64 * duration as f64))
            })
            .collect()
    }
}

/// One threshold of the `score` aggregator. `boundary` decides which side a
/// value exactly on the threshold falls: `superior` scores it below.
#[derive(Debug, Clone, Copy)]
pub struct Threshold {
    pub value: f64,
    pub inferior: bool,
}

impl Threshold {
    /// Java `Threshold.compareValue`.
    fn compare(&self, value: f64) -> i32 {
        if value > self.value {
            1
        } else if value < self.value {
            -1
        } else if self.inferior {
            1
        } else {
            -1
        }
    }
}

/// Java `ScoreAggregator`: maps each value to the index of the first
/// threshold it falls below (0..=n for n thresholds).
pub struct Score {
    pub thresholds: Vec<Threshold>,
    pub descending: bool,
}

impl SeriesAggregator for Score {
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .into_iter()
            .map(|p| {
                let v = p.value.as_f64().unwrap_or(0.0);
                let mut score = self.thresholds.len();
                for (i, threshold) in self.thresholds.iter().enumerate() {
                    if threshold.compare(v) < 0 {
                        score = i;
                        break;
                    }
                }
                if self.descending {
                    score = self.thresholds.len() - score;
                }
                DataPoint::new(p.timestamp_ms, score as f64)
            })
            .collect()
    }
}

/// Java `TimeDiffAggregator`: elapsed time between consecutive points in the
/// given unit (n-1 points, stamped at the later point). Uses the same fixed
/// unit conversion as `TimeGroupBy` (a year is 52 weeks).
pub struct TimeDiff {
    pub divisor: f64,
}

impl SeriesAggregator for TimeDiff {
    fn aggregate(&self, _ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        points
            .windows(2)
            .map(|w| {
                let diff = (w[1].timestamp_ms - w[0].timestamp_ms) as f64 / self.divisor;
                DataPoint::new(w[1].timestamp_ms, diff)
            })
            .collect()
    }
}

/// Java `SaveAsAggregator`: pass-through that also writes the stream as a
/// new metric. Tags = explicit `tags` + the current tag group-by values +
/// `saved_from`. The collected series land in `ctx.save_sink`; the caller
/// writes them through ingest.
pub struct SaveAs {
    pub metric_name: String,
    pub tags: std::collections::BTreeMap<String, String>,
    pub ttl: u32,
}

impl SeriesAggregator for SaveAs {
    fn aggregate(&self, ctx: &QueryContext, points: Vec<DataPoint>) -> Vec<DataPoint> {
        let mut set = DataPointSet::new(&self.metric_name);
        set.ttl = self.ttl;
        set.tags = self.tags.clone();
        for (k, v) in &ctx.group_tags {
            set.tags.insert(k.clone(), v.clone());
        }
        set.tags
            .insert("saved_from".to_string(), ctx.source_metric.clone());
        set.points = points
            .iter()
            .filter(|p| p.value != Value::Null)
            .cloned()
            .collect();
        if !set.points.is_empty() {
            ctx.save_sink.lock().expect("save sink poisoned").push(set);
        }
        points
    }
}

/// Builds an [`Aggregator`] from a parsed wire spec, the analogue of the
/// Guice-registered aggregator factory.
pub fn build(spec: &AggregatorSpec, fast: bool) -> Result<Aggregator> {
    let range_full = |sub: Box<dyn RangeSubAggregator>, exhaustive: bool| -> Result<Aggregator> {
        let sampling = spec
            .sampling
            .ok_or_else(|| Error::MissingSampling(spec.name.clone()))?;
        Ok(Aggregator::Range {
            sampling,
            // Java's RangeAggregator constructs with alignSampling = true.
            align_sampling: spec.align_sampling.unwrap_or(true),
            align_start_time: spec.align_start_time.unwrap_or(false),
            align_end_time: spec.align_end_time.unwrap_or(false),
            exhaustive,
            sub,
        })
    };
    let range = |sub: Box<dyn RangeSubAggregator>| range_full(sub, false);

    let series = |agg: Box<dyn SeriesAggregator>| Ok(Aggregator::Series(agg));

    match spec.name.as_str() {
        "sum" => range(Box::new(SumSub { fast })),
        "avg" => range(Box::new(AvgSub { fast })),
        "min" => range(Box::new(MinSub)),
        "max" => range(Box::new(MaxSub)),
        "count" => range(Box::new(CountSub)),
        "dev" => range(Box::new(DevSub { fast })),
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
        "pad" => range_full(
            Box::new(PadSub {
                pad_value: spec.pad_value.unwrap_or(0),
            }),
            true,
        ),
        "gaps" => range_full(Box::new(GapsSub), true),
        "least_squares" => range(Box::new(LeastSquaresSub)),
        "sampler" => series(Box::new(Sampler {
            sampling: kairos_core::Sampling::new(
                1,
                spec.unit.unwrap_or(kairos_core::TimeUnit::Milliseconds),
            ),
        })),
        "score" => {
            let mut thresholds: Vec<Threshold> = spec
                .thresholds
                .as_ref()
                .filter(|t| !t.is_empty())
                .ok_or_else(|| Error::InvalidQuery("score requires thresholds".into()))?
                .iter()
                .map(|t| Threshold {
                    value: t.value,
                    inferior: t.boundary.as_deref().is_some_and(|b| b.eq_ignore_ascii_case("inferior")),
                })
                .collect();
            // Java ScoreAggregator.setThresholds sorts ascending by value
            // (Threshold.compareTo -> compareValue); the scoring loop then
            // returns the index of the first threshold the value falls below.
            thresholds.sort_by(|a, b| a.value.partial_cmp(&b.value).expect("non-NaN threshold"));
            series(Box::new(Score {
                thresholds,
                descending: spec
                    .order
                    .as_deref()
                    .is_some_and(|o| o.eq_ignore_ascii_case("descending")),
            }))
        }
        "time_diff" => {
            // Java's fixed unit chain: a year is 52 weeks.
            let unit = spec
                .time_unit
                .or(spec.unit)
                .unwrap_or(kairos_core::TimeUnit::Seconds);
            let divisor = crate::model::java_group_size_millis(1, unit) as f64;
            series(Box::new(TimeDiff { divisor }))
        }
        "save_as" => series(Box::new(SaveAs {
            metric_name: spec
                .metric_name
                .clone()
                .filter(|m| !m.is_empty())
                .ok_or_else(|| Error::InvalidQuery("save_as requires metric_name".into()))?,
            tags: spec.tags.clone().unwrap_or_default(),
            ttl: spec.ttl.unwrap_or(0),
        })),
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
            AvgSub { fast: false }.aggregate(1, &points),
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
            Diff.aggregate(&crate::test_context(0, i64::MAX), pts(&[(1, 10.0), (2, 13.0), (3, 11.0)])),
            vec![
                DataPoint::new(2, Value::Double(3.0)),
                DataPoint::new(3, Value::Double(-2.0)),
            ]
        );
    }

    #[test]
    fn dev_is_sample_stddev() {
        let result = DevSub { fast: false }.aggregate(0, &pts(&[(1, 2.0), (2, 4.0), (3, 4.0), (4, 4.0), (5, 5.0), (6, 5.0), (7, 7.0), (8, 9.0)]));
        let Value::Double(dev) = result[0].value else { panic!() };
        assert!((dev - 2.138089935).abs() < 1e-6, "got {dev}");
    }

    #[test]
    fn dev_of_single_point_is_zero() {
        assert_eq!(
            DevSub { fast: false }.aggregate(0, &pts(&[(1, 5.0)])),
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
        let result = agg.aggregate(&crate::test_context(0, i64::MAX), pts(&[(0, 10.0), (500, 15.0)]));
        assert_eq!(result, vec![DataPoint::new(500, Value::Double(10.0))]);
    }

    #[test]
    fn sma_starts_when_window_full() {
        let agg = Sma { size: 3 };
        let result = agg.aggregate(&crate::test_context(0, i64::MAX), pts(&[(1, 1.0), (2, 2.0), (3, 3.0), (4, 4.0)]));
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
        let result = agg.aggregate(&crate::test_context(0, i64::MAX), pts(&[(1, 1.0), (2, 5.0), (3, 3.0)]));
        assert_eq!(result.len(), 2);
        assert_eq!(result[1].value, Value::Double(3.0));
    }

    #[test]
    fn trim_both_drops_endpoints() {
        let agg = Trim { first: true, last: true };
        assert_eq!(
            agg.aggregate(&crate::test_context(0, i64::MAX), pts(&[(1, 1.0), (2, 2.0), (3, 3.0)])),
            vec![DataPoint::new(2, Value::Double(2.0))]
        );
    }

    #[test]
    fn divide_divides() {
        let agg = Divide { divisor: 4.0 };
        assert_eq!(
            agg.aggregate(&crate::test_context(0, i64::MAX), pts(&[(1, 10.0)])),
            vec![DataPoint::new(1, Value::Double(2.5))]
        );
    }
}

#[cfg(test)]
mod new_aggregator_tests {
    use super::*;

    fn pts(values: &[(i64, f64)]) -> Vec<DataPoint> {
        values.iter().map(|(t, v)| DataPoint::new(*t, *v)).collect()
    }

    #[test]
    fn score_maps_values_to_threshold_indexes() {
        let agg = Score {
            thresholds: vec![
                Threshold { value: 10.0, inferior: false },
                Threshold { value: 20.0, inferior: false },
            ],
            descending: false,
        };
        let ctx = crate::test_context(0, 100);
        let result = agg.aggregate(&ctx, pts(&[(1, 5.0), (2, 10.0), (3, 15.0), (4, 25.0)]));
        let scores: Vec<f64> = result.iter().filter_map(|p| p.value.as_f64()).collect();
        // 10.0 with a superior boundary scores below the threshold.
        assert_eq!(scores, vec![0.0, 0.0, 1.0, 2.0]);
    }

    #[test]
    fn score_sorts_thresholds_like_java() {
        // Thresholds supplied OUT of ascending order; Java sorts them, so a
        // value of 15 must score 1 (it sits in [10, 20)), not 0.
        let spec: AggregatorSpec = serde_json::from_value(serde_json::json!({
            "name": "score",
            "thresholds": [{"value": 20.0}, {"value": 10.0}]
        }))
        .unwrap();
        let agg = build(&spec, false).unwrap();
        let ctx = crate::test_context(0, 100);
        let result = agg.run(&ctx, pts(&[(1, 15.0)]));
        assert_eq!(result[0].value, Value::Double(1.0));
    }

    #[test]
    fn time_diff_reports_elapsed_in_unit() {
        let agg = TimeDiff { divisor: 1000.0 }; // seconds
        let ctx = crate::test_context(0, 100);
        let result = agg.aggregate(&ctx, pts(&[(0, 1.0), (2_500, 2.0)]));
        assert_eq!(result, vec![DataPoint::new(2_500, Value::Double(2.5))]);
    }

    #[test]
    fn least_squares_fits_exact_line() {
        // y = 2x + 1 over large epoch-scale x must come back exact.
        let base = 1_765_238_400_000i64;
        let points: Vec<DataPoint> = (0..10)
            .map(|i| DataPoint::new(base + i * 60_000, 2.0 * (base + i * 60_000) as f64 + 1.0))
            .collect();
        let result = LeastSquaresSub.aggregate(0, &points);
        assert_eq!(result.len(), 2);
        for p in &result {
            let expected = 2.0 * p.timestamp_ms as f64 + 1.0;
            let got = p.value.as_f64().unwrap();
            assert!((got - expected).abs() < 1e-3, "got {got}, expected {expected}");
        }
    }

    #[test]
    fn sampler_divides_later_value_by_elapsed() {
        let agg = Sampler {
            sampling: kairos_core::Sampling::new(1, kairos_core::TimeUnit::Seconds),
        };
        let ctx = crate::test_context(0, 100);
        // 30 over 500ms -> 60/second
        let result = agg.aggregate(&ctx, pts(&[(0, 99.0), (500, 30.0)]));
        assert_eq!(result, vec![DataPoint::new(500, Value::Double(60.0))]);
    }

    #[test]
    fn gaps_marks_empty_ranges_with_null() {
        let out = GapsSub.aggregate(50, &[]);
        assert_eq!(out, vec![DataPoint::new(50, Value::Null)]);
        let pass = GapsSub.aggregate(50, &pts(&[(1, 1.0)]));
        assert_eq!(pass.len(), 1);
        assert_eq!(pass[0].value, Value::Double(1.0));
    }

    #[test]
    fn save_as_collects_into_sink_with_group_tags() {
        let agg = SaveAs {
            metric_name: "saved.metric".to_string(),
            tags: Default::default(),
            ttl: 0,
        };
        let mut ctx = crate::test_context(0, 100);
        ctx.source_metric = "src.metric".to_string();
        ctx.group_tags.insert("host".to_string(), "a".to_string());
        let out = agg.aggregate(&ctx, pts(&[(1, 1.0)]));
        assert_eq!(out.len(), 1); // pass-through
        let saved = ctx.save_sink.into_inner().unwrap();
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].name, "saved.metric");
        assert_eq!(saved[0].tags["host"], "a");
        assert_eq!(saved[0].tags["saved_from"], "src.metric");
    }
}
