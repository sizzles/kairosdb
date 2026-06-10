//! Columnar (struct-of-arrays) aggregation kernels.
//!
//! The row pipeline stores `Vec<DataPoint>` with an enum value per point;
//! extracting timestamps and values into parallel arrays once per series
//! removes the per-point enum branch from every aggregator's inner loop and
//! gives LLVM lane-parallel loops it can auto-vectorize (SSE2 at the default
//! x86-64 baseline; AVX2/AVX-512 with `-C target-cpu=native`).
//!
//! Two kernel flavors:
//! - *strict*: sequential accumulation, bit-identical to the Java server
//!   (and to our scalar path). `min`/`max`/`count`/`percentile` are exact in
//!   either flavor, so they always use the lane-parallel shape.
//! - *fast*: lane-parallel accumulation for `sum`/`avg`/`dev`. Reassociating
//!   float adds changes rounding in the last ulp, so this is opt-in
//!   (`KAIROSD_QUERY_MODE=fast`).

use kairos_core::{DataPoint, Value};

/// Lane count for manually unrolled kernels. Eight f64 lanes map to one
/// AVX-512 register, two AVX2 registers, or four SSE2 registers.
const LANES: usize = 8;

/// Extracts the value column when every point is numeric (the common case);
/// any text/custom/null point disables the columnar path for the series.
/// Indices line up with the originating `&[DataPoint]` slice.
pub fn extract_values(points: &[DataPoint]) -> Option<Vec<f64>> {
    let mut values = Vec::with_capacity(points.len());
    for point in points {
        match point.value {
            Value::Long(v) => values.push(v as f64),
            Value::Double(v) => values.push(v),
            _ => return None,
        }
    }
    Some(values)
}

/// Sequential sum, same order as the scalar pipeline and Java.
pub fn sum_strict(vals: &[f64]) -> f64 {
    vals.iter().sum()
}

/// Lane-parallel sum: 8 independent accumulators auto-vectorize, at the cost
/// of a different (typically *more* accurate) rounding than sequential.
pub fn sum_fast(vals: &[f64]) -> f64 {
    let mut acc = [0.0f64; LANES];
    let chunks = vals.chunks_exact(LANES);
    let remainder = chunks.remainder();
    for chunk in chunks {
        for i in 0..LANES {
            acc[i] += chunk[i];
        }
    }
    let mut total = remainder.iter().sum::<f64>();
    for lane in acc {
        total += lane;
    }
    total
}

pub fn sum(vals: &[f64], fast: bool) -> f64 {
    if fast {
        sum_fast(vals)
    } else {
        sum_strict(vals)
    }
}

/// Lane-parallel min: exact for non-NaN inputs regardless of order, so it is
/// always safe to vectorize.
pub fn min(vals: &[f64]) -> f64 {
    let mut acc = [f64::MAX; LANES];
    let chunks = vals.chunks_exact(LANES);
    let remainder = chunks.remainder();
    for chunk in chunks {
        for i in 0..LANES {
            acc[i] = acc[i].min(chunk[i]);
        }
    }
    let mut result = f64::MAX;
    for lane in acc {
        result = result.min(lane);
    }
    for v in remainder {
        result = result.min(*v);
    }
    result
}

pub fn max(vals: &[f64]) -> f64 {
    let mut acc = [f64::MIN; LANES];
    let chunks = vals.chunks_exact(LANES);
    let remainder = chunks.remainder();
    for chunk in chunks {
        for i in 0..LANES {
            acc[i] = acc[i].max(chunk[i]);
        }
    }
    let mut result = f64::MIN;
    for lane in acc {
        result = result.max(lane);
    }
    for v in remainder {
        result = result.max(*v);
    }
    result
}

/// Strict standard deviation: the exact Java `StdAggregator` running
/// power-sum recurrence (inherently sequential).
pub fn dev_strict(vals: &[f64]) -> f64 {
    let mut count = 0u64;
    let mut average = 0.0;
    let mut pwr_sum_avg = 0.0;
    let mut std_dev = 0.0;
    for v in vals {
        count += 1;
        average += (v - average) / count as f64;
        pwr_sum_avg += (v * v - pwr_sum_avg) / count as f64;
        std_dev = ((pwr_sum_avg * count as f64 - count as f64 * average * average)
            / (count as f64 - 1.0))
            .sqrt();
    }
    if std_dev.is_nan() {
        0.0
    } else {
        std_dev
    }
}

/// Fast standard deviation: vectorizable two-pass (mean, then centered
/// squares) — numerically *better* than the recurrence, but not bit-equal.
pub fn dev_fast(vals: &[f64]) -> f64 {
    if vals.len() < 2 {
        return 0.0;
    }
    let n = vals.len() as f64;
    let mean = sum_fast(vals) / n;

    let mut acc = [0.0f64; LANES];
    let chunks = vals.chunks_exact(LANES);
    let remainder = chunks.remainder();
    for chunk in chunks {
        for i in 0..LANES {
            let d = chunk[i] - mean;
            acc[i] += d * d;
        }
    }
    let mut ss = 0.0;
    for lane in acc {
        ss += lane;
    }
    for v in remainder {
        let d = v - mean;
        ss += d * d;
    }
    let result = (ss / (n - 1.0)).sqrt();
    if result.is_nan() {
        0.0
    } else {
        result
    }
}

pub fn dev(vals: &[f64], fast: bool) -> f64 {
    if fast {
        dev_fast(vals)
    } else {
        dev_strict(vals)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn series(n: usize) -> Vec<f64> {
        (0..n).map(|i| 50.0 + (i % 7) as f64 * 1.5 + i as f64 * 0.01).collect()
    }

    #[test]
    fn strict_sum_matches_sequential_fold() {
        let vals = series(1003);
        let sequential: f64 = vals.iter().sum();
        assert_eq!(sum_strict(&vals), sequential);
    }

    #[test]
    fn fast_sum_within_tolerance_of_strict() {
        let vals = series(100_003);
        let strict = sum_strict(&vals);
        let fast = sum_fast(&vals);
        assert!((strict - fast).abs() / strict.abs() < 1e-12, "{strict} vs {fast}");
    }

    #[test]
    fn min_max_are_exact_in_any_order() {
        let vals = series(1003);
        let expected_min = vals.iter().cloned().fold(f64::MAX, f64::min);
        let expected_max = vals.iter().cloned().fold(f64::MIN, f64::max);
        assert_eq!(min(&vals), expected_min);
        assert_eq!(max(&vals), expected_max);
        // Short series exercise the remainder-only path.
        assert_eq!(min(&vals[..3]), vals[..3].iter().cloned().fold(f64::MAX, f64::min));
    }

    #[test]
    fn dev_strict_matches_java_recurrence_and_fast_is_close() {
        let vals = series(10_007);
        let strict = dev_strict(&vals);
        let fast = dev_fast(&vals);
        assert!((strict - fast).abs() / strict < 1e-9, "{strict} vs {fast}");
        assert_eq!(dev_strict(&[5.0]), 0.0);
        assert_eq!(dev_fast(&[5.0]), 0.0);
    }

    #[test]
    fn extract_rejects_non_numeric_series() {
        use kairos_core::DataPoint;
        let mut points = vec![DataPoint::new(1, 1.5), DataPoint::new(2, 7i64)];
        assert_eq!(extract_values(&points).unwrap(), vec![1.5, 7.0]);
        points.push(DataPoint::new(3, "text"));
        assert!(extract_values(&points).is_none());
    }
}
