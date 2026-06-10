//! Aggregation micro-benchmark: row pipeline vs columnar (compat) vs
//! columnar fast (vector kernels) over an in-memory series.
//!
//! Run: `cargo run --release -p kairos-query --example agg_bench`
//! For wider vectors: `RUSTFLAGS="-C target-cpu=native" cargo run --release ...`

use std::time::Instant;

use kairos_core::{DataPoint, Sampling, TimeUnit};
use kairos_query::aggregators::{AvgSub, DevSub, MaxSub, MinSub, SumSub};
use kairos_query::{Aggregator, QueryContext, RangeSubAggregator};

const POINTS: usize = 8_000_000;
const RUNS: usize = 5;

fn series() -> Vec<DataPoint> {
    (0..POINTS)
        .map(|i| DataPoint::new(i as i64 * 1_000, 50.0 + (i % 7) as f64 * 1.5 + i as f64 * 1e-6))
        .collect()
}

fn ctx() -> QueryContext {
    QueryContext {
        start_ms: 0,
        end_ms: i64::MAX,
        tz: kairos_core::time::UTC,
        source_metric: "bench".to_string(),
        group_tags: Default::default(),
        save_sink: std::sync::Mutex::new(Vec::new()),
    }
}

fn bench(label: &str, sub: Box<dyn RangeSubAggregator>, points: &[DataPoint]) {
    bench_sampling(label, sub, points, Sampling::new(1, TimeUnit::Hours));
}

fn bench_sampling(
    label: &str,
    sub: Box<dyn RangeSubAggregator>,
    points: &[DataPoint],
    sampling: Sampling,
) {
    let agg = Aggregator::Range {
        sampling,
        align_sampling: false,
        align_start_time: false,
        align_end_time: false,
        exhaustive: false,
        sub,
    };
    let ctx = ctx();
    // warm-up
    let result = agg.run(&ctx, points.to_vec());
    let mut best = f64::MAX;
    for _ in 0..RUNS {
        let input = points.to_vec();
        let start = Instant::now();
        let out = agg.run(&ctx, input);
        let elapsed = start.elapsed().as_secs_f64();
        best = best.min(elapsed);
        assert_eq!(out.len(), result.len());
    }
    println!(
        "{label:22} {:>8.1} M pts/s   ({:>6.1} ms best of {RUNS}, {} buckets)",
        POINTS as f64 / best / 1e6,
        best * 1e3,
        result.len(),
    );
}

/// A row-pipeline reference: same kernel math, but forced through the
/// per-point enum path by a sub-aggregator without a columnar override.
struct RowSum;
impl RangeSubAggregator for RowSum {
    fn aggregate(&self, return_time: i64, points: &[DataPoint]) -> Vec<DataPoint> {
        let sum: f64 = points.iter().filter_map(|p| p.value.as_f64()).sum();
        vec![DataPoint::new(return_time, sum)]
    }
    fn aggregate_columnar(
        &self,
        return_time: i64,
        _vals: &[f64],
        points: &[DataPoint],
    ) -> Vec<DataPoint> {
        self.aggregate(return_time, points) // opt out of the fast path
    }
}

fn bench_kernel(label: &str, vals: &[f64], f: impl Fn(&[f64]) -> f64) {
    let mut best = f64::MAX;
    let mut sink = 0.0;
    for _ in 0..RUNS {
        let start = Instant::now();
        sink += f(vals);
        best = best.min(start.elapsed().as_secs_f64());
    }
    println!(
        "{label:22} {:>8.0} M vals/s   ({:.2} ms, checksum {sink:.3e})",
        vals.len() as f64 / best / 1e6,
        best * 1e3
    );
}

fn main() {
    println!("== pure kernels over {} contiguous f64 ==", POINTS);
    let vals: Vec<f64> = (0..POINTS)
        .map(|i| 50.0 + (i % 7) as f64 * 1.5 + i as f64 * 1e-6)
        .collect();
    bench_kernel("sum strict (seq)", &vals, kairos_query::columnar::sum_strict);
    bench_kernel("sum fast (lanes)", &vals, kairos_query::columnar::sum_fast);
    bench_kernel("dev strict (Java rec.)", &vals, kairos_query::columnar::dev_strict);
    bench_kernel("dev fast (two-pass)", &vals, kairos_query::columnar::dev_fast);
    bench_kernel("min (lanes)", &vals, kairos_query::columnar::min);

    println!("\n== end-to-end pipeline: {} points into 1h buckets ==", POINTS);
    let points = series();

    bench("sum row-pipeline", Box::new(RowSum), &points);
    bench("sum columnar strict", Box::new(SumSub { fast: false }), &points);
    bench("sum columnar fast", Box::new(SumSub { fast: true }), &points);
    println!();
    bench("avg columnar strict", Box::new(AvgSub { fast: false }), &points);
    bench("avg columnar fast", Box::new(AvgSub { fast: true }), &points);
    println!();
    bench("dev strict (Java rec.)", Box::new(DevSub { fast: false }), &points);
    bench("dev columnar fast", Box::new(DevSub { fast: true }), &points);
    println!();
    bench("min columnar (exact)", Box::new(MinSub), &points);
    bench("max columnar (exact)", Box::new(MaxSub), &points);

    // Wide buckets: one giant range, where the kernel is the whole job —
    // the backtest / curve-history scan shape.
    println!("\n== single-bucket scan: {} points into one range ==", POINTS);
    let wide = Sampling::new(100, TimeUnit::Years);
    bench_sampling("sum row-pipeline", Box::new(RowSum), &points, wide);
    bench_sampling("sum columnar strict", Box::new(SumSub { fast: false }), &points, wide);
    bench_sampling("sum columnar fast", Box::new(SumSub { fast: true }), &points, wide);
    bench_sampling("dev strict (Java rec.)", Box::new(DevSub { fast: false }), &points, wide);
    bench_sampling("dev columnar fast", Box::new(DevSub { fast: true }), &points, wide);
    bench_sampling("min columnar (exact)", Box::new(MinSub), &points, wide);
}
