use std::time::Instant;
use kairos_core::DataPoint;

const N: usize = 8_000_000;

fn main() {
    let points: Vec<DataPoint> = (0..N)
        .map(|i| DataPoint::new(i as i64 * 1_000, 50.0 + (i % 7) as f64 * 1.5 + i as f64 * 1e-6))
        .collect();
    println!("sizeof DataPoint = {}", std::mem::size_of::<DataPoint>());

    let t = Instant::now(); let c = points.to_vec();
    println!("clone Vec<DataPoint>      {:>7.1} ms", t.elapsed().as_secs_f64()*1e3);
    drop(c);

    let t = Instant::now();
    let vals = kairos_query::columnar::extract_values(&points).unwrap();
    println!("extract_values            {:>7.1} ms", t.elapsed().as_secs_f64()*1e3);

    let t = Instant::now();
    let mut end = 0usize; let limit = i64::MAX;
    while end < points.len() && points[end].timestamp_ms < limit { end += 1; }
    println!("boundary scan (struct)    {:>7.1} ms (end={end})", t.elapsed().as_secs_f64()*1e3);

    let t = Instant::now();
    let s: f64 = points.iter().filter_map(|p| p.value.as_f64()).sum();
    println!("row filter_map sum        {:>7.1} ms ({s:.3e})", t.elapsed().as_secs_f64()*1e3);

    let t = Instant::now();
    let s2 = kairos_query::columnar::sum_fast(&vals);
    println!("kernel sum_fast           {:>7.1} ms ({s2:.3e})", t.elapsed().as_secs_f64()*1e3);
}
