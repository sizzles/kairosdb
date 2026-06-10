use std::time::Instant;
use kairos_store::parquet_store::ParquetStore;
use kairos_store::DatastoreQuery;

fn main() {
    let store = ParquetStore::open("/tmp/kairos-parquet").unwrap();
    let q = DatastoreQuery {
        metric: "hist3.settle".into(),
        start_time_ms: 0,
        end_time_ms: i64::MAX / 2,
        tags: Default::default(),
        limit: None,
        descending: false,
    };
    let cols = store.scan_columns(&q).unwrap();
    let n: usize = cols.iter().map(|c| c.timestamps.len()).sum();
    let mut best = f64::MAX;
    for _ in 0..5 {
        let t = Instant::now();
        let cols = store.scan_columns(&q).unwrap();
        best = best.min(t.elapsed().as_secs_f64());
        assert_eq!(cols.iter().map(|c| c.timestamps.len()).sum::<usize>(), n);
    }
    println!("scan_columns: {n} pts in {:.0} ms ({:.1} M pts/s)", best*1e3, n as f64/best/1e6);

    let t = Instant::now();
    let rows = store.query(&q).unwrap();
    let rn: usize = rows.iter().map(|s| s.points.len()).sum();
    println!("row query:    {rn} pts in {:.0} ms", t.elapsed().as_secs_f64()*1e3);
}
