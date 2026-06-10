# kairosdb-rs

A Rust port of KairosDB, per the plan in
[`docs/RUST_PORT_PROPOSAL.md`](../docs/RUST_PORT_PROPOSAL.md): wire-compatible
with the Java implementation on the outside, idiomatic Rust on the inside,
with commodity-market-data extensions to follow.

## Workspace layout

| Crate | Contents |
|---|---|
| `kairos-core` | Data model (`Value`, `DataPoint`, `DataPointSet`), the zig-zag varint value codec (byte-compatible with `org.kairosdb.util.Util`), time units and the `RangeAggregator` bucketing math |
| `kairos-store` | `Datastore` trait, in-memory backend, and the Cassandra storage codec (`DataPointsRowKey` serializer and `RowSpec` column-time encoding, byte-compatible with the Java schema) |
| `kairos-query` | Range-aggregation engine, built-in aggregators (`sum`, `avg`, `min`, `max`, `count`, `first`, `last`, `scale`, `diff`), tag group-by, and the wire-compatible query JSON model |
| `kairosd` | Server binary: `/api/v1` REST endpoints (`datapoints`, `datapoints/query`, `metricnames`, `version`) on axum |

## Running

```sh
cargo run -p kairosd          # listens on 0.0.0.0:8080 (KAIROSD_LISTEN to override)
cargo test --workspace
```

Ingest and query use the same JSON as the Java server:

```sh
curl -X POST localhost:8080/api/v1/datapoints -d '[{
  "name": "price.settle",
  "tags": {"root": "CL", "contract": "2026-12", "exchange": "NYMEX"},
  "datapoints": [[1765238400000, 61.42]]
}]'

curl -X POST localhost:8080/api/v1/datapoints/query -d '{
  "start_absolute": 1765238400000,
  "metrics": [{
    "name": "price.settle",
    "aggregators": [{"name": "last", "sampling": {"value": 1, "unit": "days"}}],
    "group_by": [{"name": "tag", "tags": ["contract"]}]
  }]
}'
```

## Compatibility notes

- The varint/double/string value encodings and the Cassandra row-key bytes are
  matched to the Java sources line-by-line, with golden-byte tests.
- Range bucketing replicates `RangeAggregator` exactly, including the
  `align_sampling` day-boundary fallthrough quirk and `align_sampling`
  defaulting to true.
- Calendar math is UTC-only for now; per-query time zones are a TODO.

## Not here yet (see the proposal)

Cassandra driver integration (`scylla`), Telnet ingest, WAL-backed ingest
queue, rollups, remaining aggregators/group-bys, and the `kairos-commodity`
crate (reference data, OHLCV values, curve queries, continuous contracts).
