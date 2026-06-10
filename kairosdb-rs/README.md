# kairosdb-rs

A Rust port of KairosDB, per the plan in
[`docs/RUST_PORT_PROPOSAL.md`](../docs/RUST_PORT_PROPOSAL.md): wire-compatible
with the Java implementation on the outside, idiomatic Rust on the inside,
with commodity-market-data extensions to follow.

## Workspace layout

| Crate | Contents |
|---|---|
| `kairos-core` | Data model (`Value`, `DataPoint`, `DataPointSet`), the zig-zag varint value codec (byte-compatible with `org.kairosdb.util.Util`), time units and the `RangeAggregator` bucketing math |
| `kairos-store` | `Datastore` trait, in-memory backend, the **WAL** (segmented, CRC-checked, checkpointed), and the **Cassandra backend** (`scylla` driver, concurrent reads, batched writes) using the Java schema: `data_points`, `row_keys`, `row_key_time_index`, `string_index`, `spec` |
| `kairos-query` | Range-aggregation engine; all 23 Java aggregators (`sum`, `avg`, `min`, `max`, `count`, `dev`, `percentile`, `first`, `last`, `scale`, `div`, `diff`, `rate`, `sma`, `filter`, `trim`, `pad`, `gaps`, `least_squares`, `sampler`, `score`, `time_diff`, `save_as`); group-bys `tag`, `time`, `value`, `bin`; `order: desc`; wire-compatible query JSON model |
| `kairosd` | Server binary: `/api/v1` REST endpoints (`datapoints` incl. gzip, `datapoints/query`, `datapoints/query/tags`, `datapoints/delete`, `metric/{name}` delete, `metricnames?prefix=`, `health/check`, `health/status`, `rollups`, `version`) on axum; durable ingest pipeline (WAL → queue → batched writes, replay on restart); rollup scheduler |

## Verified storage-level interop with Java KairosDB

The Cassandra backend has been cross-validated against the Java
implementation (1.4.0-SNAPSHOT) running on the same Cassandra 4.1 keyspace:

- data written through the Java server reads back identically through
  `kairosd`, and vice versa;
- the same aggregated, tag-grouped query returns identical results from both
  servers.

Row-key blobs, column-time encoding (legacy and modern), value encodings, and
the `spec`-table row-format negotiation all match `ClusterConnection` /
`CQLBatch` semantics. The full aggregator set, `time` group-by output,
descending order with limit, `query/tags`, `save_as` write-back, and
cross-server deletes are verified against the Java server by the
[regression suite](regression/README.md) — run it any time both servers
share a Cassandra keyspace.

## Performance (same box, same single-node Cassandra 4.1, 100k points)

| | Java 1.4.0-SNAPSHOT | kairosd (release) |
|---|---|---|
| Ingest ack rate | ~80k pts/s | ~510k pts/s (WAL-durable) |
| Ingest → queryable | ~40–50k pts/s | ~75–90k pts/s |
| Query 100k points (avg to 1h buckets) | ~105–115 ms | ~80–95 ms |

Rough single-run numbers from this repo's dev container, not a tuned
benchmark; the end-to-end drain is bottlenecked by the shared Cassandra node.

## Running

```sh
cargo run -p kairosd          # in-memory backend, listens on 0.0.0.0:8080
KAIROSD_DATASTORE=cassandra KAIROSD_CASSANDRA_NODE=127.0.0.1:9042 \
  KAIROSD_CASSANDRA_KEYSPACE=kairosdb cargo run -p kairosd

cargo test --workspace        # unit tests
KAIROS_TEST_CASSANDRA=127.0.0.1:9042 cargo test --workspace   # + live integration test
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

Telnet ingest, per-query time zones (calendar math is UTC-only), the
`/api/v1/features` and metadata/service APIs, distributed rollup assignment
(rollups are single-node), legacy (pre-1.1) value decoding, and the
`kairos-commodity` crate (reference data, OHLCV values, curve queries,
continuous contracts). Known divergences: `percentile` is exact instead of
reservoir-sampled past 1028 points; `rate`/`sampler` drop equal-timestamp
pairs instead of erroring; descending queries aggregate ascending and
reverse the output.
