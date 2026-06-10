# kairosdb-rs

A Rust port of KairosDB, per the plan in
[`docs/RUST_PORT_PROPOSAL.md`](../docs/RUST_PORT_PROPOSAL.md): wire-compatible
with the Java implementation on the outside, idiomatic Rust on the inside,
with commodity-market-data extensions to follow.

## Workspace layout

| Crate | Contents |
|---|---|
| `kairos-core` | Data model (`Value`, `DataPoint`, `DataPointSet`), the zig-zag varint value codec (byte-compatible with `org.kairosdb.util.Util`), time units and the `RangeAggregator` bucketing math |
| `kairos-store` | `Datastore` trait, in-memory backend, the **WAL** (segmented, CRC-checked, checkpointed), the **Cassandra backend** (`scylla` driver, concurrent reads, batched writes) using the Java schema, and the **Parquet cold tier** (time-partitioned columnar files + tiered hot/cold datastore with compaction) |
| `kairos-query` | Range-aggregation engine; all 23 Java aggregators (`sum`, `avg`, `min`, `max`, `count`, `dev`, `percentile`, `first`, `last`, `scale`, `div`, `diff`, `rate`, `sma`, `filter`, `trim`, `pad`, `gaps`, `least_squares`, `sampler`, `score`, `time_diff`, `save_as`); group-bys `tag`, `time`, `value`, `bin`; `order: desc`; wire-compatible query JSON model |
| `kairosd` | Server binary: `/api/v1` REST endpoints (`datapoints` incl. gzip, `datapoints/query`, `datapoints/query/tags`, `datapoints/delete`, `metric/{name}` delete, `metricnames?prefix=`, `health/check`, `health/status`, `features`, `rollups`, `version`) on axum; Telnet ingest (`put`/`putm`/`puts`/`version`, port 4242); durable ingest pipeline (WAL → queue → batched writes, replay on restart); rollup scheduler |

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

## Vectorized aggregation kernels

`kairos-query/src/columnar.rs` provides lane-parallel (auto-vectorizing)
kernels; `cargo run --release -p kairos-query --example agg_bench` measures
them. Pure-kernel throughput over 8M contiguous f64 in this container
(SSE2 baseline — build with `-C target-cpu=native` for AVX2+):

| kernel | strict (Java-identical) | fast (lane-parallel) |
|---|---|---|
| sum | ~700 M vals/s | ~1.2–1.3 G vals/s |
| dev | ~150 M vals/s | ~530–660 M vals/s |
| min/max | — | ~0.9–1.1 G vals/s (exact, always on) |

`KAIROSD_QUERY_MODE=fast` switches `sum`/`avg`/`dev` to the fast kernels;
results differ from Java only by float reassociation (observed < 1e-9
relative; the two-pass `dev` is numerically *better* than the recurrence).
The default `compat` mode stays bit-identical to the Java server — the
regression suite runs against it.

## Parquet cold tier

`KAIROSD_PARQUET_DIR` puts a columnar tier behind the hot store (memory or
Cassandra). Writes land hot; `POST /api/v1/admin/compact`
(`{"older_than_ms": N}`) — or hourly auto-compaction via
`KAIROSD_COMPACT_OLDER_THAN_MS` — moves closed history into one Parquet file
per (metric, 3-week window), sorted by series and timestamp with row-group
statistics. Queries merge tiers transparently; hot wins timestamp
collisions, so late corrections into compacted ranges behave correctly
(compaction tombstones are millisecond-stamped for exactly this reason —
plain Java-style deletes use driver microsecond timestamps that would
shadow later writes, a quirk inherited from the Java implementation and
kept on the public delete API for parity).

Aggregated scans served entirely from the cold tier take the **columnar
fast path**: the Parquet reader produces (timestamps, values) arrays that
stream straight into the vector kernels — rows materialize only after
aggregation collapses the data. Queries that need row semantics (raw
values, `first`/`last` type preservation, `limit`, point-level group-bys)
fall back transparently, and the columnar results are verified equal to the
row path. Compaction also purges fully-emptied row-key index entries (with
millisecond tombstones) so the hot-emptiness check on historical scans
stays cheap.

Measured in this container: a 1.05M-point aggregated historical scan
answers in **~85 ms (≈12 M pts/s)** through the full HTTP stack vs
~1.0–2.1 s from Cassandra; the raw columnar scan runs at ~40 M pts/s. The
data sits in a 7 MB footprint (~7 bytes/point). Text/custom values stay in
the hot store; the cold tier is numeric-only.

Profiling finding (`examples/profile_stages.rs`): with row-form
`Vec<DataPoint>` input the pipeline is bound by point-struct memory traffic,
not kernel math — boxing the rare `Custom` value variant shrank `DataPoint`
48→32 bytes for a ~30% end-to-end win, while converting rows to columns
mid-query costs more than cheap kernels save. The kernels therefore run
where they pay (`dev`-fast, `percentile`) and stand ready for the planned
columnar-at-rest (Arrow/Parquet) tier, where data arrives contiguous and the
G-vals/s rates apply directly.

## Control plane

- **Config file**: TOML via `--config <path>` or `KAIROSD_CONFIG`
  (`[datastore]`, `[parquet]`, `[limits]`, `[rollups]` sections; see
  `kairosd/src/config.rs`); every `KAIROSD_*` env var still works as an
  override.
- **Query guards** (`[limits]`): `max_concurrent_queries` (slot wait bounded
  by the timeout, then 503), `query_timeout_ms` (kills and 503s),
  `max_query_points` (caps raw points scanned, 400). Plus
  `GET /api/v1/runningqueries` and `DELETE /api/v1/killquery/{id}`.
- **Multi-node rollups**: tasks live in the shared `service_index` under the
  Java `_Rollups`/`Config` keys (a Java server on the same cluster sees the
  same task list); nodes refresh on `rollups.refresh_seconds` and gate each
  execution on a lease (`LeasesRs`) that fails over within ~2 execution
  intervals of a node dying. Lease claims are last-write-wins; a rare double
  execution rewrites identical save_as points.
- `GET /metrics`: Prometheus counters (ingest/query/compaction/WAL replay
  totals, query wall time, columnar-path hits) plus uptime.
- `GET /api/v1/health/check` + `/health/status`: liveness for LBs/probes.
- `POST /api/v1/admin/compact`: manual tier compaction;
  `parquet.compact_older_than_ms` for the hourly background loop.
- SIGTERM/SIGINT: graceful shutdown (stop accepting, WAL already fsynced on
  a 100 ms cadence; unflushed ingest replays on restart).
- Still missing: auth/TLS (front with a proxy) and per-query memory caps
  (the point budget is the proxy for that).

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
- Calendar math honors the query-level `time_zone` (IANA names), defaulting
  to UTC; month/year buckets land on local-time boundaries as in Java.

## Not here yet (see the proposal)

The `/api/v1/metadata` service-values API, the `backfill` admin endpoint,
and the `kairos-commodity` crate (reference
data, OHLCV values, curve queries, continuous contracts). Known divergences:
`percentile` is exact instead of reservoir-sampled past 1028 points;
`rate`/`sampler` drop equal-timestamp pairs instead of erroring; descending
queries aggregate ascending and reverse the output; `/features` property
metadata covers common fields rather than every validation annotation.
