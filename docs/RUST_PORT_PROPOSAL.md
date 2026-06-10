# KairosDB → Rust Port & Commodity Market Data Adaptation

A design proposal in two parts: (1) how to port KairosDB to Rust without doing a
line-by-line translation, and (2) what to change so the result is genuinely good
at commodity market data rather than generic ops metrics.

---

## Part 1 — The Rust Port

### 1.1 What we're porting (current architecture)

The Java codebase is ~44K lines (361 main-source files) with a clean separation
of concerns that maps well to Rust:

| Java subsystem | Key types | Rust fate |
|---|---|---|
| Data model | `DataPoint`, `DataPointSet`, `DataPointFactory` | Port as enum + traits |
| Storage abstraction | `Datastore`, `QueryMetric`, `QueryCallback` | Port as async trait |
| Cassandra backend | `CassandraDatastore`, `DataPointsRowKey` | Port (schema-compatible) via `scylla` crate |
| H2 backend | `H2Datastore` + GenORM | **Replace** (embedded engine, see 1.4) |
| Query engine | 30+ `Aggregator`s, 4 `GroupBy`s | Port as streaming combinators |
| REST API | `MetricsResource` (Jersey/Jetty) | Port wire-compatible on `axum` |
| Telnet ingest | `TelnetServer` (`put`, `put_ms`, `put_string`) | Port on `tokio` |
| Ingest queue | `FileQueueProcessor` / BigArray mmap queue | **Replace** with a proper WAL |
| Event bus | `FilterEventBus` + `@Subscribe` | **Replace** with tokio channels |
| DI / plugins | Guice modules, `PluginClassLoader` | **Replace** (see 1.5) |
| Rollups | `RollupJob`, `AssignmentCoordinator` | Port, simplify coordination |
| Backpressure | `MemoryMonitor`, `AdaptiveCongestionController` | Replace with bounded channels |

### 1.2 Guiding principle: wire-compatible outside, idiomatic inside

The single most valuable decision: **keep the REST/Telnet wire protocols and the
Cassandra schema byte-compatible** in phase one. That makes the Rust server a
drop-in replacement that can run side-by-side against the same Cassandra cluster
as the Java version — read the same `data_points` / `row_keys` tables, serve the
same query JSON. Migration becomes "point Grafana at the new port," and every
existing integration test becomes a golden test for the port.

Internally, do *not* mimic Java idioms:

- **`DataPoint` interface → enum.** Java uses trait-object-style polymorphism
  per point, which means a vtable dispatch and heap allocation per datapoint.
  In Rust:

  ```rust
  pub enum Value {
      Long(i64),
      Double(f64),
      Text(Arc<str>),
      Complex(ComplexValue),     // real + imaginary, kept for compat
      Custom { type_id: u16, bytes: Bytes }, // extension escape hatch
  }

  pub struct DataPoint { pub timestamp_ms: i64, pub value: Value }
  ```

  Columnar batches (`Vec<i64>` timestamps + `Vec<f64>` values) for the hot
  aggregation path; the enum only at API boundaries.

- **Aggregators → streaming iterators.** The Java `Aggregator` wraps
  `DataPointGroup` in a pull pipeline. The Rust equivalent is an
  `impl Stream<Item = Batch>` combinator chain — same composability, no
  per-point dispatch, trivially testable.

- **`FilterEventBus` → channels.** The event bus exists to decouple ingest from
  storage. `tokio::sync::mpsc` (bounded) gives the same decoupling plus free,
  correct backpressure — which replaces most of `MemoryMonitor` /
  `AdaptiveCongestionController` (the Java code throttles reactively at 99.9%
  heap; bounded channels prevent the problem instead).

- **BigArray file queue → WAL.** The mmap'd `FileQueueProcessor` is a durability
  buffer. Replace with a simple segmented write-ahead log (append, fsync policy,
  replay on restart). This is ~500 lines of Rust and far easier to reason about
  than the BigArray port.

### 1.3 Crate layout (Cargo workspace)

```
kairosdb-rs/
├── kairos-core        # Value, DataPoint, tags (BTreeMap), QueryMetric, config
├── kairos-query       # aggregators, group-bys, query planner/pipeline
├── kairos-store       # Datastore trait, row-key codec, WAL
├── kairos-store-cassandra  # scylla-driver backend, Java-schema-compatible
├── kairos-store-embedded   # single-node backend (see 1.4)
├── kairos-ingest      # axum REST (wire-compat /api/v1), telnet server
├── kairos-rollup      # rollup scheduler + executor
├── kairos-commodity   # Part 2: curves, calendars, roll logic, domain aggregators
└── kairosd            # binary: wiring, config (HOCON-compatible via `hocon` crate)
```

The `Datastore` trait, mirroring `org.kairosdb.core.datastore.Datastore` but
async and streaming:

```rust
#[async_trait]
pub trait Datastore: Send + Sync {
    async fn write(&self, points: DataPointSet) -> Result<()>;
    async fn query(&self, q: &DatastoreQuery) -> Result<BoxStream<'_, Result<RowBatch>>>;
    async fn delete(&self, q: &DatastoreQuery) -> Result<()>;
    async fn metric_names(&self, prefix: Option<&str>) -> Result<Vec<String>>;
    async fn tag_index(&self, q: &DatastoreQuery) -> Result<TagSet>;
}
```

### 1.4 Storage: keep Cassandra, replace H2, add an analytical tier

- **Cassandra backend:** the `scylla` crate speaks CQL to both ScyllaDB and
  Cassandra. Port `DataPointsRowKey` serialization exactly (metric | cluster |
  3-week row_time | data_type | tags) so the Rust server reads existing data.
  The row-key LRU cache (`DataCache`) ports naturally to `moka` or a hand-rolled
  LRU.

- **H2 replacement:** for the dev/single-node mode, don't port the GenORM layer.
  Use an embedded engine — `redb` or RocksDB for a KV-shaped port, **or** (better,
  see below) Parquet files + DataFusion.

- **The big strategic option — Arrow/Parquet analytical tier.** Rust's killer
  advantage here isn't speed-for-speed's-sake, it's the **Arrow / Parquet /
  DataFusion ecosystem**. Commodity workloads (Part 2) are heavy on historical
  analytics: backtests over years of settlements, curve history scans, vol
  calculations. Proposal: hot recent data lives in the WAL + Cassandra (or an
  in-memory store); a background compactor writes immutable, time-partitioned
  Parquet to local disk or object storage; queries fan out across both tiers and
  merge. DataFusion gives us predicate pushdown, columnar execution, and
  optional SQL on the historical tier essentially for free. This is the one
  place I'd deliberately diverge from the Java architecture rather than port it.

### 1.5 Plugins without a JVM

Java's `PluginClassLoader` (drop a JAR in `plugins/`) has no direct Rust
equivalent — Rust has no stable ABI for dynamic loading. Options, in
recommended order:

1. **Static registration (start here):** aggregators/group-bys register into a
   registry via a small macro; custom builds compose crates. Covers 95% of real
   plugin usage (the in-tree aggregators).
2. **WASM plugins (later, if needed):** host third-party aggregators in
   `wasmtime` with a defined batch-in/batch-out interface. Sandboxed, language-
   agnostic, version-safe — strictly better than JAR loading.
3. Avoid `libloading`/cdylib plugins — ABI fragility makes them a support tax.

The Java `QueryPreProcessor` / `QueryPostProcessor` hooks port directly as
traits in the query pipeline.

### 1.6 Rollups

Port `Rollup` definitions (same JSON, stored in the same `service_index` table
for compat). The Java `AssignmentCoordinator`/`BalancingAlgorithm` cluster
coordination is the most complex, least-load-bearing part of the codebase —
phase one should run rollups on a single designated node (config flag), and
distributed assignment can come later (or be delegated to the existing
Cassandra-based assignment tables, which the Rust version can read).

### 1.7 Phased plan

| Phase | Deliverable | Validation |
|---|---|---|
| 0 | Compat test harness: record/replay HTTP fixtures from the Java server; benchmark baseline | Golden corpus |
| 1 | **Read path**: `kairos-core` + `kairos-query` + Cassandra backend + `/api/v1/datapoints/query` | Rust queries == Java queries on same cluster, byte-for-byte JSON |
| 2 | **Write path**: REST + Telnet ingest, WAL, batch writer | Dual-write, diff the stored rows |
| 3 | Rollups (single-node), delete, metadata APIs, internal metrics (`metrics` crate + Prometheus exporter) | Feature parity checklist |
| 4 | Embedded/Parquet tier, WASM plugins | New capability |
| 5 | `kairos-commodity` (Part 2) | New capability |

Phases 1–3 are roughly the 44K Java lines reduced to an estimated 15–20K lines
of Rust (no DI framework, no ORM, no hand-rolled event bus or congestion
control). Running Rust-read against Java-write from day one keeps the port
honest the whole way.

---

## Part 2 — Commodity Market Data Adaptations

Generic TSDBs assume: one float per timestamp, wall-clock time buckets,
append-only immutable data, and series identity = name + tags. Commodity
data breaks all four assumptions. Here's what to change.

### 2.1 Series identity: contracts, not just tags (convention + reference data)

A commodity price series is `(root, delivery period, venue, ...)` — e.g. CL
(WTI) December 2026 on NYMEX. Model this as a **tag schema convention** enforced
by an ingest profile, not a new storage concept:

```
metric: price.settle
tags:   root=CL, contract=2026-12, exchange=NYMEX,
        currency=USD, unit=bbl, location=cushing   (location/hub matters for gas & power)
```

What KairosDB lacks and we must add is the **reference data service** behind it
(`kairos-commodity`): contract specs, first-notice/expiry dates, tick sizes,
holiday calendars, session times. Every feature below depends on it. Store it
in the existing service-index mechanism or a simple embedded table; expose CRUD
via REST.

### 2.2 Multi-field observations: OHLCV and quote types

A commodity observation is rarely one number — it's settle, open/high/low/close,
volume, open interest, or bid/ask/last. KairosDB's `ComplexDataPoint` (real +
imaginary!) shows the extension point already exists. Add first-class value
types:

```rust
Value::Ohlcv { open: f64, high: f64, low: f64, close: f64,
               volume: f64, open_interest: Option<f64> }
Value::Quote { bid: f64, ask: f64, bid_size: f64, ask_size: f64 }
```

with field-projection in queries (`"field": "close"`) and aggregators that
understand them (an OHLCV bar of OHLCV bars composes correctly: first open, max
high, min low, last close, sum volume). This is dramatically cheaper at query
time than five separate metrics, and keeps a bar atomic.

### 2.3 Bitemporality: settlements get corrected

Exchanges restate settlement prices; vendors send corrections. A risk system
must answer **"what did we believe the Dec-26 settle was at 6pm yesterday?"**
(as-of/observation time) separately from **"what is the settle for Dec-26?"**
(valid time). Plan:

- Ingest corrections as **new versions, never overwrites**: add a
  `revision`/`observed_at` dimension to the stored point (fits in the Cassandra
  value blob or a parallel column).
- Default queries return latest revision; an `"as_of": <timestamp>` query option
  replays beliefs at a point in time.
- An audit endpoint lists revisions for a series/date.

This is the single highest-value commodity feature and the hardest to bolt on
later — design the Rust storage codec for it from day one, even if the API ships
in a later phase.

### 2.4 Time is exchange time: calendars and sessions

Wall-clock `TimeGroupBy` buckets are wrong for markets:

- **Trading-day group-by:** a CME energy "day" ends at the 14:30 ET settlement
  window; electronic sessions span midnight UTC. Add
  `{"name": "trading_day", "calendar": "CME-energy"}` group-by that buckets by
  exchange session, holiday-aware.
- **Session filters:** query options to include only pit hours / electronic /
  settlement window.
- **Gap semantics:** the existing `DataGapsMarkingAggregator` should become
  calendar-aware — no data on Christmas is not a gap; no settle on a trading day
  is.

### 2.5 Forward curves as a first-class query

This is the genuinely new query *shape*. A normal TSDB query is
`f(observation_time) → value` for one series. A **curve snapshot** is: fix an
as-of date, return `f(delivery_month) → value` across all contracts of a root:

```json
{ "curve": { "root": "CL", "field": "settle",
             "as_of": "2026-06-09", "axis": "delivery" } }
```

Implementation: a query-planner extension (KairosDB's `QueryPlugin` /
pre-processor hook ports exist for exactly this) that fans out over the
contract tag, takes last-value-per-contract at as-of, and pivots the axis.
Add: curve *history* queries (a surface: as-of × delivery), simple
interpolation for missing months, and seasonality normalization. Materialize
end-of-day curves via the rollup system so historical curve scans are cheap.

### 2.6 Continuous contracts and rolls

Backtests need a single front-month series stitched from individual contracts:

- **Roll rules** as reference data: fixed days-before-expiry, volume-crossover,
  open-interest-crossover.
- **Adjustment methods:** none, back-adjusted (difference), ratio-adjusted.
- Expose as a virtual series (`root=CL, continuous=c1, roll=oi, adjust=back`)
  resolved at query time by a pre-processor, and optionally materialized via
  rollups for heavy users. `c2`, `c3`… for deferred months.

### 2.7 Domain aggregators

Cheap to add on the ported aggregator framework, high leverage:

| Aggregator | Notes |
|---|---|
| `vwap` / `twap` | needs volume from OHLCV/trade points |
| `ohlc` | resample to bars (the standard chart query) |
| `returns` | simple/log returns; roll-aware on continuous series |
| `realized_vol` | annualized stddev of log returns; configurable window & day-count |
| `spread` | multi-series arithmetic: calendar spreads, crack (3-2-1), spark, dark, basis/location |
| `unit_convert` | $/bbl ↔ $/gal, MWh ↔ therm, bushel ↔ tonne — driven by reference data |
| `fx_convert` | joins an FX rate series at matching timestamps |
| `stale` / `limit_move` | data quality: flag unchanged prices N sessions, limit-up/down days |

The existing `DivideAggregator`/`ScaleAggregator` show the pattern; `spread`
needs the multi-metric join capability that `SaveAsAggregator`-style plumbing
already hints at.

### 2.8 Ingest for market data

- Keep REST/Telnet, add a **Kafka consumer** ingest path (the de facto market
  data bus internally) and a CSV/Parquet **bulk backfill** tool (years of
  vendor history is delivered as files; pushing it through HTTP one JSON array
  at a time is misery).
- Vendor adapters (Bloomberg back office files, ICE/CME EOD reports, Refinitiv)
  as standalone ingest binaries built on `kairos-core` — out of the server, in
  the workspace.
- Tick-rate data (full quote streams) is where the Rust port + WAL + columnar
  tier earns its keep: target ≥1M points/sec/node ingest as a phase-4 benchmark.

### 2.9 Storage shape implications

Commodity data is bimodal:
- **EOD settlements:** tiny (one point/contract/day, maybe ~50K points/day for a
  large universe) but queried over *decades* → ideal for the Parquet tier,
  bitemporal, never expires (don't default TTLs on).
- **Intraday ticks:** huge volume, short hot window, usually downsampled to bars
  after N days → WAL + Cassandra hot tier, rollup to OHLCV bars, expire raw
  ticks via TTL.

The tiered design in 1.4 isn't just a performance idea — it matches the data's
natural lifecycle.

---

## Recommended sequencing

1. **Phases 0–2** of the port (read path, then write path, wire-compatible) —
   proves the Rust core against real data with zero migration risk.
2. **Reference data service + tag conventions + OHLCV value type** — the
   foundation every commodity feature needs.
3. **Bitemporal storage codec** (even before the as-of API) — can't be
   retrofitted cheaply.
4. **Trading calendars + domain aggregators** — fast wins on the ported
   pipeline.
5. **Curve queries + continuous contracts** — the differentiating features.
6. **Parquet/DataFusion tier + bulk backfill + Kafka ingest** — scale-out for
   history and ticks.
