# KairosDB-rs → Excel

A showcase: drive billion-point time-series aggregations from Excel formulas.
You type `=KAIROS.QUERY("sensor.temp", DATE(2020,1,1), TODAY(), "avg", "1d")`,
`kairosd` scans the raw points server-side and returns the (small) aggregated
series, which **spills** into the sheet. Change the bucket from `1d` to `1h`
and Excel re-fires the formula and the chart redraws.

The point: **Excel as a thin client over a fast analytical backend.** The grid
caps at ~1M rows, but because the aggregation happens in the server you can
explore datasets that are orders of magnitude larger — only the answer comes
back. The status line `=KAIROS.INFO(...)` shows the headline:
*"aggregated 216,000,000 points into 365 rows in 94 ms."*

## Functions

| Function | Result |
|---|---|
| `=KAIROS.QUERY(metric, start, end, [agg], [bucket], [tags], [url])` | Spills `[date, value]` rows for the aggregated series |
| `=KAIROS.INFO(metric, start, end, [agg], [bucket], [tags], [url])` | `"aggregated N points into R rows in X ms"` |
| `=KAIROS.METRICS([prefix], [url])` | Spills a column of metric names |

- `start`/`end` are date cells (e.g. `DATE(2020,1,1)`, `TODAY()`).
- `agg`: `avg`, `sum`, `min`, `max`, `count`, `dev`, `percentile`, … (omit for raw points).
- `bucket`: `1d`, `1h`, `5m`, `1w`, `1M`, `1y`.
- `tags`: `host=h001` or `host=h001,dc=lga`.
- `url`: optional `kairosd` base URL (default `http://localhost:8080`).
- Format the timestamp column as **Date/Time** (the function returns Excel date serials).

## What's verified vs. not

This repo's CI cannot run Excel, so:

- ✅ **Server side** (`kairosd`): the `GET /api/v1/query/grid` endpoint and CORS
  are implemented and unit-tested in the Rust workspace.
- ✅ **Function logic** (`src/functions/functions.ts`): exercised end-to-end
  against a live `kairosd` (date conversion → URL → fetch → response shaping)
  — `query`, `info`, `metrics`, and tag filtering all confirmed.
- ⚠️ **Excel runtime glue** (`manifest.xml`, webpack build, custom-functions
  registration): standard Office tooling that was **not run here**. If the
  manifest doesn't validate in your Office build, use the generator path below
  (it produces the same project around our verified `functions.ts`).

## Run it

### 1. Start `kairosd` with some data

```sh
# from kairosdb-rs/ — memory backend is fine for the demo
cargo run --release -p kairosd        # listens on :8080

# load a big synthetic dataset (≈26M points: 50 series × 1 yr × 1/min)
python3 excel-addin/seed_demo.py --series 50 --days 365 --interval-sec 60
```

The seed script prints the same numbers Excel will show, e.g.
`scanned 26,280,000 points -> 365 rows in 88 ms`.

> For the biggest "wow" number, run `kairosd` with the Parquet cold tier and
> compact, so the scan hits the columnar SIMD path:
> `KAIROSD_PARQUET_DIR=./pq cargo run --release -p kairosd`, then
> `curl -XPOST localhost:8080/api/v1/admin/compact -d '{}'`.

### 2. Build and sideload the add-in

```sh
cd excel-addin
npm install
npm start            # provisions a dev cert, builds, opens Excel, sideloads
```

`npm start` serves the bundle on `https://localhost:3000` and sideloads
`manifest.xml`. In the sheet, type `=KAIROS.METRICS("sensor")` to confirm the
connection, then `=KAIROS.QUERY(...)`.

### Reliable fallback (if the manifest won't validate)

```sh
npm create office-addin@latest          # choose: Excel Custom Functions, TypeScript, Shared Runtime
# then in the generated project:
#   - replace src/functions/functions.ts with this repo's version
#   - set the namespace to KAIROS in manifest.xml
npm start
```

## Demo workbook layout

A one-screen demo:

| Cell | Contents |
|---|---|
| `B1` | metric, e.g. `sensor.temp` |
| `B2` | `=DATE(2020,1,1)` |
| `B3` | `=TODAY()` |
| `B4` | bucket, e.g. `1d` |
| `B6` | `=KAIROS.INFO(B1,B2,B3,"avg",B4)` → the headline status line |
| `A9` | `=KAIROS.QUERY(B1,B2,B3,"avg",B4)` → spills `[date, value]` down/right |

Insert a line chart over the `A9#` spill range. Change `B4` from `1d` to `1h`
to `5m` and watch the server re-aggregate and the chart redraw — live, over a
dataset far too big for the grid itself.

## Security note

The grid endpoint ships with permissive CORS for local demos. `kairosd` has no
auth/TLS of its own — keep it on localhost or behind an authenticating proxy,
and restrict the CORS origin before exposing it anywhere real.
