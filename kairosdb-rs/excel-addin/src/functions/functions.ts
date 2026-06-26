/* global CustomFunctions, fetch */

// Default kairosd base URL. Override per-call with the optional `url` argument,
// or change this constant and rebuild.
const DEFAULT_URL = "http://localhost:8080";

// Excel stores dates as serial numbers (days since 1899-12-30). 25569 is the
// number of days from that epoch to the Unix epoch (1970-01-01).
const EXCEL_EPOCH_OFFSET = 25569;
const MS_PER_DAY = 86_400_000;

/** Coerce an Excel cell value (serial number, Date, or ISO string) to epoch ms. */
function toEpochMs(v: number | string | Date): number {
  if (v instanceof Date) return v.getTime();
  if (typeof v === "number") return Math.round((v - EXCEL_EPOCH_OFFSET) * MS_PER_DAY);
  const t = Date.parse(String(v));
  if (Number.isNaN(t)) throw new Error(`invalid date: ${v}`);
  return t;
}

function epochMsToSerial(ms: number): number {
  return ms / MS_PER_DAY + EXCEL_EPOCH_OFFSET;
}

/** Parse a bucket size like "1d", "5m", "1h" into a server sampling spec. */
function parseSampling(s: string): { value: number; unit: string } | null {
  if (!s) return null;
  const m = /^(\d+)\s*([smhdwMy])$/.exec(s.trim());
  if (!m) throw new Error(`invalid sampling "${s}" (use e.g. 1d, 5m, 1h, 1w, 1M, 1y)`);
  const units: Record<string, string> = {
    s: "seconds",
    m: "minutes",
    h: "hours",
    d: "days",
    w: "weeks",
    M: "months",
    y: "years",
  };
  return { value: parseInt(m[1], 10), unit: units[m[2]] };
}

interface GridResponse {
  rows: [number, number | string][];
  row_count: number;
  sample_size: number;
  elapsed_ms: number;
}

async function gridFetch(base: string, params: Record<string, string>): Promise<GridResponse> {
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(`${base}/api/v1/query/grid?${qs}`);
  if (!res.ok) {
    throw new Error(`kairosd ${res.status}: ${await res.text()}`);
  }
  return (await res.json()) as GridResponse;
}

function buildParams(
  metric: string,
  start: number | string | Date,
  end: number | string | Date,
  aggregator?: string,
  sampling?: string,
  tags?: string
): Record<string, string> {
  const params: Record<string, string> = {
    metric,
    start: String(toEpochMs(start)),
    end: String(toEpochMs(end)),
  };
  if (aggregator) params.aggregator = aggregator;
  const samp = parseSampling(sampling || "");
  if (samp) {
    params.sampling_value = String(samp.value);
    params.sampling_unit = samp.unit;
    params.align_sampling = "false";
  }
  if (tags) params.tags = tags;
  return params;
}

/**
 * Query an aggregated time series from kairosdb-rs and spill it into the grid.
 * The aggregation runs server-side, so the source can be billions of points
 * while only the (small) result returns.
 * @customfunction
 * @param metric Metric name, e.g. "sensor.temp".
 * @param start Start time (a date cell).
 * @param end End time (a date cell).
 * @param aggregator Optional: avg, sum, min, max, count, dev, percentile, ... Omit for raw points.
 * @param sampling Optional bucket size like "1d", "1h", "5m".
 * @param tags Optional tag filter like "host=h001".
 * @param url Optional kairosd base URL (default http://localhost:8080).
 * @returns Two columns: timestamp (Excel date) and value.
 */
export async function query(
  metric: string,
  start: number | string | Date,
  end: number | string | Date,
  aggregator?: string,
  sampling?: string,
  tags?: string,
  url?: string
): Promise<(number | string)[][]> {
  const r = await gridFetch(url || DEFAULT_URL, buildParams(metric, start, end, aggregator, sampling, tags));
  if (!r.rows || r.rows.length === 0) return [["(no data)", ""]];
  // [epoch_ms, value] -> [excel_serial_date, value]. Format column A as a date.
  return r.rows.map((row) => [epochMsToSerial(Number(row[0])), row[1]]);
}

/**
 * The headline status string: how many raw points the query scanned and how
 * long it took — "aggregated N points in X ms".
 * @customfunction
 * @param metric Metric name.
 * @param start Start time.
 * @param end End time.
 * @param aggregator Optional aggregator.
 * @param sampling Optional bucket size like "1d".
 * @param tags Optional tag filter.
 * @param url Optional kairosd base URL.
 * @returns A status string.
 */
export async function info(
  metric: string,
  start: number | string | Date,
  end: number | string | Date,
  aggregator?: string,
  sampling?: string,
  tags?: string,
  url?: string
): Promise<string> {
  const r = await gridFetch(url || DEFAULT_URL, buildParams(metric, start, end, aggregator, sampling, tags));
  return `aggregated ${r.sample_size.toLocaleString()} points into ${r.row_count.toLocaleString()} rows in ${r.elapsed_ms} ms`;
}

/**
 * List metric names (optionally filtered by prefix), spilled as a column.
 * @customfunction
 * @param prefix Optional name prefix filter.
 * @param url Optional kairosd base URL.
 * @returns A column of metric names.
 */
export async function metrics(prefix?: string, url?: string): Promise<string[][]> {
  const base = url || DEFAULT_URL;
  const q = prefix ? `?prefix=${encodeURIComponent(prefix)}` : "";
  const res = await fetch(`${base}/api/v1/metricnames${q}`);
  if (!res.ok) throw new Error(`kairosd ${res.status}`);
  const j = (await res.json()) as { results?: string[] };
  const names = j.results || [];
  return names.length ? names.map((n) => [n]) : [["(no metrics)"]];
}

/** One streaming tick: query the trailing `windowSeconds` ending now. */
async function streamTick(
  base: string,
  metric: string,
  windowSeconds: number,
  aggregator?: string,
  sampling?: string,
  tags?: string
): Promise<(number | string)[][]> {
  const end = Date.now();
  const start = end - windowSeconds * 1000;
  const r = await gridFetch(base, buildParams(metric, start, end, aggregator, sampling, tags));
  if (!r.rows || r.rows.length === 0) return [["(no data)", ""]];
  return r.rows.map((row) => [epochMsToSerial(Number(row[0])), row[1]]);
}

/**
 * Live, scrolling view of a trailing time window. Re-queries every
 * `intervalSeconds` and re-spills `[date, value]` rows, so the window slides
 * forward as new data arrives. Polling, not push: `kairosd` is re-queried each
 * tick.
 * @customfunction
 * @param metric Metric name.
 * @param windowSeconds Width of the trailing window to show.
 * @param aggregator Optional aggregator (e.g. avg). Omit for raw points.
 * @param sampling Optional bucket size like "1s", "10s".
 * @param tags Optional tag filter.
 * @param intervalSeconds Refresh cadence (default 1).
 * @param url Optional kairosd base URL.
 * @param invocation Streaming handle (supplied by Excel).
 * @returns A live-updating two-column range.
 * @streaming
 */
export function stream(
  metric: string,
  windowSeconds: number,
  aggregator: string,
  sampling: string,
  tags: string,
  intervalSeconds: number,
  url: string,
  invocation: CustomFunctions.StreamingInvocation<(number | string)[][]>
): void {
  const base = url || DEFAULT_URL;
  const every = Math.max(1, Math.round(intervalSeconds || 1)) * 1000;
  const tick = () => {
    streamTick(base, metric, windowSeconds, aggregator, sampling, tags)
      .then((rows) => invocation.setResult(rows))
      .catch((e) => invocation.setResult([["error", String(e)]]));
  };
  tick();
  const timer = setInterval(tick, every);
  invocation.onCanceled = () => clearInterval(timer);
}

/**
 * A scrollable viewport: returns `pageSize` rows of the aggregated result
 * starting at `offset`. Bind `offset` to a scrollbar form control to scroll a
 * window over a result far larger than the grid can hold.
 * @customfunction
 * @param metric Metric name.
 * @param start Start time.
 * @param end End time.
 * @param offset First row of the window (0-based).
 * @param pageSize Number of rows to show.
 * @param aggregator Optional aggregator.
 * @param sampling Optional bucket size like "1d".
 * @param tags Optional tag filter.
 * @param url Optional kairosd base URL.
 * @returns A windowed [date, value] range.
 */
export async function page(
  metric: string,
  start: number | string | Date,
  end: number | string | Date,
  offset: number,
  pageSize: number,
  aggregator?: string,
  sampling?: string,
  tags?: string,
  url?: string
): Promise<(number | string)[][]> {
  const params = buildParams(metric, start, end, aggregator, sampling, tags);
  params.offset = String(Math.max(0, Math.round(offset)));
  params.limit = String(Math.max(1, Math.round(pageSize)));
  const r = await gridFetch(url || DEFAULT_URL, params);
  if (!r.rows || r.rows.length === 0) return [["(no data)", ""]];
  return r.rows.map((row) => [epochMsToSerial(Number(row[0])), row[1]]);
}

interface PivotResponse {
  columns: string[];
  rows: (number | string | null)[][];
}

/**
 * Server-side pivot: one column per distinct value of `columnTag`, one row per
 * time bucket. The pivot runs over the full dataset in kairosd; only the matrix
 * spills into Excel. The first returned row is the column header.
 * @customfunction
 * @param metric Metric name.
 * @param start Start time.
 * @param end End time.
 * @param columnTag Tag whose values become columns (e.g. "host").
 * @param aggregator Optional aggregator (e.g. avg). Omit for raw points.
 * @param sampling Optional bucket size like "1d".
 * @param tags Optional extra tag filter.
 * @param url Optional kairosd base URL.
 * @returns A header row followed by a [date, ...values] matrix.
 */
export async function pivot(
  metric: string,
  start: number | string | Date,
  end: number | string | Date,
  columnTag: string,
  aggregator?: string,
  sampling?: string,
  tags?: string,
  url?: string
): Promise<(number | string)[][]> {
  const params = buildParams(metric, start, end, aggregator, sampling, tags);
  params.column_tag = columnTag;
  const qs = new URLSearchParams(params).toString();
  const res = await fetch(`${url || DEFAULT_URL}/api/v1/query/pivot?${qs}`);
  if (!res.ok) throw new Error(`kairosd ${res.status}: ${await res.text()}`);
  const r = (await res.json()) as PivotResponse;
  if (!r.rows || r.rows.length === 0) return [["(no data)"]];
  const header: (number | string)[] = r.columns.map((c) => (c === "timestamp" ? "Time" : c));
  const data: (number | string)[][] = r.rows.map((row) => [
    epochMsToSerial(Number(row[0])),
    ...row.slice(1).map((v) => (v === null ? "" : (v as number | string))),
  ]);
  return [header, ...data];
}

CustomFunctions.associate("QUERY", query);
CustomFunctions.associate("INFO", info);
CustomFunctions.associate("METRICS", metrics);
CustomFunctions.associate("STREAM", stream);
CustomFunctions.associate("PAGE", page);
CustomFunctions.associate("PIVOT", pivot);
