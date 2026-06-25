#!/usr/bin/env python3
"""Seed a running kairosd with a large synthetic dataset so the Excel demo's
"aggregated N points in X ms" number is real.

Usage:
  python3 seed_demo.py --url http://127.0.0.1:8080 --metric sensor.temp \
      --series 50 --days 365 --interval-sec 60

That example writes 50 series x 1 point/minute x 365 days ~= 26M points.
Each series gets a distinct `host` tag so tag filtering works in the demo.
After loading it runs one daily-average grid query and prints the timing,
which is exactly what the Excel status cell will show.
"""
import argparse
import json
import time
import urllib.request


def post(url, path, payload):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{url}{path}", data, {"Content-Type": "application/json"}
    )
    with urllib.request.urlopen(req) as r:
        return r.status, r.read()


def get(url, path):
    with urllib.request.urlopen(f"{url}{path}") as r:
        return json.loads(r.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--metric", default="sensor.temp")
    ap.add_argument("--series", type=int, default=20, help="distinct host tags")
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--interval-sec", type=int, default=60)
    ap.add_argument("--batch", type=int, default=50_000, help="points per HTTP request")
    args = ap.parse_args()

    start_ms = 1_577_836_800_000  # 2020-01-01
    step_ms = args.interval_sec * 1000
    per_series = (args.days * 86_400) // args.interval_sec
    total = per_series * args.series
    print(
        f"seeding {total:,} points "
        f"({args.series} series x {per_series:,} pts) into {args.url} ..."
    )

    t0 = time.time()
    sent = 0
    for s in range(args.series):
        host = f"h{s:03d}"
        # Build one series in batches so requests stay a sane size.
        i = 0
        while i < per_series:
            n = min(args.batch, per_series - i)
            pts = []
            for j in range(i, i + n):
                ts = start_ms + j * step_ms
                # A smooth daily cycle + per-series offset + slow drift.
                val = (
                    20.0
                    + s * 0.5
                    + 8.0 * __import__("math").sin(j * step_ms / 86_400_000 * 6.283)
                    + j * 1e-6
                )
                pts.append([ts, round(val, 4)])
            st, _ = post(
                args.url,
                "/api/v1/datapoints",
                [{"name": args.metric, "tags": {"host": host}, "datapoints": pts}],
            )
            assert st == 204, f"ingest failed: {st}"
            sent += n
            i += n
        print(f"  series {s + 1}/{args.series} done ({sent:,}/{total:,})", end="\r")

    ack_s = time.time() - t0
    print(f"\ningested {sent:,} points in {ack_s:.1f}s ({sent / ack_s / 1e6:.2f} M pts/s ack)")

    # Wait until everything is queryable, then time a daily-average scan — the
    # exact shape the Excel custom function issues.
    end_ms = start_ms + per_series * step_ms
    q = (
        f"/api/v1/query/grid?metric={args.metric}&start={start_ms}&end={end_ms}"
        f"&aggregator=avg&sampling_value=1&sampling_unit=days&align_sampling=false"
    )
    while True:
        r = get(args.url, q)
        if r["sample_size"] >= sent:
            break
        time.sleep(0.5)

    print(
        f"\ngrid query (daily avg over the whole range):\n"
        f"  scanned {r['sample_size']:,} points -> {r['row_count']:,} rows "
        f"in {r['elapsed_ms']} ms"
    )
    print(
        f'\nIn Excel:  =KAIROS.QUERY("{args.metric}", DATE(2020,1,1), TODAY(), "avg", "1d")'
    )


if __name__ == "__main__":
    main()
