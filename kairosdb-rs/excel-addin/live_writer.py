#!/usr/bin/env python3
"""Append one data point per second at the current time, so a live/streaming
client (e.g. the Excel =KAIROS.STREAM function) has fresh data to scroll.

Usage:
  python3 live_writer.py --url http://127.0.0.1:8080 --metric live.ticker
"""
import argparse
import json
import math
import time
import urllib.request


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://127.0.0.1:8080")
    ap.add_argument("--metric", default="live.ticker")
    ap.add_argument("--host", default="a")
    ap.add_argument("--hz", type=float, default=1.0, help="points per second")
    args = ap.parse_args()

    period = 1.0 / args.hz
    print(f"writing {args.metric} at {args.hz} Hz to {args.url} (Ctrl-C to stop)")
    i = 0
    while True:
        ts = int(time.time() * 1000)
        # A wandering value: sine + slow drift, so the scroll is visibly alive.
        val = round(100 + 10 * math.sin(i / 12.0) + 0.5 * math.sin(i / 1.7), 3)
        body = [{"name": args.metric, "tags": {"host": args.host},
                 "datapoints": [[ts, val]]}]
        req = urllib.request.Request(
            f"{args.url}/api/v1/datapoints",
            json.dumps(body).encode(),
            {"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req)
        except Exception as e:  # noqa: BLE001
            print(f"  write failed: {e}")
        i += 1
        if i % 10 == 0:
            print(f"  wrote {i} points (last={val})", end="\r")
        time.sleep(period)


if __name__ == "__main__":
    main()
