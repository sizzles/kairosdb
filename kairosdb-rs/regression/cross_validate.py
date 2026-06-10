import json, gzip, time, urllib.request, sys

JAVA, RUST = "http://127.0.0.1:8082/api/v1", "http://127.0.0.1:18080/api/v1"

def post(base, path, payload, headers=None):
    data = json.dumps(payload).encode()
    h = {"Content-Type": "application/json"}
    if headers: h.update(headers); data = gzip.compress(data) if headers.get("Content-Encoding")=="gzip" else data
    req = urllib.request.Request(f"{base}{path}", data, h)
    try:
        with urllib.request.urlopen(req) as r: return r.status, r.read()
    except urllib.error.HTTPError as e: return e.code, e.read()

def values(resp):
    d = json.loads(resp)
    out = []
    for r in d["queries"][0]["results"]:
        out.append(r.get("values"))
    return out

passed = failed = 0
def check(name, ok, detail=""):
    global passed, failed
    if ok: passed += 1; print(f"  PASS {name}")
    else: failed += 1; print(f"  FAIL {name}: {detail}")

# Seed: rich series for aggregator tests (both servers share Cassandra; write via Java)
T0 = 1765238400000
pts = [[T0 + i*60000, 50 + (i%7)*1.5 + (0.01*i)] for i in range(60)]
st,_ = post(JAVA, "/datapoints", [{"name":"regress.series","tags":{"src":"x"},"datapoints":pts}])
assert st == 204
time.sleep(2.5)

W = {"start_absolute": T0, "end_absolute": T0 + 3600_000*2}
def q(aggs=None, extra=None):
    m = {"name":"regress.series"}
    if aggs: m["aggregators"] = aggs
    if extra: m.update(extra)
    return dict(W, metrics=[m])

print("== aggregator cross-validation (Java vs Rust) ==")
cases = [
 ("least_squares", [{"name":"least_squares","sampling":{"value":30,"unit":"minutes"},"align_sampling":False}]),
 ("time_diff",     [{"name":"time_diff","time_unit":"seconds"}]),
 ("sampler",       [{"name":"sampler","unit":"minutes"}]),
 ("score",         [{"name":"score","thresholds":[{"value":52},{"value":55},{"value":58}]}]),
 ("pad",           [{"name":"pad","sampling":{"value":10,"unit":"minutes"},"align_sampling":False},
                    {"name":"sum","sampling":{"value":10,"unit":"minutes"},"align_sampling":False}]),
 ("gaps",          [{"name":"gaps","sampling":{"value":7,"unit":"minutes"},"align_sampling":False}]),
 ("dev+filter",    [{"name":"filter","filter_op":"lt","threshold":51.0},
                    {"name":"dev","sampling":{"value":1,"unit":"hours"},"align_sampling":False}]),
 ("trim+sma",      [{"name":"trim","trim":"both"},{"name":"sma","size":5}]),
]
for name, aggs in cases:
    sj, rj = post(JAVA, "/datapoints/query", q(aggs)), post(RUST, "/datapoints/query", q(aggs))
    if sj[0] != 200 or rj[0] != 200:
        check(name, False, f"status java={sj[0]} {sj[1][:120]} rust={rj[0]} {rj[1][:120]}"); continue
    check(name, values(sj[1]) == values(rj[1]),
          f"\n   java={values(sj[1])!r:.200}\n   rust={values(rj[1])!r:.200}")

print("== desc order + limit ==")
sj = post(JAVA, "/datapoints/query", q(extra={"order":"desc","limit":5}))
rj = post(RUST, "/datapoints/query", q(extra={"order":"desc","limit":5}))
check("desc+limit", values(sj[1]) == values(rj[1]),
      f"\n   java={values(sj[1])}\n   rust={values(rj[1])}")

print("== query/tags ==")
tq = dict(W, metrics=[{"name":"xval.price.settle"}])
sj, rj = post(JAVA, "/datapoints/query/tags", tq), post(RUST, "/datapoints/query/tags", tq)
def tagsets(resp):
    d = json.loads(resp)
    return sorted([(r["name"], json.dumps({k:sorted(v) for k,v in r["tags"].items()}, sort_keys=True))
                   for qq in d["queries"] for r in qq["results"]])
check("query/tags", tagsets(sj[1]) == tagsets(rj[1]), f"\n   java={tagsets(sj[1])}\n   rust={tagsets(rj[1])}")

print("== gzip ingest (rust) ==")
st,_ = post(RUST, "/datapoints", [{"name":"regress.gzip","tags":{"a":"b"},"datapoints":[[T0,1.5]]}],
            {"Content-Encoding":"gzip"})
time.sleep(1.5)
st2, resp = post(RUST, "/datapoints/query", dict(W, metrics=[{"name":"regress.gzip"}]))
check("gzip", st == 204 and values(resp) == [[[T0, 1.5]]], f"st={st} vals={values(resp)}")

print("== save_as ==")
st, resp = post(RUST, "/datapoints/query", q([{"name":"avg","sampling":{"value":1,"unit":"hours"},"align_sampling":False},
   {"name":"save_as","metric_name":"regress.saved"}]))
time.sleep(1.5)
st2, resp2 = post(JAVA, "/datapoints/query", dict(W, metrics=[{"name":"regress.saved"}]))  # read via JAVA
vals = values(resp2)
check("save_as (read back via java)", st == 200 and vals and len(vals[0]) >= 1, f"st={st} vals={vals}")

print("== delete via rust, verify via java ==")
post(JAVA, "/datapoints", [{"name":"regress.delete","tags":{"a":"b"},"datapoints":[[T0,1],[T0+1000,2]]}])
time.sleep(2)
st,_ = post(RUST, "/datapoints/delete", dict(W, metrics=[{"name":"regress.delete"}]))
time.sleep(1)
st2, resp = post(JAVA, "/datapoints/query", dict(W, metrics=[{"name":"regress.delete"}]))
sample = json.loads(resp)["queries"][0]["sample_size"]
check("delete", st == 204 and sample == 0, f"st={st} sample={sample}")

print(f"\n{passed} passed, {failed} failed")
sys.exit(1 if failed else 0)
