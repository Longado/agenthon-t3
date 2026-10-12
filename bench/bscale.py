"""Scale stress for batch units (simulate-batch): public gbatch units with more and larger subs.

    python bscale.py NEW OLD --n 12 --seed 1 --out ~/bscale1 [--py-max 600000] [--py-jobs 6]

Per case NEW runs twice and OLD once. BAD if NEW fails, breaks an output-folder limit, reports
counts or hashes that do not match its files, differs between runs, or differs from OLD in content.
Subs small enough are then compared with the Python baseline run on that sub alone (the isolation
gate).
"""
import argparse, concurrent.futures as cf, copy, hashlib, json, math, os, pathlib, random, shutil, subprocess, sys, time

import pyarrow.parquet as pq

sys.path.insert(0, str(pathlib.Path.home() / "t3opt/bench"))
from compare import first_diff  # noqa: E402

U = pathlib.Path.home() / "track3-simulation-public/units"
BASE_IMAGE = "track3-abides-baseline:latest"
MIB = 2**20
ap = argparse.ArgumentParser()
ap.add_argument("new"); ap.add_argument("old")
ap.add_argument("--n", type=int, default=12); ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--out", type=pathlib.Path, required=True)
ap.add_argument("--py-max", type=float, default=6e5); ap.add_argument("--py-jobs", type=int, default=6)
a = ap.parse_args()
units = sorted(d for d in U.iterdir() if (d / "batch.json").exists())


def logu(r, lo, hi):
    return math.exp(r.uniform(math.log(lo), math.log(hi)))


def make(r, d):
    u = r.choice(units)
    subs = [json.loads(p.read_text()) for p in sorted((u / "scenarios").glob("*.json"))]
    n = r.choice([len(subs), r.randint(1, 3), r.randint(4, 16)])
    (d / "scenarios").mkdir(parents=True)
    for k in range(n):
        s = copy.deepcopy(subs[k % len(subs)])
        s["scenario_id"] = f"{d.name}-sub{k:02d}"
        s["seed"] = r.randrange(2**32)
        f = logu(r, 1, 8)
        for ac in s["agent_configs"]:
            ac["count"] = max(1, round(ac["count"] * f))
        s["horizon_ns"] = int(s["horizon_ns"] * logu(r, 1, 6))
        (d / "scenarios" / f"sub_{k:02d}.json").write_text(json.dumps(s))
    return u.name, n


def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()


def run(binary, d, tag):
    o = d / tag
    shutil.rmtree(o, ignore_errors=True)
    o.mkdir()
    t = time.perf_counter()
    p = subprocess.run(["taskset", "-c", "0-3", binary, "simulate-batch", "--batch-dir", str(d / "scenarios"), "--out-dir", str(o)],
                       capture_output=True, text=True, env={**os.environ, "T3SIM_NO_FALLBACK": "1"}, timeout=900)
    return p.returncode, time.perf_counter() - t, p.stderr[-300:]


root = a.out
root.mkdir(parents=True, exist_ok=False)
r = random.Random(a.seed)
nbad, small = 0, []
for i in range(a.n):
    d = root / f"b{i:03d}"
    name, nsubs = make(r, d)
    msgs = []
    c1, t1, e1 = run(a.new, d, "new1")
    if c1:
        print(f"BAD  {d.name} {name} new exit {c1}: {e1}", flush=True)
        nbad += 1
        continue
    o = d / "new1"
    files = [f for f in o.rglob("*") if f.is_file()]
    if len(files) > 256 or max(f.stat().st_size for f in files) > 256 * MIB or sum(f.stat().st_size for f in files) > 256 * MIB:
        msgs.append("output limits")
    agg = json.loads((o / "batch_events.json").read_text())
    rows = 0
    for sd in sorted(x for x in o.iterdir() if x.is_dir()):
        ev = json.loads((sd / "events.json").read_text())
        n = pq.ParquetFile(sd / "trace.parquet").metadata.num_rows
        rows += n
        if ev["n_events"] != n or ev.get("trace_sha256") != sha(sd / "trace.parquet") or not (sd / "message_trace.parquet").exists() \
                or ev.get("message_trace_sha256") != sha(sd / "message_trace.parquet"):
            msgs.append(f"{sd.name} counts/hashes/ledger")
        if n <= a.py_max:
            small.append(sd)
    if agg["total_events"] != rows:
        msgs.append(f"total_events {agg['total_events']} != rows {rows}")
    c2, t2, _ = run(a.new, d, "new2")
    if c2 or any(sha(f) != sha(d / "new2" / f.relative_to(o)) for f in files if f.suffix == ".parquet"):
        msgs.append("repeat run differs")
    co, to, eo = run(a.old, d, "old")
    if co:
        msgs.append(f"old exit {co}: {eo}")
    else:
        for f in files:
            if f.suffix == ".parquet" and not pq.read_table(f).equals(pq.read_table(d / "old" / f.relative_to(o))):
                msgs.append(f"{f.relative_to(o)} differs from old")
    t = min(t1, t2)
    peak = max(json.loads((sd / "events.json").read_text()).get("peak_memory_bytes", 0) for sd in o.iterdir() if sd.is_dir())
    print(f"{'BAD ' if msgs else 'OK  '} {d.name} {name[3:]:22s} subs {nsubs:2d} rows {rows:>10,d} {t:6.2f}s ({rows / t / 1e6:4.1f}M/s) "
          f"peak {peak / MIB:6.0f} MiB out {sum(f.stat().st_size for f in files) / MIB:6.1f} MiB old {to:6.2f}s {'; '.join(msgs)}", flush=True)
    nbad += bool(msgs)
    for tag in ("new2", "old"):
        shutil.rmtree(d / tag, ignore_errors=True)


def python_check(sd):
    d = sd.parent.parent
    scen = d / "scenarios" / f"{sd.name}.json"
    out = d / "py" / sd.name
    out.mkdir(parents=True, exist_ok=True)
    p = subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{scen.parent}:/in:ro", "-v", f"{out}:/out", BASE_IMAGE,
                        "simulate", "--config", f"/in/{scen.name}", "--out", "/out/trace.parquet"], capture_output=True, text=True, timeout=7200)
    if p.returncode:
        return f"{d.name}/{sd.name}", "SKIP baseline failed"
    import pandas as pd
    diffs = [f"[{f}] {x}" for f in ("trace.parquet", "message_trace.parquet")
             if (x := first_diff(pd.read_parquet(sd / f), pd.read_parquet(out / f)))]
    return f"{d.name}/{sd.name}", ("BAD " + " ".join(diffs)) if diffs else "OK"


print(f"native phase: {a.n - nbad}/{a.n} clean; python phase on {len(small)} subs", flush=True)
pybad = 0
with cf.ThreadPoolExecutor(a.py_jobs) as ex:
    for name, res in ex.map(python_check, small):
        pybad += res.startswith("BAD")
        print(f"PY   {name} {res}", flush=True)
print(f"SUMMARY native bad {nbad}/{a.n}, python bad {pybad}/{len(small)}", flush=True)
