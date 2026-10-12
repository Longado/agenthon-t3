"""Scale stress for the Final: public-shaped scenarios scaled far past the public sizes (more agents,
longer horizons, deeper market-maker books, wider prices), run on 4 cpus as the Final would.

    python scale.py NEW OLD --n 40 --seed 1 --out ~/scale1 [--py-max 2500000] [--py-jobs 6]

Per case, NEW runs twice and OLD once. A case is BAD if NEW fails, takes > 300 s, peaks above
16 GiB, breaks an output-folder limit (256 MiB per file and in total, 256 files), reports an
n_events or sha256 that does not match its files, differs between its two runs by a byte, or
differs in content from OLD. Cases small enough (actual events <= --py-max) are then run through
the Python baseline image and compared in content: the exact-match gate the Final applies.
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
ap.add_argument("--n", type=int, default=40); ap.add_argument("--seed", type=int, default=1)
ap.add_argument("--out", type=pathlib.Path, required=True)
ap.add_argument("--max-events", type=float, default=35e6)
ap.add_argument("--agent-lo", type=float, default=1); ap.add_argument("--agent-hi", type=float, default=12)
ap.add_argument("--py-max", type=float, default=2.5e6); ap.add_argument("--py-jobs", type=int, default=6)
a = ap.parse_args()

bases = []
for d in sorted(U.iterdir()):
    if (d / "scenario.json").exists():
        bases.append((d.name, json.loads((d / "scenario.json").read_text()), json.loads((d / "events.json").read_text())["n_events"]))
heavy = [b for b in bases if b[0].startswith(("t3-gb-", "t3-mr-", "t3-mp0", "t3-s012", "t3-cancelmodify"))]


def logu(r, lo, hi):
    return math.exp(r.uniform(math.log(lo), math.log(hi)))


def make(r: random.Random, i: int):
    name, base, n0 = r.choice(heavy if r.random() < 0.75 else bases)
    s = copy.deepcopy(base)
    s["scenario_id"] = f"scale-{i:03d}"
    s["seed"] = r.randrange(2**32)
    agents = logu(r, a.agent_lo, a.agent_hi)
    horizon = logu(r, 1, 6)
    for ac in s["agent_configs"]:
        ac["count"] = max(1, round(ac["count"] * agents))
        p = ac.setdefault("params", {})
        if ac["agent_type"] == "MarketMaker" and r.random() < 0.4:
            p["depth_levels"] = int(min(40, max(1, p.get("depth_levels", 3)) * r.randint(2, 6)))
        if "arrival_rate_hz" in p and r.random() < 0.3:
            p["arrival_rate_hz"] = float(p["arrival_rate_hz"]) * r.uniform(1, 3)
    if sum(ac["count"] for ac in s["agent_configs"]) > 3000:
        return None
    op = s.setdefault("oracle_config", {}).setdefault("params", {})
    if r.random() < 0.25:  # wide price paths: volatile fundamental with jumps
        op["sigma"] = r.uniform(1e-3, 5e-3)
        op["jump_intensity"] = r.uniform(1, 20)
        op["jump_sigma"] = r.uniform(500, 5000)
    est = n0 * agents * horizon
    if est > a.max_events:
        horizon *= a.max_events / est
        est = a.max_events
    s["horizon_ns"] = int(base["horizon_ns"] * max(horizon, 0.2))
    ledger = est <= 7e6 and r.random() < 0.4
    return name, s, ledger, est


def sha(p):
    h = hashlib.sha256()
    with open(p, "rb") as f:
        for b in iter(lambda: f.read(1 << 22), b""):
            h.update(b)
    return h.hexdigest()


def run(binary, d, tag):
    o = d / tag
    shutil.rmtree(o, ignore_errors=True)
    o.mkdir()
    with open(d / f"{tag}.err", "w+") as err:
        t = time.perf_counter()
        p = subprocess.Popen(["taskset", "-c", "0-3", binary, "simulate", "--config", str(d / "scenario.json"),
                              "--out", str(o / "trace.parquet")], stdout=subprocess.DEVNULL, stderr=err,
                             env={**os.environ, "T3SIM_NO_FALLBACK": "1"})
        _, status, ru = os.wait4(p.pid, 0)  # taskset execs the binary: same pid, its own rusage
        secs = time.perf_counter() - t
        err.seek(0)
        return os.waitstatus_to_exitcode(status), ru.ru_maxrss * 1024, secs, err.read()[-300:]


def checks(d, o):
    bad = []
    files = [f for f in o.rglob("*") if f.is_file()]
    sizes = {f.name: f.stat().st_size for f in files}
    if len(files) > 256 or max(sizes.values()) > 256 * MIB or sum(sizes.values()) > 256 * MIB:
        bad.append(f"output limits: {sizes}")
    ev = json.loads((o / "events.json").read_text())
    rows = pq.ParquetFile(o / "trace.parquet").metadata.num_rows
    if ev["n_events"] != rows:
        bad.append(f"n_events {ev['n_events']} != rows {rows}")
    for f, k in (("trace.parquet", "trace_sha256"), ("message_trace.parquet", "message_trace_sha256")):
        if (o / f).exists() and ev.get(k) != sha(o / f):
            bad.append(f"{f} sha mismatch")
    return bad, rows, sum(sizes.values())


def tables_equal(x, y):
    return pq.read_table(x).equals(pq.read_table(y))


root = a.out
root.mkdir(parents=True, exist_ok=False)
r = random.Random(a.seed)
cases = []
while len(cases) < a.n:
    m = make(r, len(cases))
    if m is None:
        continue
    name, s, ledger, est = m
    d = root / f"c{len(cases):03d}"
    d.mkdir()
    (d / "scenario.json").write_text(json.dumps(s))
    if not ledger:
        (d / "card.toml").write_text("[scoring.params]\nrequires_message_ledger = false\n")
    cases.append((d, name, ledger))

nbad = 0
small = []
for d, name, ledger in cases:
    msgs = []
    c1, rss1, t1, err1 = run(a.new, d, "new1")
    if c1:
        print(f"BAD  {d.name} {name} new exit {c1}: {err1}", flush=True)
        nbad += 1
        continue
    bad, rows, out_bytes = checks(d, d / "new1")
    msgs += bad
    c2, rss2, t2, _ = run(a.new, d, "new2")
    if c2 or any(sha(d / "new1" / f) != sha(d / "new2" / f) for f in ("trace.parquet", "message_trace.parquet") if (d / "new1" / f).exists()):
        msgs.append("repeat run differs")
    co, _, to, erro = run(a.old, d, "old")
    if co:
        msgs.append(f"old exit {co}: {erro}")
    else:
        for f in ("trace.parquet", "message_trace.parquet"):
            if (d / "new1" / f).exists() != (d / "old" / f).exists() or ((d / "new1" / f).exists() and not tables_equal(d / "new1" / f, d / "old" / f)):
                msgs.append(f"{f} differs from old")
    # the binary's own VmHWM: wait4's maxrss also counts the harness's memory copied at fork
    rss, t = json.loads((d / "new1" / "events.json").read_text())["peak_memory_bytes"], min(t1, t2)
    if t > 300 or rss > 16 * 2**30:
        msgs.append("over the Final run limits")
    agents = sum(ac["count"] for ac in json.loads((d / "scenario.json").read_text())["agent_configs"])
    print(f"{'BAD ' if msgs else 'OK  '} {d.name} {name[3:]:30s} agents {agents:5d} ledger {int(ledger)} rows {rows:>10,d} "
          f"{t:6.2f}s ({rows / t / 1e6:4.1f}M/s) rss {rss / 2**20:6.0f} MiB out {out_bytes / MIB:6.1f} MiB "
          f"old {to:6.2f}s {'; '.join(msgs)}", flush=True)
    nbad += bool(msgs)
    if not msgs and rows <= a.py_max:
        small.append(d)
    for tag in ("new2", "old"):
        shutil.rmtree(d / tag, ignore_errors=True)


def python_check(d):
    o = d / "py"
    o.mkdir(exist_ok=True)
    p = subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{d}:/in:ro", "-v", f"{o}:/out", BASE_IMAGE,
                        "simulate", "--config", "/in/scenario.json", "--out", "/out/trace.parquet"],
                       capture_output=True, text=True, timeout=7200)
    if p.returncode:
        return d.name, f"SKIP baseline failed: {(p.stderr.strip().splitlines() or ['?'])[-1][:200]}"
    import pandas as pd
    diffs = []
    for f in ("trace.parquet", "message_trace.parquet"):
        x, y = d / "new1" / f, o / f
        if x.exists():
            df = first_diff(pd.read_parquet(x), pd.read_parquet(y))
            if df:
                diffs.append(f"[{f}] {df}")
    return d.name, ("BAD " + " ".join(diffs)) if diffs else "OK"


print(f"native phase: {len(cases) - nbad}/{len(cases)} clean; python phase on {len(small)} cases", flush=True)
pybad = 0
with cf.ThreadPoolExecutor(a.py_jobs) as ex:
    for name, res in ex.map(python_check, small):
        pybad += res.startswith("BAD")
        print(f"PY   {name} {res}", flush=True)
print(f"SUMMARY native bad {nbad}/{len(cases)}, python bad {pybad}/{len(small)}", flush=True)
