"""Run every public single-scenario unit through an image, timed the way the Final ranks it.

Per scenario: rows in trace.parquet / Docker's own container window (State.StartedAt -> FinishedAt),
plus SHA-256 of trace.parquet and message_trace.parquet. With --baseline the speedup is printed.
Output content is checked separately (the native writer's bytes differ from pandas'):
    python bench/compare.py --outputs <out> --ref <python baseline outputs>

    python bench.py --image t3opt:dev --out ~/runs/dev [--baseline ~/runs/bench_base.json] [--only as06]
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import datetime as dt
import hashlib
import json
import pathlib
import statistics
import subprocess

import pyarrow.parquet as pq

SCEN_DIR = pathlib.Path.home() / "track3-simulation-public/regression_suite/scenarios"


def _ts(s: str) -> float:
    # Docker prints nanoseconds; datetime takes microseconds.
    head, frac = s.rstrip("Z").split(".")
    base = dt.datetime.fromisoformat(head).replace(tzinfo=dt.timezone.utc).timestamp()
    return base + float("0." + frac)


def _sha(p: pathlib.Path) -> str | None:
    return hashlib.sha256(p.read_bytes()).hexdigest() if p.exists() else None


def run_one(image: str, scen: pathlib.Path, out_root: pathlib.Path, cpus: str, mem: str) -> dict:
    out = out_root / scen.stem
    # Containers run as root (as the graders do: ABIDES writes ./log under the workdir), so outputs are
    # root-owned and cannot be cleaned up here; every bench run needs a fresh --out.
    out.mkdir(parents=True, exist_ok=False)
    cid = subprocess.run(
        ["docker", "run", "-d", "--network", "none", f"--cpus={cpus}", f"--memory={mem}",
         "-v", f"{scen}:/input/scenario.json:ro", "-v", f"{out}:/output",
         image, "simulate", "--config", "/input/scenario.json", "--out", "/output/trace.parquet"],
        check=True, capture_output=True, text=True).stdout.strip()
    try:
        subprocess.run(["docker", "wait", cid], check=True, capture_output=True, timeout=1800)
        st = json.loads(subprocess.run(["docker", "inspect", cid, "--format", "{{json .State}}"],
                                       check=True, capture_output=True, text=True).stdout)
        if st["ExitCode"] != 0:
            log = subprocess.run(["docker", "logs", "--tail", "20", cid], capture_output=True, text=True)
            (out / "stderr.txt").write_text(log.stdout + log.stderr)
    finally:
        subprocess.run(["docker", "rm", "-f", cid], capture_output=True)
    window = _ts(st["FinishedAt"]) - _ts(st["StartedAt"])
    trace = out / "trace.parquet"
    rows = pq.ParquetFile(trace).metadata.num_rows if trace.exists() else 0
    return {"name": scen.stem, "exit": st["ExitCode"], "rows": rows, "window_sec": window,
            "rate": rows / window if window > 0 else 0.0,
            "trace_sha": _sha(trace), "msg_sha": _sha(out / "message_trace.parquet")}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--image", required=True)
    ap.add_argument("--out", required=True, type=pathlib.Path)
    ap.add_argument("--baseline", type=pathlib.Path)
    ap.add_argument("--only", default="", help="comma-separated name prefixes")
    ap.add_argument("--workers", type=int, default=2)
    ap.add_argument("--cpus", default="4")
    ap.add_argument("--memory", default="16g")
    a = ap.parse_args()

    scens = sorted(p for p in SCEN_DIR.glob("*.json") if p.name != "index.json")
    if a.only:
        scens = [p for p in scens if any(p.stem.startswith(o) for o in a.only.split(","))]
    a.out.mkdir(parents=True, exist_ok=True)
    with cf.ThreadPoolExecutor(a.workers) as ex:
        res = sorted(ex.map(lambda s: run_one(a.image, s, a.out, a.cpus, a.memory), scens),
                     key=lambda r: r["name"])
    (a.out / "bench.json").write_text(json.dumps(res, indent=1))

    base = {r["name"]: r for r in json.loads(a.baseline.read_text())} if a.baseline else {}
    bad, ratios = [], []
    for r in res:
        b = base.get(r["name"])
        if r["exit"] != 0:
            bad.append(r["name"])
        if b and b["rate"] > 0:
            ratios.append(r["rate"] / b["rate"])
        print(f'{r["name"]:38s} exit={r["exit"]} rows={r["rows"]:>8} {r["window_sec"]:7.2f}s '
              f'{r["rate"]:10.0f}/s' + (f'  x{r["rate"] / b["rate"]:.2f}' if b else "")
              )
    print(f"mean rate {statistics.mean(r['rate'] for r in res):.0f}/s over {len(res)} scenarios")
    if ratios:
        print(f"speedup vs baseline: geo-mean x{statistics.geometric_mean(ratios):.2f}, "
              f"mean-rate x{statistics.mean(r['rate'] for r in res) / statistics.mean(base[r['name']]['rate'] for r in res):.2f}")
    print("BAD: " + (", ".join(bad) if bad else "none"))
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
