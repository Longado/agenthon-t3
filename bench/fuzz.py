"""Differential fuzz: random variants of the public scenarios, Python baseline image vs native binary.

    python bench/fuzz.py --bin native/target/release/t3sim --n 200 --out ~/runs/fuzz1 [--jobs 7] [--seed 1]

Each variant perturbs seed, horizon, exchange protocol/STP, oracle, latency model and the agent mix
within the public parameter vocabulary (hidden scenarios use no new params). A variant the baseline
itself rejects is skipped; one the native engine refuses counts as fallback (slow but correct);
any content difference is a bug and is printed with its scenario file kept for replay.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import copy
import json
import os
import pathlib
import random
import subprocess

import pandas as pd

from compare import first_diff

SCEN_DIR = pathlib.Path.home() / "track3-simulation-public/regression_suite/scenarios"
BASE_IMAGE = "track3-abides-baseline:latest"


def num(r: random.Random, lo: float, hi: float, as_int: bool, float_rate: float = 0.15):
    """A value in [lo, hi]; ints sometimes written as floats (Python int/float paths differ)."""
    v = r.uniform(lo, hi)
    if as_int:
        v = int(round(v))
        return float(v) if r.random() < float_rate else v
    return v


def mutate(base: dict, r: random.Random, i: int) -> dict:
    s = copy.deepcopy(base)
    s["scenario_id"] = f"fuzz-{i:04d}"
    s["seed"] = r.randrange(2**32)
    # Keep the baseline's runtime bounded: big families get a short horizon.
    scale = r.uniform(0.15, 1.0) * (0.12 if base.get("scenario_family", "").startswith("gb") or base["horizon_ns"] > 2e10 else 1.0)
    s["horizon_ns"] = max(int(base["horizon_ns"] * scale), 50_000_000)

    ex = s.setdefault("exchange_config", {})
    if r.random() < 0.4:
        ex["protocol_enforcement"] = True
    else:
        ex.pop("protocol_enforcement", None)
    ex["stp_policy"] = r.choice(["cancel_newest", "cancel_oldest", "cancel_newest", None])
    # No public scenario sets these; a large compute delay congests the exchange quadratically in
    # the baseline (a 200 us one ran > 6 min), so keep them rare and small.
    for k in ("ack_delay_ns", "compute_delay_ns"):
        if r.random() < 0.15:
            ex[k] = num(r, 0, 20_000, True)
        else:
            ex.pop(k, None)

    op = s.setdefault("oracle_config", {}).setdefault("params", {})
    op["initial_price"] = num(r, 1_000, 500_000, True, 0.05)
    op["kappa"] = r.choice([0.0, num(r, 0.001, 2.0, False)])
    op["sigma"] = num(r, 1e-5, 5e-3, False)
    jumps = r.random() < 0.4
    op["jump_intensity"] = num(r, 0.05, 20.0, False) if jumps else 0.0
    op["jump_sigma"] = num(r, 50, 5_000, False) if jumps else 0.0
    if r.random() < 0.25:
        op["scheduled_jump"] = {"time_ns": int(s["horizon_ns"] * r.uniform(0.05, 0.95)),
                                "magnitude": num(r, -2_000, 2_000, True, 0.05)}
    else:
        op.pop("scheduled_jump", None)

    model = r.choice(["log_normal", "log_normal", "uniform", "pareto", "deterministic"])
    lp = {"mean_ns": num(r, 1_000, 2_000_000, True), "min_ns": num(r, 0, 5_000, True)}
    lp["max_ns"] = num(r, 3_000_000, 50_000_000, True)
    if model == "log_normal":
        lp["sigma"] = num(r, 0.05, 1.6, False)
    if model == "pareto":
        lp["alpha"] = num(r, 1.05, 3.5, False)
    s["latency_config"] = {"model": model, "params": lp}

    agents = []
    for kind in ("MarketMaker", "ValueTrader", "NoiseTrader", "MomentumTrader"):
        if r.random() < 0.25 and kind != "NoiseTrader":
            continue
        p: dict = {}
        if kind == "MarketMaker":
            p = {"spread_ticks": num(r, 1, 10, True), "depth_levels": num(r, 1, 6, True),
                 "size_per_level": num(r, 1, 40, True), "rebalance_interval_ns": num(r, 200_000, 50_000_000, True)}
        elif kind == "ValueTrader":
            p = {"fundamental_value_source": "oracle", "order_size_mean": num(r, 1, 60, True),
                 "threshold_ticks": num(r, 0, 6, True), "arrival_rate_hz": num(r, 0.5, 40, r.random() < 0.3)}
        elif kind == "NoiseTrader":
            p = {"order_size_mean": num(r, 1, 40, True), "order_size_std": num(r, 0, 10, True),
                 "arrival_rate_hz": num(r, 0.5, 60, r.random() < 0.3), "price_offset_ticks": num(r, 0, 10, True)}
        else:
            p = {"order_size_mean": num(r, 1, 40, True), "threshold_ticks": num(r, 0, 6, True),
                 "lookback": num(r, 1, 20, True), "arrival_rate_hz": num(r, 0.5, 40, r.random() < 0.3)}
        for k in list(p):
            if r.random() < 0.08:
                del p[k]  # exercise the adapter's defaults
        count = r.randint(1, 3) if kind == "MarketMaker" else r.randint(1, 12)
        agents.append({"agent_type": kind, "count": count, "params": p})
    r.shuffle(agents)
    s["agent_configs"] = agents
    return s


def run_python(scen: pathlib.Path, out: pathlib.Path) -> str:
    out.mkdir(parents=True, exist_ok=True)
    p = subprocess.run(["docker", "run", "--rm", "--network", "none", "-v", f"{scen.parent}:/in:ro",
                        "-v", f"{out}:/out", BASE_IMAGE, "simulate", "--config", f"/in/{scen.name}",
                        "--out", "/out/trace.parquet"], capture_output=True, text=True, timeout=900)
    return "" if p.returncode == 0 else (p.stderr.strip().splitlines() or ["?"])[-1][:200]


def run_native(binary: str, scen: pathlib.Path, out: pathlib.Path) -> tuple[int, str]:
    out.mkdir(parents=True, exist_ok=True)
    p = subprocess.run([binary, "simulate", "--config", str(scen), "--out", str(out / "trace.parquet")],
                       capture_output=True, text=True, env={**os.environ, "T3SIM_NO_FALLBACK": "1"}, timeout=900)
    return p.returncode, p.stderr.strip()[-300:]


def check(binary: str, scen: pathlib.Path, root: pathlib.Path) -> tuple[str, str, str]:
    py_out, nat_out = root / "py" / scen.stem, root / "nat" / scen.stem
    err = run_python(scen, py_out)
    if err:
        return scen.stem, "SKIP", f"baseline failed: {err}"
    code, stderr = run_native(binary, scen, nat_out)
    if code == 3:
        return scen.stem, "FALLBACK", stderr
    if code != 0:
        return scen.stem, "BAD", f"native exit {code}: {stderr}"
    msgs = []
    for f in ("trace.parquet", "message_trace.parquet"):
        d = first_diff(pd.read_parquet(nat_out / f), pd.read_parquet(py_out / f))
        if d:
            msgs.append(f"[{f}] {d}")
    rows = len(pd.read_parquet(py_out / "trace.parquet", columns=["t_ns"]))
    return scen.stem, "BAD" if msgs else "OK", "\n".join(msgs) or f"{rows} rows"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bin", required=True)
    ap.add_argument("--n", type=int, default=100)
    ap.add_argument("--out", type=pathlib.Path, required=True)
    ap.add_argument("--jobs", type=int, default=7)
    ap.add_argument("--seed", type=int, default=1)
    a = ap.parse_args()
    r = random.Random(a.seed)
    bases = [json.loads(p.read_text()) for p in sorted(SCEN_DIR.glob("*.json")) if p.name != "index.json"]
    scen_dir = a.out / "scenarios"
    scen_dir.mkdir(parents=True, exist_ok=False)
    paths = []
    for i in range(a.n):
        b = r.choice(bases)
        p = scen_dir / f"fuzz-{i:04d}.json"
        p.write_text(json.dumps(mutate(b, r, i), indent=1))
        paths.append(p)
    tally: dict[str, int] = {}
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        for name, status, info in ex.map(lambda p: check(a.bin, p, a.out), paths):
            tally[status] = tally.get(status, 0) + 1
            line = info if status != "BAD" else info.replace("\n", "\n    ")
            print(f"{status:8s} {name} {line if status != 'OK' else info}", flush=True)
    print("TALLY", json.dumps(tally), flush=True)
    return 1 if tally.get("BAD") else 0


if __name__ == "__main__":
    raise SystemExit(main())
