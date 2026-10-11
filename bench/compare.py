"""Run the native binary on public scenarios and diff its outputs against the Python baseline's.

    python bench/compare.py --bin native/target/release/t3sim --ref ~/runs/b_official [--only as06,mp01] [--jobs 6]
    python bench/compare.py --outputs ~/runs/n1 --ref ~/runs/b_official   # diff an existing bench.py run

For each scenario: run `t3sim simulate` (T3SIM_NO_FALLBACK=1, so an unsupported scenario fails
instead of silently running Python), then compare trace.parquet and message_trace.parquet with
the reference files as pandas DataFrames. Prints the first differing row with context.
"""

from __future__ import annotations

import argparse
import concurrent.futures as cf
import os
import pathlib
import tomllib
import subprocess
import tempfile
import time

import pandas as pd

SCEN_DIR = pathlib.Path.home() / "track3-simulation-public/regression_suite/scenarios"


def first_diff(a: pd.DataFrame, b: pd.DataFrame) -> str:
    if list(a.columns) != list(b.columns):
        return f"columns {list(a.columns)} != {list(b.columns)}"
    dt = [(c, str(a[c].dtype), str(b[c].dtype)) for c in a.columns if a[c].dtype != b[c].dtype]
    n = min(len(a), len(b))
    for c in a.columns:
        x, y = a[c].iloc[:n].reset_index(drop=True), b[c].iloc[:n].reset_index(drop=True)
        neq = ~((x == y) | (x.isna() & y.isna())).fillna(False).astype(bool)
        if neq.any():
            i = int(neq.idxmax())
            lo, hi = max(0, i - 3), i + 3
            return (f"rows {len(a)} vs {len(b)}; first diff at row {i} col {c}\n"
                    f"--- ours\n{a.iloc[lo:hi].to_string()}\n--- ref\n{b.iloc[lo:hi].to_string()}")
    if len(a) != len(b):
        return f"rows {len(a)} vs {len(b)} (common prefix identical)"
    if dt:
        return f"dtypes differ {dt}"
    return ""


UNITS_DIR = pathlib.Path.home() / "track3-simulation-public/units"


def ledger_optional(stem: str) -> bool:
    """True when the unit's card says message_trace.parquet may be omitted."""
    card = UNITS_DIR / ("t3-" + stem.replace("_", "-")) / "card.toml"
    return card.exists() and tomllib.loads(card.read_text())["scoring"]["params"].get("requires_message_ledger") is False


def diff_dir(out: pathlib.Path, ref: pathlib.Path) -> str:
    msgs = []
    for f in ("trace.parquet", "message_trace.parquet"):
        if not (out / f).exists():
            if not (f == "message_trace.parquet" and ledger_optional(ref.name)):
                msgs.append(f"[{f}] missing")
            continue
        d = first_diff(pd.read_parquet(out / f), pd.read_parquet(ref / f))
        if d:
            msgs.append(f"[{f}] {d}")
    return "\n".join(msgs)


def check(binary: str, scen: pathlib.Path, ref_root: pathlib.Path) -> tuple[str, str, float]:
    with tempfile.TemporaryDirectory() as td:
        out = pathlib.Path(td) / "trace.parquet"
        t0 = time.perf_counter()
        p = subprocess.run([binary, "simulate", "--config", str(scen), "--out", str(out)],
                           capture_output=True, text=True, env={**os.environ, "T3SIM_NO_FALLBACK": "1"})
        dt = time.perf_counter() - t0
        if p.returncode != 0:
            return scen.stem, f"exit {p.returncode}: {p.stderr.strip()[-400:]}", dt
        return scen.stem, diff_dir(out.parent, ref_root / scen.stem), dt


def main() -> int:
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--bin")
    src.add_argument("--outputs", type=pathlib.Path, help="existing per-scenario output dirs")
    ap.add_argument("--ref", required=True, type=pathlib.Path)
    ap.add_argument("--only", default="")
    ap.add_argument("--jobs", type=int, default=6)
    a = ap.parse_args()
    scens = sorted(p for p in SCEN_DIR.glob("*.json") if p.name != "index.json")
    if a.only:
        scens = [p for p in scens if any(p.stem.startswith(o) for o in a.only.split(","))]
    bad = 0
    with cf.ThreadPoolExecutor(a.jobs) as ex:
        if a.outputs:
            job = lambda s: (s.stem, diff_dir(a.outputs / s.stem, a.ref / s.stem), 0.0)
        else:
            job = lambda s: check(a.bin, s, a.ref)
        for name, diff, dt in ex.map(job, scens):
            print(f"{'OK ' if not diff else 'BAD'} {name:34s} {dt:6.3f}s")
            if diff:
                bad += 1
                print("    " + diff.replace("\n", "\n    "))
    print(f"{len(scens) - bad}/{len(scens)} identical")
    return 1 if bad else 0


if __name__ == "__main__":
    raise SystemExit(main())
