<h1 align="center">agenthon-t3</h1>
<p align="center"><strong>A Rust rebuild of the ABIDES market simulator that writes the same trace, event for event.</strong></p>
<p align="center">Built in four days for Agenthon 2026 Track 3 (simulation speed). 10th of 55 teams in the verified final ranking.</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/License-MIT-blue" alt="MIT"></a>
  <img src="https://img.shields.io/badge/tested%20on-macOS%20%7C%20Linux%20x86--64-black" alt="tested on macOS and Linux x86-64">
  <img src="https://img.shields.io/badge/Rust-1.98-orange" alt="Rust 1.98">
  <img src="https://img.shields.io/badge/status-Alpha-orange" alt="Alpha">
</p>

<p align="center">
  <a href="README.zh-CN.md">中文</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#build-with-us">Build with us</a>
</p>

On the same public scenario and the same Mac, the Python path in this repo takes 1.6 seconds. The Rust engine takes about 15 milliseconds and writes the same rows.

**Same rows as Python ABIDES · No network access · Python only as a fallback**

## What it is

[ABIDES](https://github.com/abides-sim/abides) is an agent-based simulator of a limit-order-book market. Agenthon 2026 Track 3 asked for faster simulators whose output matches the reference ABIDES fork exactly. The event count depends on every random draw, so a faster engine has to replay numpy's `RandomState` draws in the same order as the Python code.

This repo holds that engine (`native/`, a single `t3sim` binary with `simulate` and `simulate-batch` verbs), the pinned ABIDES it reproduces (`abides/`, commit `f9cbe51` plus the four official Track 3 patches, with Python-level speedups that leave its output unchanged), the Python adapter (`abides_fork/`), and the tools used to check and time it (`bench/`).

> **A competition entry, not a library.** The engine covers the four agent types and the order paths in the public Track 3 scenarios. Anything else is handed to the Python adapter, or refused with `T3SIM_NO_FALLBACK=1`. Checked: all 65 public single scenarios give the same `trace.parquet` and `message_trace.parquet` content as the Python path (rerun for this release), plus random variants through `bench/fuzz.py` during development. Not checked: scenario shapes outside the public vocabulary. See [Known issues](#known-issues).

## Results

| | Score | Rank |
|---|---|---|
| Final, verified (hidden units) | 2,581,199 | 10 of 55 |
| Development board, 12 Oct | 210,604 | 10 of 56 |

The score is the mean, over units, of events per second inside the Docker container window, with each unit capped at 10 million. The final winner scored 3,349,638. Leaderboard: [agenthon.net](https://www.agenthon.net/#leaderboard).

One run, as printed by `t3sim` (Apple M5):

```text
$ t3sim simulate --config native/pgo/t3-as06-throughput-fast/scenario.json --out out/as06/trace.parquet
{"events_per_sec":4703916.3,"n_events":74502,"n_messages":83337,"seed":1040981657,
 "trace_sha256":"247b65e7fe46a98dbeb2d05c298ad1ce9c855a1d5b1592e377374c35b2381dfd","wall_clock_sec":0.0158, ...}
```

## How it works

| You | It |
|---|---|
| run `t3sim simulate --config <scenario.json> --out <dir>/trace.parquet` | writes `trace.parquet`, `message_trace.parquet` and `events.json` (event count, SHA-256 of each file, wall clock) into `<dir>` |
| put a `card.toml` next to the scenario with `requires_message_ledger = false` | skips `message_trace.parquet` |
| run `t3sim simulate-batch --batch-dir <dir> --out-dir <dir>` | runs each sub-scenario on its own thread and writes `batch_events.json` |
| give it something it does not support | re-runs the same command as `python -m abides_fork.simulate`; with `T3SIM_NO_FALLBACK=1` it exits with code 3 instead |
| run the same scenario twice | gets byte-identical files |

What made it fast, measured on x86 cloud machines during development:

| Change | Time saved |
|---|---|
| Message latencies drawn ahead on a helper thread (they come from their own `RandomState`, one draw per message) | 21–24% on large units |
| 4-ary event queue whose child pick uses `select_unpredictable` instead of a branch | 11–14% on large units (Intel) |
| Order-book sides keep spare slots in front, so changes at the best price move nothing | 35–37% on scenarios with about 1,000 agents |
| A busy agent's requeued messages wait behind its front one | a congested case went from 295 s to 0.05 s |
| Output files written and hashed while the run goes on | 7–10% on 3× and 10× scaled units |
| Profile-guided build in the image | 2–3% |

Tried and dropped: a radix heap (16–22% slower), `x86-64-v3` (about 1%), binary search over price levels (2–5% slower when cancels cluster near the best price).

## Try these

| Try | You see |
|---|---|
| Run `t3-as06-throughput-fast` (command above) | 74,502 events in 15–40 ms on an Apple M5, and the same `trace_sha256` every time |
| Run the largest public unit, `native/pgo/t3-gb-mega-throughput/scenario.json` | 1,168,360 events in about 0.14 s; no `message_trace.parquet`, because its card says the ledger is not needed |
| Run all public scenarios in Python and compare (steps below) | `OK` per scenario, ending in `65/65 identical` |
| `T3SIM_NO_FALLBACK=1 t3sim simulate --config /dev/null` | `t3sim: unsupported (missing --config/--out)` and exit code 3 |

## Quick start

### Option A: let your AI set it up

Paste this into Claude Code or Codex:

```text
Read README.md in https://github.com/Longado/agenthon-t3 first.
Check that Rust 1.98 or newer is installed (rustc --version).
Clone the repo, build with: cargo build --release --manifest-path native/Cargo.toml
Run the t3-as06-throughput-fast scenario as shown in the README and show me events.json.
If I want the Python comparison: clone Agenthon-2026/track3-simulation-public into my home folder,
make a Python 3.11 venv with numpy==1.26.4 pandas==1.5.3 pyarrow==15.0.2 scipy coloredlogs,
and follow the README's comparison steps. Do not install anything globally.
```

### Option B: by hand

```sh
git clone https://github.com/Longado/agenthon-t3 && cd agenthon-t3
cargo build --release --manifest-path native/Cargo.toml
native/target/release/t3sim simulate \
  --config native/pgo/t3-as06-throughput-fast/scenario.json --out out/as06/trace.parquet
cat out/as06/events.json
```

Compare against the Python path on every public scenario:

```sh
git clone https://github.com/Agenthon-2026/track3-simulation-public ~/track3-simulation-public
python3.11 -m venv .venv && .venv/bin/pip install numpy==1.26.4 pandas==1.5.3 pyarrow==15.0.2 scipy coloredlogs
export PYTHONPATH=$PWD/abides/abides-core:$PWD/abides/abides-markets:$PWD
for f in ~/track3-simulation-public/regression_suite/scenarios/*.json; do
  u=$(basename "$f" .json); [ "$u" = index ] && continue
  .venv/bin/python -m abides_fork.simulate simulate --config "$f" --out "ref/$u/trace.parquet"
done
.venv/bin/python bench/compare.py --bin native/target/release/t3sim --ref ref
```

The submission image (`Dockerfile`, linux/amd64 only) builds the engine with profile-guided optimisation and copies only the static binary into an empty image.

## Known issues

- `bench/*.py` look for the official repo at `~/track3-simulation-public`; there is no flag for another path.
- The engine's parquet bytes differ from pandas' output. Content is identical, and each run's own files are byte-identical across repeats; compare content with `bench/compare.py`, not file hashes.
- The fallback needs `python` with ABIDES importable on `PATH`; without it an unsupported scenario fails with exit code 2.
- When a batch has at least as many sub-scenarios as cores, each gets one thread and runs about 17% slower than the same scenario alone.
- Before the Final we measured about 5.17 million events per second on the public units on an Intel machine of the graders' generation. The Final score was half that. The cause is unknown; Final logs are not published.

## Build with us

**Can an agent-based market simulator be exact and still fast enough for large studies?**

| If you like… | You could… |
|---|---|
| Rust performance | The event queue is still about 30% of the main thread. mimalloc and moving more state-free random draws off the main thread are untried. |
| Market microstructure | Port agent types and order paths beyond the four public agents; today they fall back to Python. |
| Testing | Extend `bench/fuzz.py` to new scenario shapes and keep the Python path as the oracle. |
| Tooling | Make the official-repo path a flag in `bench/`. |
| Batch runs | Share spare cores better when a batch has a few large sub-scenarios. |

Issues and pull requests are welcome.

## Development

```sh
cargo test --release --manifest-path native/Cargo.toml          # unit tests
python bench/compare.py --bin native/target/release/t3sim --ref <python outputs>   # content diff
python bench/fuzz.py --bin native/target/release/t3sim --n 200 --out <dir>         # random variants (needs Docker and the baseline image)
python bench/bench.py --image <image> --out <dir>                                  # Final-style container timing (needs Docker)
```

`bench/scale.py` and `bench/bscale.py` run scaled-up stress cases.

## Credits and license

The code written for this repo is MIT, © 2026 Yutai Lin ([LICENSE](LICENSE)). ABIDES is BSD-3-Clause, © Georgia Tech Research Corporation ([abides/LICENSE-ABIDES.txt](abides/LICENSE-ABIDES.txt)). The `abides_fork/` adapter, the Track 3 patches, and the scenarios and task cards under `native/pgo/` come from [Agenthon-2026/track3-simulation-public](https://github.com/Agenthon-2026/track3-simulation-public) (MIT). Details in [THIRD-PARTY-NOTICES.md](THIRD-PARTY-NOTICES.md). Agenthon 2026 is organised by the Society of Quantitative Analysts and CEWIT at Stony Brook University.
