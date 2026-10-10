# agenthon-t3

Agenthon 2026 Track 3 submission: ABIDES (pinned `f9cbe51` + the four official Track 3 patches,
vendored under `abides/`) with speedups that leave `trace.parquet` and `message_trace.parquet`
byte-identical to the official baseline image on all 65 public scenarios.

- `Dockerfile` — submission image (`simulate`, `simulate-batch` verbs).
- `bench/bench.py` — runs the public scenarios, times each container the way the Final ranks it,
  and checks output hashes against a baseline run.
- `.github/workflows/image.yml` — builds and pushes `ghcr.io/longado/agenthon-t3:<sha>`.
