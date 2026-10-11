#!/usr/bin/env python3
"""Bit-exact parity check: Rust rng.rs (via rngcheck) vs numpy RandomState.

Usage:
  rng_parity.py run --rust-bin PATH [--workdir DIR]   # gen scripts, run rust + numpy, compare
  rng_parity.py gen OUT.json                          # only write the scripts
  rng_parity.py compare SCRIPTS.json RUST.json        # run numpy on SCRIPTS and compare to RUST output
Floats are exchanged as IEEE-754 bit-pattern hex strings; ints as ints.
"""
import json, math, random, struct, subprocess, sys, os
import numpy as np


def hx(x):
    return "0x%016x" % struct.unpack("<Q", struct.pack("<d", x))[0]


def bits(x):
    return "%016x" % struct.unpack("<Q", struct.pack("<d", x))[0]


LN5E5 = math.log(5e5)
# (name, args-builder) pools used by the random mix; every f64 arg given as float.
def op_pool(rng):
    c = rng.random()
    if c < 0.18:
        return ["normal", hx(0.0), hx(0.0)]
    if c < 0.28:
        return ["normal", hx(1e5 + rng.uniform(-1e3, 1e3)), hx(math.sqrt(1000))]
    if c < 0.36:
        return ["normal", hx(1e5 + rng.uniform(-1e3, 1e3)), hx(rng.choice([1e-9, 1e-12, 1e-300]))]
    if c < 0.40:
        return ["normal", hx(rng.uniform(-5, 5)), hx(rng.uniform(0, 10))]
    if c < 0.48:
        return ["lognormal", hx(LN5E5), hx(rng.uniform(0.1, 2.0))]
    if c < 0.54:
        return ["uniform", hx(1e3), hx(2e6)]
    if c < 0.57:
        x = rng.uniform(1, 1e6)
        return ["uniform", hx(x), hx(x)]
    if c < 0.63:
        return ["pareto", hx(rng.uniform(1.1, 3.0))]
    if c < 0.68:
        return ["exponential", hx(1 / 2.77778e-18)]
    if c < 0.73:
        return ["exponential", hx(1 / 2e-9)]
    if c < 0.77:
        return ["exponential", hx(1.0)]
    if c < 0.80:
        return ["rand"]
    if c < 0.85:
        return ["u64"]
    hi_lo = rng.choice([(0, 2), (0, 1), (0, 3), (0, 6), (0, 11), (0, 101), (0, 2**31), (0, 2**31 + 5), (5, 17),
                        (0, 2**32), (0, 2**33), (-5, 2**40 + 3), (0, 2**63 - 1), (-2**63, 2**63 - 1)])
    return ["randint", hi_lo[0], hi_lo[1]]


def build_scripts():
    rng = random.Random(20261011)
    seeds = [0, 1, 42, 2**31, 2**32 - 1] + [rng.randrange(2**32) for _ in range(200)]
    scripts = []
    # A: targeted coverage per seed (all required families, gaussian cache interleaved)
    for s in seeds:
        ops = []
        for lo, hi in [(0, 2), (0, 1), (0, 3), (0, 6), (0, 11), (0, 101), (0, 2**31), (0, 2**31 + 5), (5, 17),
                       (0, 2**32), (0, 2**33), (-5, 2**40 + 3)]:
            ops += [["randint", lo, hi]] * 5
        ops += [["u64"]] * 5
        ops += [["normal", hx(1e5), hx(0.0)], ["randint", 0, 2], ["normal", hx(1e5), hx(math.sqrt(1000))],
                ["normal", hx(1e5), hx(1e-9)], ["normal", hx(0.0), hx(0.0)]] * 3
        for sg in [0.1, 0.5, 1.0, 2.0]:
            ops += [["lognormal", hx(LN5E5), hx(sg)], ["normal", hx(0.0), hx(1.0)]]
        ops += [["uniform", hx(1e3), hx(2e6)]] * 3 + [["uniform", hx(123.5), hx(123.5)]] * 2
        for a in [1.1, 1.5, 2.0, 3.0]:
            ops += [["pareto", hx(a)]] * 3
        ops += [["exponential", hx(1 / 2.77778e-18)], ["exponential", hx(1 / 2e-9)], ["exponential", hx(1.0)]] * 3
        ops += [["rand"]] * 3
        scripts.append({"seed": s, "ops": ops, "tag": "targeted"})
    # B: 50 seeds x 5000 random-mix calls
    for _ in range(50):
        s = rng.randrange(2**32)
        scripts.append({"seed": s, "ops": [op_pool(rng) for _ in range(5000)], "tag": "mix5000"})
    # C: seed chains (per-agent RNG derivation)
    for _ in range(20):
        s = rng.randrange(2**32)
        ops = []
        for _ in range(200):
            ops.append(["spawn"])
            ops += [op_pool(rng) for _ in range(20)]
            if rng.random() < 0.3:  # parent state keeps going too (its gauss cache must survive)
                ops += [["use", 0], ["normal", hx(0.0), hx(1.0)]]
        scripts.append({"seed": s, "ops": ops, "tag": "chain"})
    return scripts


def np_run(script):
    states = [np.random.RandomState(script["seed"])]
    cur = 0
    out = []
    for op in script["ops"]:
        n = op[0]
        if n == "spawn":
            sd = int(states[0].randint(low=0, high=2**32, dtype="uint64"))
            states.append(np.random.RandomState(sd))
            cur = len(states) - 1
            out.append(sd)
            continue
        if n == "use":
            cur = op[1]
            out.append(None)
            continue
        rs = states[cur]
        F = lambda h: struct.unpack("<d", struct.pack("<Q", int(h, 16)))[0]
        if n == "normal": r = rs.normal(F(op[1]), F(op[2]))
        elif n == "lognormal": r = rs.lognormal(F(op[1]), F(op[2]))
        elif n == "uniform": r = rs.uniform(F(op[1]), F(op[2]))
        elif n == "pareto": r = rs.pareto(F(op[1]))
        elif n == "exponential": r = rs.exponential(F(op[1]))
        elif n == "rand": r = rs.random_sample()
        elif n == "u64": r = int(rs.randint(low=0, high=2**32, dtype="uint64"))
        elif n == "randint": r = int(rs.randint(op[1], op[2]))
        else: raise SystemExit("unknown op " + n)
        out.append(bits(r) if isinstance(r, (float, np.floating)) else r)
    return out


def compare(scripts, rust):
    assert len(scripts) == len(rust), "script count mismatch"
    total_ops = mism = 0
    by_tag = {}
    for si, (sc, rr) in enumerate(zip(scripts, rust)):
        ref = np_run(sc)
        assert len(ref) == len(rr) == len(sc["ops"]), "op count mismatch"
        t = by_tag.setdefault(sc["tag"], [0, 0, 0])
        t[0] += 1
        t[1] += len(ref)
        for i, (a, b) in enumerate(zip(ref, rr)):
            total_ops += 1
            if a != b:
                mism += 1
                t[2] += 1
                if mism == 1:
                    print(f"FIRST MISMATCH script#{si} tag={sc['tag']} seed={sc['seed']} op#{i} {sc['ops'][i]}")
                    print(f"  numpy={a}  rust={b}")
                    for j in range(max(0, i - 3), i):
                        print(f"  ctx op#{j} {sc['ops'][j]} numpy={ref[j]} rust={rr[j]}")
    for tag, (n, o, m) in by_tag.items():
        print(f"{tag}: scripts={n} ops={o} mismatches={m}")
    print(f"TOTAL: scripts={len(scripts)} ops={total_ops} mismatches={mism} numpy={np.__version__}")
    return mism


def main():
    cmd = sys.argv[1]
    if cmd == "gen":
        json.dump({"scripts": build_scripts()}, open(sys.argv[2], "w"))
    elif cmd == "compare":
        scripts = json.load(open(sys.argv[2]))["scripts"]
        rust = json.load(open(sys.argv[3]))
        sys.exit(1 if compare(scripts, rust) else 0)
    elif cmd == "run":
        binp = sys.argv[sys.argv.index("--rust-bin") + 1]
        wd = sys.argv[sys.argv.index("--workdir") + 1] if "--workdir" in sys.argv else "/tmp"
        sp, rp = os.path.join(wd, "rng_scripts.json"), os.path.join(wd, "rng_rust.json")
        json.dump({"scripts": build_scripts()}, open(sp, "w"))
        with open(rp, "w") as f:
            subprocess.run([binp, sp], stdout=f, check=True)
        sys.exit(1 if compare(json.load(open(sp))["scripts"], json.load(open(rp))) else 0)
    else:
        raise SystemExit(__doc__)


if __name__ == "__main__":
    main()
