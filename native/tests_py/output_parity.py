"""Parity test for src/output.rs against pandas/pyarrow-written reference parquet files.

Two phases (so the same Rust outputs can be checked under several pandas versions; run `gen` under the baseline
pandas 1.5.3 / pyarrow 15 env so the extra-case references are what the Python baseline would write):
  gen   (needs pandas+numpy): read reference parquet -> dump columns -> run `outcheck` -> ours.parquet
  check (any env): assert pd.read_parquet(ours) is IDENTICAL to pd.read_parquet(ref), plus arrow/parquet schema.

  python output_parity.py gen   --refs ~/runs/b_official --work ~/outwork/parity --outcheck target/release/outcheck [--all]
  python output_parity.py check --refs ~/runs/b_official --work ~/outwork/parity [--all]

Extra cases (written by pandas in the gen env as their own reference): empty frames, and a trace with null `side`.
"""
import argparse, glob, hashlib, json, os, struct, subprocess, sys
import numpy as np
import pandas as pd

DEFAULT = ["s001_price_time_priority", "gb_mega_throughput", "as01_base_mix", "ca_baseline_calm",
           "eq_uniform_wide", "mp01_stp_newest_baseline", "mr_deep_book_state_size", "gb_pop_128_agents",
           "st05_latency_spike_pareto", "s012_partial_fill_cancel_race", "fastlob_core", "stp_cancel_newest"]
FILES = {"trace": "T", "message_trace": "M"}
TRACE_DT = dict(t_ns="int64", agent_id="int32", msg_type="string", side="string", price="int64", size="int64", order_id="int64")
MSG_DT = dict(seq="int64", t_recv_ns="int64", t_send_ns="Int64", latency_ns="int64", src_id="int32", dst_id="int32",
              message_id="int64", msg_type="string", order_id="Int64", causal_parent="Int64")
DT = {"trace": TRACE_DT, "message_trace": MSG_DT}


def dump(df, kind, path):
    exp = DT[kind]
    assert list(df.columns) == list(exp), list(df.columns)
    for c, t in exp.items():
        assert str(df[c].dtype) == t, (c, df[c].dtype)
    n = len(df)
    codes_of = {}
    for c in ("msg_type", "side"):
        if c in df:
            cd, un = pd.factorize(df[c])
            codes_of[c] = (np.where(cd < 0, 65535, cd).astype("<u2"), [str(u) for u in un])
    d = {"msg_type": codes_of["msg_type"][1], "side": codes_of.get("side", (None, []))[1]}
    dj = json.dumps(d).encode()
    out = [struct.pack("<BQQ", ord(FILES[kind]), n, len(dj)), dj]
    for c, t in exp.items():
        s = df[c]
        if t in ("int64", "int32"):
            out.append(s.to_numpy(dtype=t).tobytes())  # host is little-endian
        elif t == "Int64":
            out.append(s.fillna(0).to_numpy(dtype="int64").tobytes())
            out.append(s.isna().to_numpy().astype("uint8").tobytes())
        else:
            out.append(codes_of[c][0].tobytes())
    open(path, "wb").write(b"".join(out))


def run_case(name, df, kind, work, outcheck, refs, ref_path=None):
    d = os.path.join(work, name)
    os.makedirs(d, exist_ok=True)
    if ref_path is None:  # extra case: pandas writes its own reference (run gen under the baseline env: pandas 1.5.3)
        ref_path = os.path.join(d, kind + ".ref.parquet")
        df.to_parquet(ref_path, compression="snappy", index=False)
        ref_rel = "work:" + name + "/" + kind + ".ref.parquet"
    else:
        ref_rel = "refs:" + os.path.relpath(ref_path, refs)
    dump(df, kind, os.path.join(d, kind + ".bin"))
    ours = os.path.join(d, kind + ".parquet")
    r = subprocess.run([outcheck, os.path.join(d, kind + ".bin"), ours, "5"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    st = json.loads(r.stdout)
    assert st["deterministic"], (name, kind)
    assert st["sha256"] == hashlib.sha256(open(ours, "rb").read()).hexdigest()  # sha256_file == hashlib
    json.dump(dict(st, ref=ref_rel), open(os.path.join(d, kind + ".json"), "w"))
    return st


def cases(a):
    names = sorted(os.listdir(a.refs)) if a.all else DEFAULT
    names = [n for n in names if os.path.isdir(os.path.join(a.refs, n))]
    for n in names:
        for kind in FILES:
            yield n, kind, os.path.join(a.refs, n, kind + ".parquet")


def extremes(kind, n):
    """Full-range ints (delta overflow / 64-bit widths), runs, many-distinct strings (dict index width > 8), nulls."""
    rng = np.random.default_rng(7)
    i64 = lambda: rng.integers(-2**63, 2**63 - 1, n, dtype=np.int64, endpoint=True)
    i32 = lambda: rng.integers(-2**31, 2**31 - 1, n, dtype=np.int32, endpoint=True)
    alt = np.where(np.arange(n) % 2 == 0, np.iinfo(np.int64).min, np.iinfo(np.int64).max).astype(np.int64)
    mt = pd.Series([f"type_{i % 3001}" for i in range(n)], dtype="string")
    nul = lambda x: pd.Series(x, dtype="Int64").mask(rng.random(n) < 0.3)
    if kind == "trace":
        side = pd.Series(rng.choice(["BID", "ASK", "x\u00e9\u4e2d"], n), dtype="string").mask(rng.random(n) < 0.2)
        return pd.DataFrame({"t_ns": i64(), "agent_id": i32(), "msg_type": mt, "side": side, "price": alt,
                             "size": np.full(n, 5, dtype=np.int64), "order_id": np.cumsum(rng.integers(0, 3, n)).astype(np.int64)})
    return pd.DataFrame({"seq": np.arange(n, dtype=np.int64), "t_recv_ns": i64(), "t_send_ns": nul(i64()),
                         "latency_ns": alt, "src_id": i32(), "dst_id": np.full(n, -7, dtype=np.int32), "message_id": i64(),
                         "msg_type": mt, "order_id": nul(i64()), "causal_parent": nul(np.cumsum(rng.integers(0, 3, n)).astype(np.int64))})


def gen(a):
    for n, kind, p in cases(a):
        df = pd.read_parquet(p)
        st = run_case(n, df, kind, a.work, a.outcheck, a.refs, ref_path=p)
        print(f"gen {n}/{kind} rows={st['rows']} write_ms(min)={min(st['write_ms']):.1f}")
    # extra: empty frames and null sides (reference written by pandas in this env)
    for kind in FILES:
        base = pd.read_parquet(os.path.join(a.refs, "s001_price_time_priority", kind + ".parquet"))
        run_case("extra_empty", base.head(0), kind, a.work, a.outcheck, a.refs)
        run_case("extra_extremes", extremes(kind, 200_003), kind, a.work, a.outcheck, a.refs)
        if kind == "trace":
            nb = base.copy()
            nb.loc[nb.index[::7], "side"] = pd.NA
            run_case("extra_null_side", nb, kind, a.work, a.outcheck, a.refs)
            nb = nb.iloc[::-1].reset_index(drop=True)
            nb.loc[:, "price"] = -nb["price"]  # negatives
            run_case("extra_null_side_neg", nb, kind, a.work, a.outcheck, a.refs)
        else:
            nb = base.copy()
            nb.loc[nb.index[::3], ["t_send_ns", "order_id", "causal_parent"]] = pd.NA
            run_case("extra_null_int", nb, kind, a.work, a.outcheck, a.refs)


def check(a):
    import pyarrow.parquet as pq
    print("pandas", pd.__version__)
    ok = 0
    for d in sorted(glob.glob(os.path.join(a.work, "*"))):
        for kind in FILES:
            j = os.path.join(d, kind + ".json")
            if not os.path.exists(j):
                continue
            st = json.load(open(j))
            ours = os.path.join(d, kind + ".parquet")
            where, rel = st["ref"].split(":", 1)  # paths differ between envs (docker mounts)
            ref = os.path.join(a.refs if where == "refs" else a.work, rel)
            do, dr = pd.read_parquet(ours), pd.read_parquet(ref)
            pd.testing.assert_frame_equal(do, dr, check_dtype=True, check_index_type=True)
            assert list(do.dtypes.astype(str)) == list(dr.dtypes.astype(str)) and list(do.columns) == list(dr.columns)
            assert do.index.equals(dr.index) and type(do.index) is type(dr.index), (do.index, dr.index)
            so, sr = pq.read_schema(ours), pq.read_schema(ref)
            assert so.equals(sr, check_metadata=True), (so, sr)
            assert [(f.name, str(f.type), f.nullable) for f in so] == [(f.name, str(f.type), f.nullable) for f in sr]
            fo, fr = pq.ParquetFile(ours), pq.ParquetFile(ref)
            assert fo.schema.equals(fr.schema), (fo.schema, fr.schema)
            assert fo.metadata.num_rows == fr.metadata.num_rows
            assert [fo.metadata.row_group(i).num_rows for i in range(fo.metadata.num_row_groups)] == \
                   [fr.metadata.row_group(i).num_rows for i in range(fr.metadata.num_row_groups)] or len(dr) == 0
            assert pq.read_table(ours).equals(pq.read_table(ref))
            ok += 1
    print(f"check OK: {ok} files identical")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("phase", choices=["gen", "check"])
    ap.add_argument("--refs", default=os.path.expanduser("~/runs/b_official"))
    ap.add_argument("--work", default=os.path.expanduser("~/outwork/parity"))
    ap.add_argument("--outcheck", default="target/release/outcheck")
    ap.add_argument("--all", action="store_true")
    a = ap.parse_args()
    (gen if a.phase == "gen" else check)(a)
