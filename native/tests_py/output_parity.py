"""Parity test for src/output.rs against pandas/pyarrow-written reference parquet files.

Two phases (so the same Rust outputs can be checked under several pandas versions):
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
            codes_of[c] = (np.where(cd < 0, 255, cd).astype("uint8"), [str(u) for u in un])
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


def run_case(name, df, kind, work, outcheck, ref_path=None):
    d = os.path.join(work, name)
    os.makedirs(d, exist_ok=True)
    if ref_path is None:  # extra case: pandas writes its own reference
        ref_path = os.path.join(d, kind + ".ref.parquet")
        df.to_parquet(ref_path, compression="snappy", index=False)
    dump(df, kind, os.path.join(d, kind + ".bin"))
    ours = os.path.join(d, kind + ".parquet")
    r = subprocess.run([outcheck, os.path.join(d, kind + ".bin"), ours, "5"], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    st = json.loads(r.stdout)
    assert st["deterministic"], (name, kind)
    assert st["sha256"] == hashlib.sha256(open(ours, "rb").read()).hexdigest()  # sha256_file == hashlib
    json.dump(dict(st, ref=ref_path), open(os.path.join(d, kind + ".json"), "w"))
    return st


def cases(a):
    names = sorted(os.listdir(a.refs)) if a.all else DEFAULT
    names = [n for n in names if os.path.isdir(os.path.join(a.refs, n))]
    for n in names:
        for kind in FILES:
            yield n, kind, os.path.join(a.refs, n, kind + ".parquet")


def gen(a):
    for n, kind, p in cases(a):
        df = pd.read_parquet(p)
        st = run_case(n, df, kind, a.work, a.outcheck, ref_path=p)
        print(f"gen {n}/{kind} rows={st['rows']} write_ms(min)={min(st['write_ms']):.1f}")
    # extra: empty frames and null sides (reference written by pandas in this env)
    for kind in FILES:
        base = pd.read_parquet(os.path.join(a.refs, "s001_price_time_priority", kind + ".parquet"))
        run_case("extra_empty", base.head(0), kind, a.work, a.outcheck)
        if kind == "trace":
            nb = base.copy()
            nb.loc[nb.index[::7], "side"] = pd.NA
            run_case("extra_null_side", nb, kind, a.work, a.outcheck)
            nb = nb.iloc[::-1].reset_index(drop=True)
            nb.loc[:, "price"] = -nb["price"]  # negatives
            run_case("extra_null_side_neg", nb, kind, a.work, a.outcheck)
        else:
            nb = base.copy()
            nb.loc[nb.index[::3], ["t_send_ns", "order_id", "causal_parent"]] = pd.NA
            run_case("extra_null_int", nb, kind, a.work, a.outcheck)


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
            ours, ref = os.path.join(d, kind + ".parquet"), st["ref"]
            if not os.path.exists(ref):  # reference moved between envs (docker mount): re-resolve
                ref = os.path.join(a.refs, os.path.basename(os.path.dirname(ref)), kind + ".parquet")
            do, dr = pd.read_parquet(ours), pd.read_parquet(ref)
            pd.testing.assert_frame_equal(do, dr, check_dtype=True, check_index_type=True)
            assert list(do.dtypes.astype(str)) == list(dr.dtypes.astype(str)) and list(do.columns) == list(dr.columns)
            assert do.index.equals(dr.index) and type(do.index) is type(dr.index), (do.index, dr.index)
            so, sr = pq.read_schema(ours), pq.read_schema(ref)
            assert so.equals(sr, check_metadata=True), (so, sr)
            assert [(f.name, str(f.type), f.nullable) for f in so] == [(f.name, str(f.type), f.nullable) for f in sr]
            fo, fr = pq.ParquetFile(ours), pq.ParquetFile(ref)
            assert str(fo.schema) == str(fr.schema), (fo.schema, fr.schema)
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
