// Test driver for output.rs.
// usage: outcheck <dump.bin> <out.parquet> [repeats=3]
// Writes the dumped columns with the Rust writer `repeats` times (same path), prints one JSON line:
// {"kind","rows","write_ms":[...],"sha256":"...","deterministic":bool}.
// Dump format (little endian), produced by tests_py/output_parity.py:
//   u8 kind ('T' trace | 'M' message_trace), u64 nrows, u64 dict_len, dict JSON {"msg_type":[..],"side":[..]},
//   then columns in file order. Ints: raw LE. Strings: u8 codes into dict (255 = null).
//   Nullable ints (t_send_ns, order_id, causal_parent): i64 values then u8 mask (1 = null).
#[path = "../output.rs"]
mod output;

use output::*;
use std::path::Path;
use std::time::Instant;

struct Rd<'a> {
    b: &'a [u8],
    p: usize,
}

impl<'a> Rd<'a> {
    fn take(&mut self, n: usize) -> &'a [u8] {
        let s = &self.b[self.p..self.p + n];
        self.p += n;
        s
    }
    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take(8).try_into().unwrap())
    }
    fn i64s(&mut self, n: usize) -> Vec<i64> {
        self.take(n * 8).chunks_exact(8).map(|c| i64::from_le_bytes(c.try_into().unwrap())).collect()
    }
    fn i32s(&mut self, n: usize) -> Vec<i32> {
        self.take(n * 4).chunks_exact(4).map(|c| i32::from_le_bytes(c.try_into().unwrap())).collect()
    }
    fn opt_i64s(&mut self, n: usize) -> Vec<Option<i64>> {
        let v = self.i64s(n);
        let mask = self.take(n);
        v.iter().zip(mask).map(|(x, m)| if *m == 1 { None } else { Some(*x) }).collect()
    }
    fn codes(&mut self, n: usize) -> Vec<u8> {
        self.take(n).to_vec()
    }
}

fn leak_dict(j: &serde_json::Value, key: &str) -> Vec<&'static str> {
    j[key].as_array().unwrap().iter().map(|s| &*Box::leak(s.as_str().unwrap().to_string().into_boxed_str())).collect()
}

fn main() {
    let a: Vec<String> = std::env::args().collect();
    let buf = std::fs::read(&a[1]).expect("read dump");
    let out = Path::new(&a[2]);
    let reps: usize = a.get(3).map(|s| s.parse().unwrap()).unwrap_or(3);
    let mut r = Rd { b: &buf, p: 0 };
    let kind = r.take(1)[0];
    let n = r.u64() as usize;
    let dl = r.u64() as usize;
    let dict: serde_json::Value = serde_json::from_slice(r.take(dl)).unwrap();
    let mt = leak_dict(&dict, "msg_type");
    let sd = leak_dict(&dict, "side");

    let mut times = vec![];
    let mut hashes = vec![];
    let write: Box<dyn Fn() -> std::io::Result<()>> = if kind == b'T' {
        let t = TraceCols {
            t_ns: r.i64s(n),
            agent_id: r.i32s(n),
            msg_type: r.codes(n).iter().map(|&c| mt[c as usize]).collect(),
            side: r.codes(n).iter().map(|&c| if c == 255 { None } else { Some(sd[c as usize]) }).collect(),
            price: r.i64s(n),
            size: r.i64s(n),
            order_id: r.i64s(n),
        };
        Box::new(move || write_trace(out, &t))
    } else {
        let m = MsgCols {
            seq: r.i64s(n),
            t_recv_ns: r.i64s(n),
            t_send_ns: r.opt_i64s(n),
            latency_ns: r.i64s(n),
            src_id: r.i32s(n),
            dst_id: r.i32s(n),
            message_id: r.i64s(n),
            msg_type: r.codes(n).iter().map(|&c| mt[c as usize]).collect(),
            order_id: r.opt_i64s(n),
            causal_parent: r.opt_i64s(n),
        };
        Box::new(move || write_message_trace(out, &m))
    };
    assert_eq!(r.p, buf.len(), "dump has trailing bytes");
    for _ in 0..reps {
        let t0 = Instant::now();
        write().expect("write");
        times.push(t0.elapsed().as_secs_f64() * 1e3);
        hashes.push(sha256_file(out).unwrap());
    }
    let det = hashes.iter().all(|h| *h == hashes[0]);
    println!(
        "{}",
        serde_json::json!({"kind": (kind as char).to_string(), "rows": n, "write_ms": times, "sha256": hashes[0], "deterministic": det})
    );
}
