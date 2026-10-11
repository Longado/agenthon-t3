// Test driver: rngcheck <script.json>  -> prints JSON array of results (f64 as bit-pattern hex strings).
// Script: {"seed":u32, "ops":[["normal",loc,scale], ["randint",lo,hi], ["u64"], ["rand"], ["seed_chain"...]]}
// Multi-state scripts: {"scripts":[{...}, ...]} -> array of result arrays.
// f64 args are passed as hex bit strings ("0x...") to avoid decimal round-trip error; ints as JSON ints.
#[path = "../rng.rs"]
mod rng;
use rng::RandomState;
use serde_json::{json, Value};

fn f(v: &Value) -> f64 {
    let s = v.as_str().expect("f64 arg must be hex string");
    f64::from_bits(u64::from_str_radix(s.trim_start_matches("0x"), 16).expect("bad hex"))
}
fn h(x: f64) -> Value {
    json!(format!("{:016x}", x.to_bits()))
}

fn run(script: &Value) -> Value {
    // states[0] is the parent; ["spawn"] seeds a new child from the parent's uint64 randint and selects it; ["use",i] selects state i.
    let mut states = vec![RandomState::new(script["seed"].as_u64().expect("seed") as u32)];
    let mut cur = 0usize;
    let mut out = Vec::new();
    for op in script["ops"].as_array().expect("ops") {
        let a = op.as_array().unwrap();
        let name = a[0].as_str().unwrap();
        if name == "spawn" {
            let sd = states[0].randint_u32_as_u64();
            states.push(RandomState::new(sd as u32));
            cur = states.len() - 1;
            out.push(json!(sd));
            continue;
        }
        if name == "use" {
            cur = a[1].as_u64().unwrap() as usize;
            out.push(json!(null));
            continue;
        }
        let rs = &mut states[cur];
        let r = match name {
            "u32" => json!(rs.next_u32()),
            "rand" => h(rs.random_sample()),
            "u64" => json!(rs.randint_u32_as_u64()),
            "randint" => json!(rs.randint(a[1].as_i64().unwrap(), a[2].as_i64().unwrap())),
            "normal" => h(rs.normal(f(&a[1]), f(&a[2]))),
            "lognormal" => h(rs.lognormal(f(&a[1]), f(&a[2]))),
            "uniform" => h(rs.uniform(f(&a[1]), f(&a[2]))),
            "pareto" => h(rs.pareto(f(&a[1]))),
            "exponential" => h(rs.exponential(f(&a[1]))),
            _ => panic!("unknown op {name}"),
        };
        out.push(r);
    }
    Value::Array(out)
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: rngcheck script.json");
    let v: Value = serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
    let res: Vec<Value> = v["scripts"].as_array().unwrap().iter().map(run).collect();
    println!("{}", Value::Array(res));
}
