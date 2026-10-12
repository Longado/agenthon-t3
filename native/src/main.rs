//! `simulate` / `simulate-batch` for the Track 3 image. Runs the native engine when the scenario
//! is fully supported; otherwise replaces itself with the Python ABIDES baseline (same arguments).

// Shared by t3sim (page-level streaming writer) and outcheck (whole-column writer).
#[allow(dead_code)]
mod output;
mod queue;
mod rng;
mod scenario;
mod sim;

use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{mpsc, Mutex};
use std::time::Instant;

use serde_json::{json, Value};

/// Hand the whole invocation to the Python baseline adapter; never returns on success.
fn fallback(module: &str, args: &[String], why: &str) -> ! {
    if std::env::var_os("T3SIM_NO_FALLBACK").is_some() {
        eprintln!("t3sim: unsupported ({why})");
        std::process::exit(3);
    }
    eprintln!("t3sim: falling back to python ({why})");
    let err = Command::new("python").arg("-m").arg(module).args(args).exec();
    eprintln!("t3sim: cannot exec python: {err}");
    std::process::exit(2);
}

fn peak_rss_bytes() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|kb| kb.parse::<u64>().ok())
        })
        .map(|kb| kb * 1024)
        .unwrap_or(0)
}

struct Prepared {
    cfg: scenario::Config,
    seed: i64,
}

fn prepare(config_path: &Path, seed: Option<i64>) -> Result<Prepared, String> {
    let text = std::fs::read_to_string(config_path).map_err(|e| format!("read {}: {e}", config_path.display()))?;
    let v: Value = serde_json::from_str(&text).map_err(|e| format!("json: {e}"))?;
    let cfg = scenario::build(&v, seed).map_err(|u| u.0)?;
    Ok(Prepared { seed: cfg.seed as i64, cfg })
}

/// Run one prepared scenario and write trace.parquet, events.json and, when `ledger`,
/// message_trace.parquet. `threads` is this scenario's CPU budget; `leak` skips freeing the
/// simulation state (the process is about to exit, and unmapping hundreds of MB is measurable).
/// Ok(None): the run emitted trace events out of time order, its partial files were removed, and
/// the caller must fall back.
fn run_one(p: Prepared, trace_path: &Path, threads: usize, leak: bool, ledger: bool) -> std::io::Result<Option<Value>> {
    let t0 = Instant::now();
    let scenario_id = p.cfg.scenario_id.clone();
    let dir = trace_path.parent().map(Path::to_path_buf).unwrap_or_else(|| PathBuf::from("."));
    std::fs::create_dir_all(&dir)?;
    let msg_path = dir.join("message_trace.parquet");
    let trace_file = sim::trace_file(trace_path)?;
    let msg_file = if ledger { Some(sim::message_file(&msg_path)?) } else { None };
    let mut s = sim::Sim::new(p.cfg);
    let mut t_sim = Default::default();
    // Trace segments and message_trace pages are encoded by `threads - 1` helpers while the
    // simulation runs (the main thread encodes whatever is left once it finishes), and one more
    // thread writes them to the files in row order as they come.
    let (tx, rx) = mpsc::channel();
    let rx = Mutex::new(rx);
    let (done_tx, done_rx) = mpsc::channel();
    let streamed = std::thread::scope(|sc| {
        let writer = sc.spawn(move || sim::write_ahead(done_rx, trace_file, msg_file));
        for _ in 1..threads {
            let (rx, done_tx) = (&rx, done_tx.clone());
            sc.spawn(move || sim::work(rx, &done_tx));
        }
        s.run(tx, ledger, threads > 1);
        t_sim = t0.elapsed();
        sim::work(&rx, &done_tx);
        drop(done_tx);
        writer.join().expect("file writer panicked")
    })?;
    if s.disordered() {
        drop(streamed);
        let _ = std::fs::remove_file(trace_path);
        let _ = std::fs::remove_file(&msg_path);
        return Ok(None);
    }
    let t_run = t0.elapsed();
    let w = s.write_outputs(streamed, threads)?;
    if std::env::var_os("T3SIM_TIMING").is_some() {
        eprintln!("t3sim timing: sim {:?} run {:?} write {:?} (threads {threads})", t_sim, t_run, t0.elapsed() - t_run);
    }
    if leak {
        std::mem::forget(s);
    }
    let wall = t0.elapsed().as_secs_f64();
    let mut events = json!({
        "scenario_id": scenario_id,
        "seed": p.seed,
        "n_events": w.n_events,
        "wall_clock_sec": wall,
        "events_per_sec": if wall > 0.0 { w.n_events as f64 / wall } else { 0.0 },
        "trace_sha256": w.trace_sha,
        "peak_memory_bytes": peak_rss_bytes(),
        "gpu_seconds": 0.0,
    });
    if let (Some(n), Some(sha)) = (w.n_messages, w.message_sha) {
        events["n_messages"] = json!(n);
        events["message_trace_sha256"] = json!(sha);
    }
    std::fs::write(dir.join("events.json"), serde_json::to_string_pretty(&events)? + "\n")?;
    Ok(Some(events))
}

/// Whether the unit's card asks for message_trace.parquet: `[scoring.params]
/// requires_message_ledger = false` is the only thing that turns it off, so a missing or
/// unreadable card keeps the ledger.
fn ledger_required(config: &Path) -> bool {
    let card = config.parent().unwrap_or(Path::new(".")).join("card.toml");
    std::fs::read_to_string(card).map(|t| ledger_in_card(&t)).unwrap_or(true)
}

fn ledger_in_card(card: &str) -> bool {
    let mut in_params = false;
    for line in card.lines() {
        let l = line.split('#').next().unwrap_or("").trim();
        if l.starts_with('[') {
            in_params = l == "[scoring.params]";
        } else if let (true, Some((k, v))) = (in_params, l.split_once('=')) {
            if k.trim() == "requires_message_ledger" {
                return v.trim() != "false";
            }
        }
    }
    true
}

fn cpu_budget() -> usize {
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(1)
}

fn arg_value(args: &[String], flag: &str) -> Option<String> {
    let eq = format!("{flag}=");
    let mut it = args.iter();
    while let Some(a) = it.next() {
        if a == flag {
            return it.next().cloned();
        }
        if let Some(v) = a.strip_prefix(&eq) {
            return Some(v.to_string());
        }
    }
    None
}

fn cmd_simulate(args: &[String]) {
    let py = "abides_fork.simulate";
    let (Some(config), Some(out)) = (arg_value(args, "--config"), arg_value(args, "--out")) else {
        fallback(py, args, "missing --config/--out");
    };
    let seed = match arg_value(args, "--seed") {
        Some(s) => match s.parse::<i64>() {
            Ok(v) => Some(v),
            Err(_) => fallback(py, args, "unparsed --seed"),
        },
        None => None,
    };
    let prepared = match prepare(Path::new(&config), seed) {
        Ok(p) => p,
        Err(why) => fallback(py, args, &why),
    };
    let ledger = ledger_required(Path::new(&config));
    match run_one(prepared, Path::new(&out), cpu_budget(), true, ledger) {
        Ok(Some(ev)) => println!("{ev}"),
        Ok(None) => fallback(py, args, "trace events out of time order"),
        Err(e) => {
            eprintln!("t3sim: write failed: {e}");
            std::process::exit(1);
        }
    }
}

fn cmd_batch(args: &[String]) {
    let py = "abides_fork.simulate_batch";
    let (Some(batch_dir), Some(out_dir)) = (arg_value(args, "--batch-dir"), arg_value(args, "--out-dir")) else {
        fallback(py, args, "missing --batch-dir/--out-dir");
    };
    let t0 = Instant::now();
    let mut subs: Vec<PathBuf> = match std::fs::read_dir(&batch_dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
            .collect(),
        Err(e) => fallback(py, args, &format!("read batch dir: {e}")),
    };
    // Python sorts the paths; within one directory that is byte order of the file names.
    subs.sort();
    if subs.is_empty() {
        fallback(py, args, "no sub-scenarios");
    }
    let mut prepared = Vec::with_capacity(subs.len());
    for p in &subs {
        match prepare(p, None) {
            Ok(x) => prepared.push(x),
            Err(why) => fallback(py, args, &format!("{}: {why}", p.display())),
        }
    }
    let out_dir = PathBuf::from(out_dir);
    let names: Vec<String> = subs.iter().map(|p| p.file_stem().unwrap().to_string_lossy().into_owned()).collect();
    let workers = cpu_budget().min(prepared.len());
    // Subs are independent simulations; run them on all available cores, results kept in sub order.
    let jobs: Vec<(usize, Prepared)> = prepared.into_iter().enumerate().collect();
    let queue = std::sync::Mutex::new(jobs.into_iter());
    let results: Vec<std::sync::Mutex<Option<std::io::Result<Option<Value>>>>> = (0..names.len()).map(|_| std::sync::Mutex::new(None)).collect();
    std::thread::scope(|sc| {
        for _ in 0..workers {
            sc.spawn(|| loop {
                let next = queue.lock().unwrap().next();
                let Some((i, p)) = next else { break };
                // Batch units need a ledger per sub whatever the card says.
                let r = run_one(p, &out_dir.join(&names[i]).join("trace.parquet"), 1, false, true);
                *results[i].lock().unwrap() = Some(r);
            });
        }
    });
    let mut per = Vec::new();
    let mut total: u64 = 0;
    for (i, r) in results.into_iter().enumerate() {
        match r.into_inner().unwrap().unwrap() {
            Ok(None) => fallback(py, args, &format!("{}: trace events out of time order", names[i])),
            Ok(Some(ev)) => {
                let n = ev["n_events"].as_u64().unwrap();
                total += n;
                per.push(json!({"sub": names[i], "n_events": n, "trace_sha256": ev["trace_sha256"]}));
            }
            Err(e) => {
                eprintln!("t3sim: sub {} failed: {e}", names[i]);
                std::process::exit(1);
            }
        }
    }
    let wall = t0.elapsed().as_secs_f64();
    let agg = json!({
        "n_scenarios": names.len(),
        "total_events": total,
        "wall_clock_sec": wall,
        "events_per_sec": if wall > 0.0 { total as f64 / wall } else { 0.0 },
        "peak_memory_bytes": peak_rss_bytes(),
        "gpu_seconds": 0.0,
        "per_scenario": per,
    });
    if let Err(e) = std::fs::create_dir_all(&out_dir)
        .and_then(|_| std::fs::write(out_dir.join("batch_events.json"), serde_json::to_string_pretty(&agg).unwrap() + "\n"))
    {
        eprintln!("t3sim: write batch_events.json: {e}");
        std::process::exit(1);
    }
    println!("{agg}");
}

/// Panics hand the invocation to Python instead of aborting: a native bug on an unforeseen
/// scenario then costs speed, not the unit. (exec from any thread replaces the whole process.)
fn fallback_on_panic(module: &'static str, args: Vec<String>) {
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("t3sim: {info}");
        fallback(module, &args, "native engine panicked");
    }));
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let prog = Path::new(&argv[0]).file_name().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
    let mut args: Vec<String> = argv[1..].to_vec();
    // Accept both `simulate --config ...` (verb as argument) and a binary named after the verb.
    let verb = match args.first().map(String::as_str) {
        Some("simulate") | Some("simulate-batch") => args.remove(0),
        _ => prog,
    };
    match verb.as_str() {
        "simulate-batch" => {
            let mut a = vec!["simulate-batch".to_string()];
            a.extend(args);
            fallback_on_panic("abides_fork.simulate_batch", a.clone());
            cmd_batch(&a)
        }
        _ => {
            let mut a = vec!["simulate".to_string()];
            a.extend(args);
            fallback_on_panic("abides_fork.simulate", a.clone());
            cmd_simulate(&a)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ledger_in_card;

    #[test]
    fn card_ledger_flag() {
        let off = "[scoring]\nmetric = \"x\"\n\n[scoring.params]\nsemantic_tier = \"A\"\nrequires_message_ledger = false  # throughput unit\n";
        assert!(!ledger_in_card(off));
        assert!(ledger_in_card(&off.replace("= false", "= true")));
        // Only [scoring.params] counts; the same key elsewhere is not the card's setting.
        assert!(ledger_in_card("[scoring]\nrequires_message_ledger = false\n[scoring.params]\nx = 1\n"));
        assert!(ledger_in_card("[scoring.params]\nsemantic_tier = \"B\"\n"));
        assert!(ledger_in_card(""));
    }
}
