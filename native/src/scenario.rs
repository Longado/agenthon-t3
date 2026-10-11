//! Scenario JSON -> simulation config, mirroring `abides_fork/config.py` and the agent
//! constructors in `abides_fork/agents.py`, including Python's int()/float()/max() conversions.
//! Anything the native engine does not reproduce exactly returns `Unsupported`, and the caller
//! hands the run to the Python baseline instead.

use serde_json::Value;

#[derive(Debug)]
pub struct Unsupported(pub String);

fn unsup<T>(why: impl Into<String>) -> Result<T, Unsupported> {
    Err(Unsupported(why.into()))
}

/// A JSON number as Python sees it: int or float.
#[derive(Clone, Copy, Debug)]
pub enum PyNum {
    I(i64),
    F(f64),
}

impl PyNum {
    fn from_value(v: &Value) -> Result<PyNum, Unsupported> {
        match v {
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Ok(PyNum::I(i))
                } else if n.is_u64() {
                    unsup("integer beyond int64")
                } else {
                    Ok(PyNum::F(n.as_f64().unwrap()))
                }
            }
            Value::Bool(b) => Ok(PyNum::I(*b as i64)),
            _ => unsup(format!("non-numeric value {v}")),
        }
    }
    /// Python int(x).
    pub fn to_int(self) -> Result<i64, Unsupported> {
        match self {
            PyNum::I(i) => Ok(i),
            PyNum::F(f) => {
                if !f.is_finite() || f.abs() >= 9.2e18 {
                    return unsup("float out of int range");
                }
                Ok(f.trunc() as i64)
            }
        }
    }
    /// Python float(x).
    pub fn to_float(self) -> f64 {
        match self {
            PyNum::I(i) => i as f64,
            PyNum::F(f) => f,
        }
    }
    fn gt(self, other: PyNum) -> bool {
        // Only used on small agent parameters, where f64 comparison is exact.
        self.to_float() > other.to_float()
    }
}

/// Python max(a, b): returns `a` unless `b` is strictly greater.
fn py_max(a: PyNum, b: PyNum) -> PyNum {
    if b.gt(a) {
        b
    } else {
        a
    }
}

fn get<'a>(obj: &'a Value, key: &str) -> Option<&'a Value> {
    obj.as_object().and_then(|m| m.get(key))
}

fn num_or(obj: &Value, key: &str, default: PyNum) -> Result<PyNum, Unsupported> {
    match get(obj, key) {
        Some(v) => PyNum::from_value(v),
        None => Ok(default),
    }
}

/// Python truthiness of a JSON value.
fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) => false,
        Some(Value::Bool(b)) => *b,
        Some(Value::Number(n)) => n.as_f64().map(|f| f != 0.0).unwrap_or(true),
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
    }
}

#[derive(Clone, Debug)]
pub enum AgentKind {
    Noise { size_mean: f64, size_std: f64, offset_ticks: i64, reference_price: i64 },
    MarketMaker { spread_ticks: i64, depth_levels: i64, size_per_level: i64, reference_price: i64 },
    Value { size_mean: f64, threshold_ticks: i64, sigma_n: f64 },
    Momentum { size_mean: f64, threshold_ticks: i64, lookback: i64 },
}

#[derive(Clone, Debug)]
pub struct AgentCfg {
    pub kind: AgentKind,
    pub interval_ns: i64,
}

#[derive(Clone, Debug)]
pub enum LatencyKind {
    LogNormal { mu: f64, sigma: f64 },
    Uniform,
    Pareto { alpha: f64 },
    Constant,
}

#[derive(Clone, Debug)]
pub struct LatencyCfg {
    pub kind: LatencyKind,
    pub mean_ns: f64,
    pub min_ns: f64,
    pub max_ns: f64,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub scenario_id: String,
    pub seed: u32,
    pub date_ns: i64,
    pub mkt_open: i64,
    pub mkt_close: i64,
    pub oracle_close: i64,
    pub stop_time: i64,
    pub r_bar: i64,
    pub kappa: f64,
    pub fund_vol: f64,
    pub megashock_lambda_a: f64,
    pub megashock_mean: f64,
    pub megashock_var: f64,
    pub scheduled_jump: Option<(i64, i64)>,
    pub stp_cancel_oldest: Option<bool>, // None = no STP; Some(true) = cancel_oldest; Some(false) = any other policy
    pub pipeline_delay: i64,
    pub compute_delay: i64,
    pub agents: Vec<AgentCfg>,
    pub latency: LatencyCfg,
}

// pd.to_datetime("20210205").value, str_to_ns("09:30:00"), str_to_ns("16:00:00"), str_to_ns("1s")
const DATE_NS: i64 = 1_612_483_200_000_000_000;
const NS_0930: i64 = 34_200_000_000_000;
const NS_1600: i64 = 57_600_000_000_000;
const NS_1S: i64 = 1_000_000_000;

fn interval_ns(params: &Value) -> Result<i64, Unsupported> {
    if let Some(v) = get(params, "rebalance_interval_ns") {
        return PyNum::from_value(v)?.to_int();
    }
    let hz = num_or(params, "arrival_rate_hz", PyNum::F(1.0))?.to_float();
    if hz > 0.0 {
        PyNum::F(1e9 / hz).to_int()
    } else {
        Ok(1_000_000_000)
    }
}

pub fn build(scenario: &Value, seed_override: Option<i64>) -> Result<Config, Unsupported> {
    if !scenario.is_object() {
        return unsup("scenario is not an object");
    }
    let seed = match seed_override {
        Some(s) => s,
        None => match get(scenario, "seed") {
            Some(v) => PyNum::from_value(v)?.to_int()?,
            None => return unsup("no seed"),
        },
    };
    if !(0..=u32::MAX as i64).contains(&seed) {
        return unsup("seed outside [0, 2**32)");
    }
    let scenario_id = match get(scenario, "scenario_id") {
        Some(Value::String(s)) => s.clone(),
        Some(v) if !v.is_null() => v.to_string(),
        _ => return unsup("no scenario_id"),
    };

    let exchange = match get(scenario, "exchange_config") {
        Some(e) if e.is_object() => e,
        _ => return unsup("no exchange_config"),
    };
    let proto = truthy(get(exchange, "protocol_enforcement"));
    let stp_cancel_oldest = if proto && truthy(get(exchange, "stp_policy")) {
        match get(exchange, "stp_policy") {
            Some(Value::String(s)) => Some(s == "cancel_oldest"),
            _ => return unsup("non-string stp_policy"),
        }
    } else {
        None
    };
    let pipeline_delay = if proto { num_or(exchange, "ack_delay_ns", PyNum::I(0))?.to_int()? } else { 0 };
    let compute_delay = if proto { num_or(exchange, "compute_delay_ns", PyNum::I(0))?.to_int()? } else { 0 };

    let oracle_cfg = match get(scenario, "oracle_config") {
        Some(o) if o.is_object() => o,
        _ => return unsup("no oracle_config"),
    };
    let empty = Value::Object(Default::default());
    let oracle_params = get(oracle_cfg, "params").unwrap_or(&empty);
    let r_bar = num_or(oracle_params, "initial_price", PyNum::I(100_000))?.to_int()?;
    let horizon_ns = match get(scenario, "horizon_ns") {
        Some(v) => PyNum::from_value(v)?.to_int()?,
        None => return unsup("no horizon_ns"),
    };
    let mkt_open = DATE_NS + NS_0930;
    let mkt_close = mkt_open.checked_add(horizon_ns).ok_or(Unsupported("horizon overflow".into()))?;
    let oracle_close = DATE_NS + NS_1600;

    let kappa_s = num_or(oracle_params, "kappa", PyNum::F(0.0))?.to_float();
    let kappa = if kappa_s > 0.0 { kappa_s / 1e9 } else { 1.67e-16 };
    let jump_rate = num_or(oracle_params, "jump_intensity", PyNum::F(0.0))?.to_float();
    let megashock_lambda_a = if jump_rate > 0.0 { jump_rate / 1e9 } else { 2.77778e-18 };
    let jump_sigma = num_or(oracle_params, "jump_sigma", PyNum::F(0.0))?.to_float();
    let megashock_mean = if jump_sigma != 0.0 { jump_sigma } else { 1000.0 };
    let fund_vol = num_or(oracle_params, "sigma", PyNum::F(5e-5))?.to_float();
    let scheduled_jump = if truthy(get(oracle_params, "scheduled_jump")) {
        let sj = get(oracle_params, "scheduled_jump").unwrap();
        let t = PyNum::from_value(get(sj, "time_ns").ok_or(Unsupported("jump time".into()))?)?.to_int()?;
        let m = PyNum::from_value(get(sj, "magnitude").ok_or(Unsupported("jump magnitude".into()))?)?.to_int()?;
        Some((mkt_open + t, m))
    } else {
        None
    };
    if !(fund_vol.is_finite() && kappa.is_finite() && megashock_lambda_a.is_finite()) {
        return unsup("non-finite oracle params");
    }

    let agent_cfgs = match get(scenario, "agent_configs") {
        Some(Value::Array(a)) => a,
        _ => return unsup("no agent_configs"),
    };
    let mut agents = Vec::new();
    for ac in agent_cfgs {
        let agent_type = match get(ac, "agent_type") {
            Some(Value::String(s)) => s.as_str(),
            _ => return unsup("agent_type"),
        };
        let params = get(ac, "params").unwrap_or(&empty);
        if !params.is_object() {
            return unsup("params not an object");
        }
        let interval = interval_ns(params)?;
        let interval = py_max(PyNum::I(1), PyNum::I(interval)).to_int()?;
        let kind = match agent_type {
            "NoiseTrader" => {
                let offset_ticks = num_or(params, "price_offset_ticks", PyNum::I(5))?.to_int()?;
                if offset_ticks < 0 {
                    return unsup("negative price_offset_ticks");
                }
                AgentKind::Noise {
                    size_mean: num_or(params, "order_size_mean", PyNum::I(10))?.to_float(),
                    size_std: num_or(params, "order_size_std", PyNum::I(2))?.to_float(),
                    offset_ticks,
                    reference_price: r_bar,
                }
            }
            "MarketMaker" => AgentKind::MarketMaker {
                spread_ticks: py_max(PyNum::I(2), num_or(params, "spread_ticks", PyNum::I(2))?).to_int()?,
                depth_levels: py_max(PyNum::I(1), num_or(params, "depth_levels", PyNum::I(3))?).to_int()?,
                size_per_level: py_max(PyNum::I(1), num_or(params, "size_per_level", PyNum::I(10))?).to_int()?,
                reference_price: r_bar,
            },
            "ValueTrader" => AgentKind::Value {
                size_mean: num_or(params, "order_size_mean", PyNum::I(25))?.to_float(),
                threshold_ticks: num_or(params, "threshold_ticks", PyNum::I(2))?.to_int()?,
                sigma_n: 1000.0,
            },
            "MomentumTrader" => AgentKind::Momentum {
                size_mean: num_or(params, "order_size_mean", PyNum::I(15))?.to_float(),
                threshold_ticks: num_or(params, "threshold_ticks", PyNum::I(2))?.to_int()?,
                lookback: py_max(PyNum::I(1), num_or(params, "lookback", PyNum::I(5))?).to_int()?,
            },
            other => return unsup(format!("agent_type {other}")),
        };
        if let AgentKind::Noise { size_mean, size_std, .. } | AgentKind::Value { size_mean, sigma_n: size_std, .. } = &kind {
            if !size_mean.is_finite() || !size_std.is_finite() {
                return unsup("non-finite agent params");
            }
        }
        let count = match get(ac, "count") {
            Some(v) => PyNum::from_value(v)?.to_int()?,
            None => return unsup("count"),
        };
        for _ in 0..count.max(0) {
            agents.push(AgentCfg { kind: kind.clone(), interval_ns: interval });
        }
    }

    let latency_cfg = get(scenario, "latency_config");
    if !truthy(latency_cfg) {
        return unsup("no latency_config (ABIDES default latency model)");
    }
    let lc = latency_cfg.unwrap();
    let lparams = get(lc, "params").unwrap_or(&empty);
    let model = match get(lc, "model") {
        None => "deterministic".to_string(),
        Some(Value::String(s)) => s.clone(),
        Some(_) => return unsup("non-string latency model"),
    };
    let mean_ns = num_or(lparams, "mean_ns", PyNum::F(0.0))?.to_float();
    let sigma = num_or(lparams, "sigma", PyNum::F(0.0))?.to_float();
    let min_ns = num_or(lparams, "min_ns", PyNum::F(0.0))?.to_float();
    let max_ns = num_or(lparams, "max_ns", PyNum::F(1e12))?.to_float();
    let alpha = num_or(lparams, "alpha", PyNum::F(1.5))?.to_float();
    // float(np.log(mean_ns)): numpy's log matched libm on every scenario value (checked on the VM).
    let mu = if mean_ns > 0.0 { std::hint::black_box(mean_ns).ln() } else { 0.0 };
    let kind = match model.as_str() {
        "log_normal" => LatencyKind::LogNormal { mu, sigma },
        "uniform" => LatencyKind::Uniform,
        "pareto" => LatencyKind::Pareto { alpha },
        _ => LatencyKind::Constant,
    };
    if !(mean_ns.is_finite() && sigma.is_finite() && min_ns.is_finite() && max_ns.is_finite() && alpha.is_finite()) {
        return unsup("non-finite latency params");
    }
    if matches!(kind, LatencyKind::LogNormal { .. }) && sigma < 0.0 {
        return unsup("negative lognormal sigma");
    }
    if matches!(kind, LatencyKind::Pareto { .. }) && alpha <= 0.0 {
        return unsup("non-positive pareto alpha");
    }

    Ok(Config {
        scenario_id,
        seed: seed as u32,
        date_ns: DATE_NS,
        mkt_open,
        mkt_close,
        oracle_close,
        stop_time: mkt_close + NS_1S,
        r_bar,
        kappa,
        fund_vol,
        megashock_lambda_a,
        megashock_mean,
        megashock_var: 50_000.0,
        scheduled_jump,
        stp_cancel_oldest,
        pipeline_delay,
        compute_delay,
        agents,
        latency: LatencyCfg { kind, mean_ns, min_ns, max_ns },
    })
}
