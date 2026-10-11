//! The simulation: a line-by-line reproduction of the ABIDES kernel, ExchangeAgent/OrderBook,
//! TradingAgent and the four `abides_fork` agents, SparseMeanRevertingOracle and the scenario
//! latency model, restricted to the code paths those agents reach. Python references are given as
//! `file:function` in comments where the ordering of side effects matters.

use std::cmp::Ordering;
use std::io;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};

use crate::output::{self, Chunk, Col};
use crate::queue::{Entry, Queue};
use crate::rng::RandomState;
use crate::scenario::{AgentKind, Config, LatencyKind};

// ---------------------------------------------------------------------------------------------
// Python numeric helpers

/// Python int(round(x)) for a float: round half to even.
#[inline]
fn py_round(x: f64) -> i64 {
    x.round_ties_even() as i64
}

extern "C" {
    fn pow(x: f64, y: f64) -> f64;
}

/// Python `x ** y` on floats (C pow), kept as a real libm call.
fn py_pow(x: f64, y: f64) -> f64 {
    unsafe { pow(std::hint::black_box(x), std::hint::black_box(y)) }
}

/// A Python int or float (the oracle's clock switches to float once a megashock time is used).
#[derive(Clone, Copy, Debug)]
enum Num {
    I(i128),
    F(f64),
}

impl Num {
    fn as_f64(self) -> f64 {
        match self {
            Num::I(i) => i as f64,
            Num::F(f) => f,
        }
    }
    /// Python `self - other`.
    fn sub(self, other: Num) -> Num {
        match (self, other) {
            (Num::I(a), Num::I(b)) => Num::I(a - b),
            _ => Num::F(self.as_f64() - other.as_f64()),
        }
    }
    /// Python `self + other`.
    fn add(self, other: Num) -> Num {
        match (self, other) {
            (Num::I(a), Num::I(b)) => Num::I(a + b),
            _ => Num::F(self.as_f64() + other.as_f64()),
        }
    }
    /// Python's exact comparison between ints and floats.
    fn cmp(self, other: Num) -> Ordering {
        match (self, other) {
            (Num::I(a), Num::I(b)) => a.cmp(&b),
            (Num::F(a), Num::F(b)) => a.partial_cmp(&b).expect("NaN in oracle clock"),
            (Num::I(a), Num::F(b)) => cmp_int_float(a, b),
            (Num::F(a), Num::I(b)) => cmp_int_float(b, a).reverse(),
        }
    }
}

fn cmp_int_float(i: i128, f: f64) -> Ordering {
    assert!(f.is_finite(), "non-finite oracle clock");
    let t = f.trunc();
    // |t| < 2^127 for any finite f64 below 1.7e38, which covers every clock value here.
    let ti = t as i128;
    match i.cmp(&ti) {
        Ordering::Equal => {
            let frac = f - t;
            if frac > 0.0 {
                Ordering::Less
            } else if frac < 0.0 {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        o => o,
    }
}

// ---------------------------------------------------------------------------------------------
// Orders and messages

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Side {
    Bid,
    Ask,
}

impl Side {
    /// Index into SIDES (the trace's side strings).
    fn idx(self) -> u32 {
        match self {
            Side::Bid => 0,
            Side::Ask => 1,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Order {
    pub id: i64,
    pub agent: u32,
    pub side: Side,
    pub qty: i64,
    pub limit: i64,
    pub fill_price: Option<i64>,
}

#[derive(Clone, Debug)]
enum Msg {
    Wakeup,
    MarketHoursRequest,
    MarketHours,
    MarketClosePriceRequest,
    MarketClosePrice,
    MarketClosed,
    QuerySpread,
    QuerySpreadResponse { bid: Option<(i64, i64)>, ask: Option<(i64, i64)>, mkt_closed: bool },
    LimitOrder(Order),
    CancelOrder(Order),
    OrderAccepted(Order),
    OrderExecuted(Order),
    OrderCancelled(Order),
}

impl Msg {
    /// Index into MSG_TYPES (the Python class name written to message_trace).
    fn type_idx(&self) -> u8 {
        match self {
            Msg::Wakeup => 0,
            Msg::MarketHoursRequest => 1,
            Msg::MarketHours => 2,
            Msg::MarketClosePriceRequest => 3,
            Msg::MarketClosePrice => 4,
            Msg::MarketClosed => 5,
            Msg::QuerySpread => 6,
            Msg::QuerySpreadResponse { .. } => 7,
            Msg::LimitOrder(_) => 8,
            Msg::CancelOrder(_) => 9,
            Msg::OrderAccepted(_) => 10,
            Msg::OrderExecuted(_) => 11,
            Msg::OrderCancelled(_) => 12,
        }
    }
    fn order_id(&self) -> Option<i64> {
        match self {
            Msg::LimitOrder(o)
            | Msg::CancelOrder(o)
            | Msg::OrderAccepted(o)
            | Msg::OrderExecuted(o)
            | Msg::OrderCancelled(o) => Some(o.id),
            _ => None,
        }
    }
}

/// `Sim::front` value for an agent with no requeued message; above every real key.
const NO_FRONT: u64 = u64::MAX;
/// Set in `Entry::slot` on an agent's front message (see run_kernel); slot numbers stay below it.
const FRONT: u32 = 1 << 31;

const MSG_TYPES: [&str; 14] = [
    "WakeupMsg",
    "MarketHoursRequestMsg",
    "MarketHoursMsg",
    "MarketClosePriceRequestMsg",
    "MarketClosePriceMsg",
    "MarketClosedMsg",
    "QuerySpreadMsg",
    "QuerySpreadResponseMsg",
    "LimitOrderMsg",
    "CancelOrderMsg",
    "OrderAcceptedMsg",
    "OrderExecutedMsg",
    "OrderCancelledMsg",
    "AGENT_WAKEUP",
];
const WAKE_ROW: u8 = 13;
/// Absent order_id / causal_parent / t_send (ids and times are never i64::MIN).
const NONE: i64 = i64::MIN;

/// Send-time ledger fields of an in-flight message, kept beside its payload slot.
#[derive(Clone, Copy)]
struct SendRow {
    t_send: i64,
    t_recv: i64,
    order_id: i64,
    causal: i64,
    msg_type: u8,
}

const WAKE_SEND: SendRow = SendRow { t_send: NONE, t_recv: NONE, order_id: NONE, causal: NONE, msg_type: 0 };

/// One message_trace row, appended at delivery (so the log is already in `seq` order).
pub struct MsgRow {
    message_id: i64,
    t_send: i64,
    t_recv: i64,
    order_id: i64,
    causal: i64,
    src: u32,
    dst: u32,
    msg_type: u8,
}

/// One trace.parquet row; `kind` indexes TRACE_TYPES and `side` SIDES.
#[derive(Clone, Copy)]
struct TraceRow {
    t: i64,
    order_id: i64,
    price: i64,
    size: i64,
    agent: i32,
    kind: u8,
    side: u8,
}

/// One column of a completed run of message_trace rows: (block index, column, rows); block b
/// holds rows b*PAGE... Each block is sent once per column so its columns encode in parallel.
pub struct MsgBlock(usize, usize, Arc<Vec<MsgRow>>);

/// Work for the encoder pool while the run goes on.
pub enum Job {
    Msg(MsgBlock),
    Trace(TraceSeg),
}

/// The order and quote events of one span of time, complete (every later event is later in
/// time), as segment `idx` of trace.parquet. `base` is the log index of `orders[0]`.
pub struct TraceSeg {
    idx: usize,
    base: usize,
    orders: Vec<OrderEvent>,
    quotes: Vec<QuoteEvent>,
}

/// Order plus quote events that make a run cut off a trace segment for the encoders.
const SEG_EVENTS: usize = 1 << 17;

/// msg_type is the one trace column left for the end of the run: an execution row is
/// ORDER_FILLED only if it is its order's last execution.
const TRACE_KIND: usize = 2;
const TRACE_DICTS: [Option<&'static [&'static str]>; 7] = [None, None, Some(&TRACE_TYPES), Some(&SIDES), None, None, None];
const MSG_DICTS: [Option<&'static [&'static str]>; 10] = [None, None, None, None, None, None, None, Some(&MSG_TYPES), None, None];

/// An encoded trace segment: data pages of every column but msg_type (`pages[TRACE_KIND]` is
/// empty), each segment cut into pages of at most PAGE rows.
pub struct SegPages {
    page_rows: Vec<usize>,
    pages: Vec<Vec<Chunk>>,
    kinds: Vec<u8>,
    /// (row, log index, order id) of every execution row; its kind is settled at the end.
    execs: Vec<(u32, u32, u32)>,
}

/// What the encoder pool produced: message pages by block, trace segments by index.
#[derive(Default)]
pub struct Encoded {
    msg_blocks: Vec<Vec<Option<Chunk>>>,
    msg_rows: usize,
    segs: Vec<Option<SegPages>>,
}

/// Encodes jobs from `rx` until every sender is gone. Any number of threads may call this on the
/// same receiver concurrently.
pub fn work(rx: &Mutex<mpsc::Receiver<Job>>, into: &Mutex<Encoded>) {
    loop {
        let next = rx.lock().unwrap().recv();
        let Ok(job) = next else { break };
        match job {
            Job::Msg(MsgBlock(b, c, block)) => {
                let page = msg_cols(&block, b * output::PAGE, |cols| output::encode_page(&cols[c], 0, block.len()));
                let mut e = into.lock().unwrap();
                if c == 0 {
                    e.msg_rows += block.len();
                }
                if e.msg_blocks.len() <= b {
                    e.msg_blocks.resize_with(b + 1, || (0..MSG_COLS).map(|_| None).collect());
                }
                e.msg_blocks[b][c] = Some(page);
            }
            Job::Trace(seg) => {
                let idx = seg.idx;
                let pages = seg.encode();
                let mut e = into.lock().unwrap();
                if e.segs.len() <= idx {
                    e.segs.resize_with(idx + 1, || None);
                }
                e.segs[idx] = Some(pages);
            }
        }
    }
}

impl TraceSeg {
    /// trace.parquet rows of this span: order events sorted by (t_ns, order_id) with ties in log
    /// order, merged with one quote row per (t_ns, side) (values of its last emission, at its
    /// first), quotes first at equal t. Then every column but msg_type is encoded.
    fn encode(self) -> SegPages {
        let (ev, qs) = (&self.orders, &self.quotes);
        let mut idx: Vec<u32> = (0..ev.len() as u32).collect();
        let mut a = 0;
        while a < idx.len() {
            let t = ev[idx[a] as usize].t;
            let mut b = a + 1;
            while b < idx.len() && ev[idx[b] as usize].t == t {
                b += 1;
            }
            if b - a > 1 {
                idx[a..b].sort_unstable_by_key(|&k| (ev[k as usize].o.id, k));
            }
            a = b;
        }
        let mut quotes: Vec<TraceRow> = Vec::new();
        let mut run_start = 0;
        for q in qs {
            if quotes.len() > run_start && quotes[run_start].t != q.t {
                run_start = quotes.len();
            }
            let side = q.side.idx() as u8;
            match quotes[run_start..].iter_mut().find(|x| x.side == side) {
                Some(x) => {
                    x.price = q.price;
                    x.size = q.qty;
                }
                None => quotes.push(TraceRow { t: q.t, order_id: -1, price: q.price, size: q.qty, agent: 0, kind: 0, side }),
            }
        }
        let mut rows = Vec::with_capacity(idx.len() + quotes.len());
        let mut execs = Vec::new();
        let (mut qi, mut oi) = (0, 0);
        while qi < quotes.len() || oi < idx.len() {
            let take_quote = match (quotes.get(qi), idx.get(oi)) {
                (Some(q), Some(&k)) => q.t <= ev[k as usize].t,
                (Some(_), None) => true,
                _ => false,
            };
            if take_quote {
                rows.push(quotes[qi]);
                qi += 1;
                continue;
            }
            let k = idx[oi];
            oi += 1;
            let e = &ev[k as usize];
            let (kind, price) = match e.kind {
                K_SUBMITTED => (1, e.o.limit),
                K_ACCEPTED => (2, e.o.limit),
                K_CANCELLED => (3, e.o.limit),
                _ => {
                    execs.push((rows.len() as u32, (self.base + k as usize) as u32, e.o.id as u32));
                    (4, e.o.fill_price.unwrap())
                }
            };
            rows.push(TraceRow { t: e.t, order_id: e.o.id, price, size: e.o.qty, agent: e.o.agent as i32, kind, side: e.o.side.idx() as u8 });
        }
        let n = rows.len();
        let page_rows: Vec<usize> = (0..n).step_by(output::PAGE).map(|s| (n - s).min(output::PAGE)).collect();
        let pages = trace_cols(&rows, |cols| {
            (0..cols.len())
                .map(|c| {
                    if c == TRACE_KIND {
                        return Vec::new();
                    }
                    (0..n).step_by(output::PAGE).map(|s| output::encode_page(&cols[c], s, (s + output::PAGE).min(n))).collect()
                })
                .collect()
        });
        SegPages { page_rows, pages, kinds: rows.iter().map(|r| r.kind).collect(), execs }
    }
}

/// The 7 trace.parquet columns (schema order) as row accessors over `rows`.
fn trace_cols<R>(rows: &[TraceRow], f: impl FnOnce(&[Col; 7]) -> R) -> R {
    let t_ns = |i: usize| rows[i].t;
    let agent = |i: usize| rows[i].agent;
    let msg_type = |i: usize| rows[i].kind as u32;
    let side = |i: usize| rows[i].side as u32;
    let price = |i: usize| rows[i].price;
    let size = |i: usize| rows[i].size;
    let order_id = |i: usize| rows[i].order_id;
    f(&[Col::I64(&t_ns), Col::I32(&agent), Col::Dict(&TRACE_TYPES, &msg_type), Col::Dict(&SIDES, &side), Col::I64(&price), Col::I64(&size), Col::I64(&order_id)])
}

/// Moves `v[..b]` out, leaving the rest (a short same-time tail) in `v`.
fn split_front<T>(v: &mut Vec<T>, b: usize, keep: usize) -> Vec<T> {
    let mut rest = Vec::with_capacity(keep.max(v.len() - b));
    rest.extend(v.drain(b..));
    std::mem::replace(v, rest)
}

/// Columns of message_trace.parquet (see msg_cols).
const MSG_COLS: usize = 10;

const TRACE_TYPES: [&str; 6] = ["QUOTE_UPDATE", "ORDER_SUBMITTED", "ORDER_ACCEPTED", "ORDER_CANCELLED", "ORDER_FILLED", "PARTIAL_FILL"];
const SIDES: [&str; 2] = ["BID", "ASK"];

fn opt(v: i64) -> Option<i64> {
    if v == NONE { None } else { Some(v) }
}

// ---------------------------------------------------------------------------------------------
// Order book (abides_markets/order_book.py, price_level.py), visible limit orders only

struct Level {
    price: i64,
    total: i64,
    orders: std::collections::VecDeque<Order>,
}

#[derive(Default)]
struct Book {
    bids: Vec<Level>, // best (highest) first
    asks: Vec<Level>, // best (lowest) first
    last_trade: i64,
}

/// PriceLevel.order_has_better_price for an order on the level's own side.
#[inline]
fn better(side: Side, order_price: i64, level_price: i64) -> bool {
    match side {
        Side::Bid => order_price > level_price,
        Side::Ask => order_price < level_price,
    }
}

/// PriceLevel.order_is_match for an order on the opposite side of the level (no post-only orders).
#[inline]
fn crosses(order: &Order, level_price: i64) -> bool {
    match order.side {
        Side::Bid => order.limit >= level_price,
        Side::Ask => order.limit <= level_price,
    }
}

// ---------------------------------------------------------------------------------------------
// Agents

/// `Sim::order_pos` entry for an order in no dict.
const GONE: u32 = u32::MAX;

/// Insertion-ordered order dict (TradingAgent.orders). The index is shared by all traders:
/// order ids are global and sequential, so `pos[id]` (Sim::order_pos) is where the order sits
/// in its owner's `slots`, an array load instead of a hash lookup.
#[derive(Default)]
struct OrderDict {
    slots: Vec<Option<Order>>,
    live: usize,
}

impl OrderDict {
    fn insert(&mut self, pos: &mut Vec<u32>, o: Order) {
        // Order ids are fresh on every placement, so this never overwrites.
        let id = o.id as usize;
        if pos.len() <= id {
            pos.resize(id + 1, GONE);
        }
        pos[id] = self.slots.len() as u32;
        self.slots.push(Some(o));
        self.live += 1;
        if self.slots.len() > 64 && self.live * 4 < self.slots.len() {
            self.compact(pos);
        }
    }
    /// Where order `id` sits here, if this dict holds it.
    fn find(&self, pos: &[u32], id: i64) -> Option<usize> {
        let i = *pos.get(usize::try_from(id).ok()?)? as usize;
        match self.slots.get(i) {
            Some(Some(o)) if o.id == id => Some(i),
            _ => None,
        }
    }
    fn get_mut(&mut self, pos: &[u32], id: i64) -> Option<&mut Order> {
        let i = self.find(pos, id)?;
        self.slots[i].as_mut()
    }
    fn remove(&mut self, pos: &mut [u32], id: i64) -> bool {
        match self.find(pos, id) {
            Some(i) => {
                self.slots[i] = None;
                pos[id as usize] = GONE;
                self.live -= 1;
                true
            }
            None => false,
        }
    }
    fn compact(&mut self, pos: &mut [u32]) {
        self.slots.retain(|s| s.is_some());
        for (i, s) in self.slots.iter().enumerate() {
            pos[s.unwrap().id as usize] = i as u32;
        }
    }
}

struct Trader {
    kind: AgentKind,
    rs: RandomState,
    interval: i64,
    current_time: i64,
    mkt_open: Option<i64>,
    mkt_close: Option<i64>,
    mkt_closed: bool,
    first_wake: bool,
    awaiting_spread: bool,
    known_bid: Option<i64>,
    known_ask: Option<i64>,
    orders: OrderDict,
    mid_history: std::collections::VecDeque<f64>,
}

// ---------------------------------------------------------------------------------------------
// Oracle (abides_markets/oracles/sparse_mean_reverting_oracle.py, scheduled-jump patch)

struct Oracle {
    mkt_close: i64,
    r_bar: i64,
    kappa: f64,
    fund_vol: f64,
    lambda_a: f64,
    ms_mean: f64,
    ms_var: f64,
    rs: RandomState,
    pt: Num,
    pv: i64,
    ms_time: Num,
    ms_value: f64,
    jump: Option<(i64, i64, bool)>,
}

impl Oracle {
    fn compute_at(&mut self, ts: Num, v_adj: f64, pt: Num, pv: i64) -> i64 {
        let d = ts.sub(pt).as_f64();
        let mu = self.r_bar;
        let gamma = self.kappa;
        let theta = self.fund_vol;
        let loc = mu as f64 + ((pv - mu) as f64) * (-gamma * d).exp();
        let scale = ((py_pow(theta, 2.0) / (2.0 * gamma)) * (1.0 - (-2.0 * gamma * d).exp())).sqrt();
        let mut v = self.rs.normal(loc, scale);
        v += v_adj;
        let mut v = if v > 0.0 { py_round(v) } else { 0 };
        if let Some((t, mag, consumed)) = self.jump.as_mut() {
            if !*consumed && ts.cmp(Num::I(*t as i128)) != Ordering::Less {
                v = (v + *mag).max(0);
                *consumed = true;
            }
        }
        self.pt = ts;
        self.pv = v;
        v
    }

    fn advance(&mut self, current_time: i64, global: &mut RandomState) -> i64 {
        let (mut pt, mut pv) = (self.pt, self.pv);
        let now = Num::I(current_time as i128);
        if now.cmp(pt) != Ordering::Greater {
            return pv;
        }
        let mut mst = self.ms_time;
        let mut msv = self.ms_value;
        while mst.cmp(now) == Ordering::Less {
            let v = self.compute_at(mst, msv, pt, pv);
            pt = mst;
            pv = v;
            let gap = global.exponential(1.0 / self.lambda_a);
            mst = pt.add(Num::I(gap.trunc() as i128));
            msv = self.rs.normal(self.ms_mean, self.ms_var.sqrt());
            if self.rs.randint(0, 2) != 0 {
                msv = -msv;
            }
            self.ms_time = mst;
            self.ms_value = msv;
        }
        // v_adj is the int 0 here; adding int 0 to a float leaves it unchanged.
        self.compute_at(now, 0.0, pt, pv)
    }

    fn observe(&mut self, current_time: i64, rs: &mut RandomState, sigma_n: f64, global: &mut RandomState) -> i64 {
        let r_t = if current_time >= self.mkt_close {
            self.advance(self.mkt_close - 1, global)
        } else {
            self.advance(current_time, global)
        };
        if sigma_n == 0.0 {
            r_t
        } else {
            py_round(rs.normal(r_t as f64, sigma_n.sqrt()))
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Trace logging

const K_SUBMITTED: u8 = 0;
const K_ACCEPTED: u8 = 1;
const K_EXECUTED: u8 = 2;
const K_CANCELLED: u8 = 3;

struct OrderEvent {
    t: i64,
    kind: u8,
    o: Order,
}

struct QuoteEvent {
    t: i64,
    side: Side,
    price: i64,
    qty: i64,
}

// ---------------------------------------------------------------------------------------------
// Kernel + world

struct Latency {
    kind: LatencyKind,
    mean_ns: f64,
    min_ns: f64,
    max_ns: f64,
    rs: RandomState,
}

impl Latency {
    fn get(&mut self, sender: u32, recipient: u32) -> i64 {
        if sender == recipient {
            return 0;
        }
        let value = match self.kind {
            LatencyKind::LogNormal { mu, sigma } => self.rs.lognormal(mu, sigma),
            LatencyKind::Uniform => self.rs.uniform(self.min_ns, self.max_ns),
            LatencyKind::Pareto { alpha } => {
                let base = if self.min_ns > 0.0 { self.min_ns } else { 1.0 };
                base * (1.0 + self.rs.pareto(alpha))
            }
            LatencyKind::Constant => self.mean_ns,
        };
        // Python: max(value, min_ns) returns value unless min_ns is larger; min likewise.
        let v = if self.min_ns > value { self.min_ns } else { value };
        let v = if self.max_ns < v { self.max_ns } else { v };
        py_round(v)
    }
}

pub struct Sim {
    cfg: Config,
    global: RandomState,
    oracle: Oracle,
    latency: Latency,
    // kernel
    current_time: i64,
    agent_times: Vec<i64>,
    comp_delay: Vec<i64>,
    heap: Queue,
    /// Per agent: the key of its "front" message (the one of its requeued messages that stays in
    /// `heap`, NO_FRONT if none) and the requeued messages parked behind it (time 0, so they
    /// come out by key). See run_kernel.
    front: Vec<u64>,
    parked: Vec<Queue>,
    slots: Vec<Msg>,
    slot_rows: Vec<SendRow>,
    free_slots: Vec<u32>,
    next_msg_id: i64,
    next_order_id: i64,
    causal: Option<i64>,
    // message_trace rows, PAGE at a time: the open block, the number already handed off, and
    // where completed blocks go (an encoder pool while running; see Sim::run).
    msgs: Vec<MsgRow>,
    msg_blocks: usize,
    /// The encoder pool (while running); `ledger` says whether message rows are kept at all.
    tx: Option<mpsc::Sender<Job>>,
    ledger: bool,
    // exchange (agent 0)
    ex_time: i64,
    book: Book,
    close_price_subs: Vec<u32>,
    // traders: index i is agent id i + 1
    traders: Vec<Trader>,
    // trace: both logs in emission order, which is nondecreasing in t whenever every delay is
    // non-negative (scenario.rs refuses the rest); `disordered` records a violation anyway.
    order_events: Vec<OrderEvent>,
    quote_events: Vec<QuoteEvent>,
    last_exec: Vec<u32>, // order id -> log index of its last ORDER_EXECUTED event
    order_pos: Vec<u32>, // order id -> place in its owner's OrderDict, or GONE
    // Events before `ev_base` (log index) went to the encoders as `segs_sent` trace segments.
    ev_base: usize,
    segs_sent: usize,
    last_event_t: i64,
    disordered: bool,
}

pub struct Written {
    pub n_events: u64,
    pub trace_sha: String,
    /// None when the run kept no message ledger.
    pub n_messages: Option<u64>,
    pub message_sha: Option<String>,
}

impl Sim {
    pub fn new(cfg: Config) -> Sim {
        // abides_fork/config.py:build_config — global draws in this exact order.
        let mut global = RandomState::new(cfg.seed);
        let mut oracle_rs = RandomState::new(global.randint_u32_as_u64() as u32);
        // SparseMeanRevertingOracle.__init__
        let ms_delta = global.exponential(1.0 / cfg.megashock_lambda_a);
        let ms_time = Num::F(cfg.mkt_open as f64 + ms_delta);
        let mut ms_value = oracle_rs.normal(cfg.megashock_mean, cfg.megashock_var.sqrt());
        if oracle_rs.randint(0, 2) != 0 {
            ms_value = -ms_value;
        }
        let _exchange_rs = global.randint_u32_as_u64();
        let mut traders = Vec::with_capacity(cfg.agents.len());
        for a in &cfg.agents {
            traders.push(Trader {
                kind: a.kind.clone(),
                rs: RandomState::new(global.randint_u32_as_u64() as u32),
                interval: a.interval_ns,
                current_time: 0,
                mkt_open: None,
                mkt_close: None,
                mkt_closed: false,
                first_wake: true,
                awaiting_spread: false,
                known_bid: None,
                known_ask: None,
                orders: OrderDict::default(),
                mid_history: Default::default(),
            });
        }
        let latency_rs = RandomState::new(global.randint_u32_as_u64() as u32);
        let _kernel_rs = global.randint_u32_as_u64();

        let n_agents = traders.len() + 1;
        let oracle = Oracle {
            mkt_close: cfg.oracle_close,
            r_bar: cfg.r_bar,
            kappa: cfg.kappa,
            fund_vol: cfg.fund_vol,
            lambda_a: cfg.megashock_lambda_a,
            ms_mean: cfg.megashock_mean,
            ms_var: cfg.megashock_var,
            rs: oracle_rs,
            pt: Num::I(cfg.mkt_open as i128),
            pv: cfg.r_bar,
            ms_time,
            ms_value,
            jump: cfg.scheduled_jump.map(|(t, m)| (t, m, false)),
        };
        let latency = Latency {
            kind: cfg.latency.kind.clone(),
            mean_ns: cfg.latency.mean_ns,
            min_ns: cfg.latency.min_ns,
            max_ns: cfg.latency.max_ns,
            rs: latency_rs,
        };
        let book = Book { last_trade: cfg.r_bar, ..Default::default() };
        Sim {
            current_time: cfg.date_ns,
            agent_times: vec![cfg.date_ns; n_agents],
            comp_delay: vec![50; n_agents],
            heap: Queue::default(),
            front: vec![NO_FRONT; n_agents],
            parked: (0..n_agents).map(|_| Queue::default()).collect(),
            slots: Vec::new(),
            slot_rows: Vec::new(),
            free_slots: Vec::new(),
            next_msg_id: 1,
            next_order_id: 0,
            causal: None,
            msgs: Vec::with_capacity(output::PAGE),
            msg_blocks: 0,
            tx: None,
            ledger: false,
            ex_time: 0,
            book,
            close_price_subs: Vec::new(),
            traders,
            order_events: Vec::new(),
            quote_events: Vec::new(),
            last_exec: Vec::new(),
            order_pos: Vec::new(),
            ev_base: 0,
            segs_sent: 0,
            last_event_t: i64::MIN,
            disordered: false,
            global,
            oracle,
            latency,
            cfg,
        }
    }

    // --- kernel ------------------------------------------------------------------------------

    fn new_msg_id(&mut self) -> i64 {
        let id = self.next_msg_id;
        assert!(id < 1 << 32, "message id exceeds the heap key width");
        self.next_msg_id += 1;
        id
    }

    fn store(&mut self, m: Msg, row: SendRow) -> u32 {
        match self.free_slots.pop() {
            Some(s) => {
                self.slots[s as usize] = m;
                if self.ledger {
                    self.slot_rows[s as usize] = row; // send-time fields only the ledger needs
                }
                s
            }
            None => {
                assert!(self.slots.len() < FRONT as usize, "message slots exhausted");
                self.slots.push(m);
                if self.ledger {
                    self.slot_rows.push(row);
                }
                (self.slots.len() - 1) as u32
            }
        }
    }

    /// Kernel.send_message with an already-assigned message id (a broadcast reuses one id).
    fn send_with_id(&mut self, sender: u32, recipient: u32, msg: Msg, msg_id: i64, delay: i64) {
        let sent_time = self.current_time + self.comp_delay[sender as usize] + delay;
        let latency = self.latency.get(sender, recipient);
        let deliver_at = sent_time + latency;
        let row = SendRow {
            t_send: sent_time,
            t_recv: deliver_at,
            order_id: msg.order_id().unwrap_or(NONE),
            causal: self.causal.unwrap_or(NONE),
            msg_type: msg.type_idx(),
        };
        let slot = self.store(msg, row);
        self.heap.push(Entry::new(deliver_at, sender, recipient, msg_id, slot));
    }

    fn send(&mut self, sender: u32, recipient: u32, msg: Msg) {
        let id = self.new_msg_id();
        self.send_with_id(sender, recipient, msg, id, 0);
    }

    /// ExchangeAgent.send_message: order-book notifications carry the pipeline delay.
    fn exchange_send(&mut self, recipient: u32, msg: Msg) {
        let delay = match msg {
            Msg::OrderAccepted(_) | Msg::OrderCancelled(_) | Msg::OrderExecuted(_) => self.cfg.pipeline_delay,
            _ => 0,
        };
        let id = self.new_msg_id();
        self.send_with_id(0, recipient, msg, id, delay);
    }

    fn set_wakeup(&mut self, agent: u32, t: i64) {
        assert!(!(self.current_time != 0 && t < self.current_time), "wakeup in the past");
        let id = self.new_msg_id();
        let slot = self.store(Msg::Wakeup, WAKE_SEND);
        self.heap.push(Entry::new(t, agent, agent, id, slot));
    }

    fn note_time(&mut self, t: i64) {
        self.disordered |= t < self.last_event_t;
        self.last_event_t = t;
    }

    fn log_order(&mut self, t: i64, kind: u8, o: Order) {
        self.note_time(t);
        if kind == K_EXECUTED {
            let id = o.id as usize;
            if self.last_exec.len() <= id {
                self.last_exec.resize(self.next_order_id as usize, u32::MAX);
            }
            self.last_exec[id] = (self.ev_base + self.order_events.len()) as u32;
        }
        self.order_events.push(OrderEvent { t, kind, o });
        self.maybe_cut();
    }

    fn log_quote(&mut self, q: QuoteEvent) {
        self.note_time(q.t);
        self.quote_events.push(q);
        self.maybe_cut();
    }

    #[inline]
    fn maybe_cut(&mut self) {
        if self.order_events.len() + self.quote_events.len() >= SEG_EVENTS {
            self.cut_trace(false);
        }
    }

    /// Hands every event before the current time (`all`: every event) to the encoders as the
    /// next trace segment. Later events are never earlier in time (see `disordered`), so the
    /// segment's rows are final apart from execution kinds.
    fn cut_trace(&mut self, all: bool) {
        let (b, qb) = if all {
            (self.order_events.len(), self.quote_events.len())
        } else {
            let t = self.last_event_t;
            (self.order_events.partition_point(|e| e.t < t), self.quote_events.partition_point(|q| q.t < t))
        };
        if b == 0 && qb == 0 {
            return; // one instant holds them all so far; wait for time to move on
        }
        let orders = split_front(&mut self.order_events, b, SEG_EVENTS);
        let quotes = split_front(&mut self.quote_events, qb, SEG_EVENTS / 4);
        let seg = TraceSeg { idx: self.segs_sent, base: self.ev_base, orders, quotes };
        self.ev_base += b;
        self.segs_sent += 1;
        // The receiving side lives until run() returns (work() drains it).
        self.tx.as_ref().expect("encoder pool").send(Job::Trace(seg)).expect("encoder pool alive");
    }

    /// True when trace events were emitted out of time order; the trace layout here assumes
    /// otherwise, so the caller must not use this run's outputs.
    pub fn disordered(&self) -> bool {
        self.disordered
    }

    fn push_msg(&mut self, row: MsgRow) {
        if !self.ledger {
            return; // no message ledger for this unit
        }
        self.msgs.push(row);
        if self.msgs.len() == output::PAGE {
            self.flush_msgs();
        }
    }

    fn flush_msgs(&mut self) {
        if self.msgs.is_empty() {
            return;
        }
        let block = Arc::new(std::mem::replace(&mut self.msgs, Vec::with_capacity(output::PAGE)));
        let tx = self.tx.as_ref().expect("encoder pool");
        // The receiving side lives until run() returns (work() drains it).
        for col in 0..MSG_COLS {
            tx.send(Job::Msg(MsgBlock(self.msg_blocks, col, Arc::clone(&block)))).expect("encoder pool alive");
        }
        self.msg_blocks += 1;
    }

    /// Runs the simulation. Trace segments and (with `ledger`) message_trace rows in PAGE-row
    /// blocks leave through `tx` as they become final, so encoders work alongside; the sender is
    /// dropped on return.
    pub fn run(&mut self, tx: mpsc::Sender<Job>, ledger: bool) {
        self.tx = Some(tx);
        self.ledger = ledger;
        self.run_kernel();
        self.flush_msgs();
        self.cut_trace(true);
        self.tx = None;
    }

    fn run_kernel(&mut self) {
        // Kernel.initialize: kernel_initializing for all, then kernel_starting for all.
        self.set_wakeup(0, self.cfg.mkt_close); // ExchangeAgent.kernel_initializing
        let start = self.cfg.date_ns;
        for a in 0..=self.traders.len() as u32 {
            self.set_wakeup(a, start); // Agent.kernel_starting
        }
        self.current_time = start;

        // Kernel.runner
        while !self.heap.is_empty() && self.current_time != 0 && self.current_time <= self.cfg.stop_time {
            let e = self.heap.top();
            self.current_time = e.time;
            let r = e.recipient() as usize;
            let busy_until = self.agent_times[r];
            if busy_until > e.time {
                // Python requeues at the agent's time. Every requeued message of a busy agent then
                // sits at that same time, ordered by key, and is requeued again after each message
                // the agent handles: quadratic in a burst. Only the agent's lowest-keyed requeued
                // message (its front) stays in the heap; the rest park and the next one enters
                // when the front is handled, already at the agent's new time. A parked message's
                // skipped requeues are pops at the time of a message just handled, so the stop
                // test never fires on one: the handled order and the stop point are unchanged.
                if e.key > self.front[r] {
                    self.heap.pop();
                    self.parked[r].push(Entry { time: 0, slot: e.slot & !FRONT, ..e });
                } else {
                    // A lower key takes over; the old front stays queued, its flag now stale.
                    self.front[r] = e.key;
                    self.heap.requeue_top(busy_until, e.slot | FRONT); // pop + push in one sift
                }
                continue;
            }
            self.heap.take_top(); // closed below, or filled by the handler's first push
            let was_front = e.slot & FRONT != 0 && e.key == self.front[r];
            let slot = e.slot & !FRONT;
            self.agent_times[r] = e.time;
            let msg = std::mem::replace(&mut self.slots[slot as usize], Msg::Wakeup);
            self.free_slots.push(slot);
            if let Msg::Wakeup = msg {
                self.causal = Some(e.msg_id());
                self.push_msg(MsgRow {
                    message_id: e.msg_id(),
                    t_send: NONE,
                    t_recv: e.time,
                    order_id: NONE,
                    causal: NONE,
                    src: e.recipient(),
                    dst: e.recipient(),
                    msg_type: WAKE_ROW,
                });
                self.wakeup(e.recipient(), e.time);
                self.agent_times[r] += self.comp_delay[r];
            } else {
                self.agent_times[r] += self.comp_delay[r];
                self.causal = Some(e.msg_id());
                if self.ledger {
                    let w = self.slot_rows[slot as usize];
                    self.push_msg(MsgRow {
                        message_id: e.msg_id(),
                        t_send: w.t_send,
                        t_recv: w.t_recv,
                        order_id: w.order_id,
                        causal: w.causal,
                        src: e.sender(),
                        dst: e.recipient(),
                        msg_type: w.msg_type,
                    });
                }
                self.receive(e.recipient(), e.time, e.sender(), msg);
            }
            if was_front {
                self.front[r] = if self.parked[r].is_empty() {
                    NO_FRONT
                } else {
                    let p = self.parked[r].pop();
                    self.heap.push(Entry { time: self.agent_times[r], slot: p.slot | FRONT, ..p });
                    p.key
                };
            }
            self.heap.close();
        }
    }

    // --- agents ------------------------------------------------------------------------------

    fn wakeup(&mut self, agent: u32, t: i64) {
        if agent == 0 {
            // ExchangeAgent.wakeup
            self.ex_time = t;
            if t >= self.cfg.mkt_close {
                let id = self.new_msg_id();
                let subs = self.close_price_subs.clone();
                for s in subs {
                    self.send_with_id(0, s, Msg::MarketClosePrice, id, 0);
                }
            }
            return;
        }
        let i = agent as usize - 1;
        // TradingAgent.wakeup
        self.traders[i].current_time = t;
        if self.traders[i].first_wake {
            self.traders[i].first_wake = false;
            self.send(agent, 0, Msg::MarketClosePriceRequest);
        }
        if self.traders[i].mkt_open.is_none() {
            self.send(agent, 0, Msg::MarketHoursRequest);
        }
        // ScheduledAgent.wakeup
        let tr = &self.traders[i];
        let open_ok = matches!(tr.mkt_open, Some(v) if v != 0);
        let close_ok = matches!(tr.mkt_close, Some(v) if v != 0);
        if !open_ok || !close_ok || tr.mkt_closed {
            return;
        }
        let next = t + tr.interval;
        self.set_wakeup(agent, next);
        self.send(agent, 0, Msg::QuerySpread);
        self.traders[i].awaiting_spread = true;
    }

    fn receive(&mut self, agent: u32, t: i64, sender: u32, msg: Msg) {
        if agent == 0 {
            self.exchange_receive(t, sender, msg);
            return;
        }
        let i = agent as usize - 1;
        self.traders[i].current_time = t;
        let had_hours = self.traders[i].mkt_open.is_some() && self.traders[i].mkt_close.is_some();
        let mut spread_response = false;
        match msg {
            Msg::MarketHours => {
                self.traders[i].mkt_open = Some(self.cfg.mkt_open);
                self.traders[i].mkt_close = Some(self.cfg.mkt_close);
            }
            Msg::MarketClosePrice => {}
            Msg::MarketClosed => self.traders[i].mkt_closed = true,
            Msg::OrderExecuted(o) => {
                self.log_order(t, K_EXECUTED, o);
                let (orders, pos) = (&mut self.traders[i].orders, &mut self.order_pos);
                let remove = match orders.get_mut(pos, o.id) {
                    Some(mine) => {
                        if o.qty >= mine.qty {
                            true
                        } else {
                            mine.qty -= o.qty;
                            false
                        }
                    }
                    None => false,
                };
                if remove {
                    orders.remove(pos, o.id);
                }
            }
            Msg::OrderAccepted(o) => self.log_order(t, K_ACCEPTED, o),
            Msg::OrderCancelled(o) => {
                self.log_order(t, K_CANCELLED, o);
                self.traders[i].orders.remove(&mut self.order_pos, o.id);
            }
            Msg::QuerySpreadResponse { bid, ask, mkt_closed } => {
                let tr = &mut self.traders[i];
                if mkt_closed {
                    tr.mkt_closed = true;
                }
                tr.known_bid = bid.map(|b| b.0);
                tr.known_ask = ask.map(|a| a.0);
                spread_response = true;
            }
            other => panic!("trader received unexpected {}", MSG_TYPES[other.type_idx() as usize]),
        }
        let have_hours = self.traders[i].mkt_open.is_some() && self.traders[i].mkt_close.is_some();
        if have_hours && !had_hours {
            self.set_wakeup(agent, self.cfg.mkt_open); // + get_wake_frequency() == 0
        }
        // ScheduledAgent.receive_message
        if spread_response && self.traders[i].awaiting_spread {
            if !self.traders[i].mkt_closed {
                self.act(agent);
            }
            self.traders[i].awaiting_spread = false;
        }
    }

    /// TradingAgent.place_limit_order (ignore_risk=True, quantity always > 0 here).
    fn place_limit_order(&mut self, agent: u32, side: Side, qty: i64, limit: i64) {
        let i = agent as usize - 1;
        let o = Order { id: self.next_order_id, agent, side, qty, limit, fill_price: None };
        self.next_order_id += 1;
        assert!(qty > 0, "non-positive order quantity");
        self.traders[i].orders.insert(&mut self.order_pos, o);
        self.send(agent, 0, Msg::LimitOrder(o));
        let t = self.traders[i].current_time;
        self.log_order(t, K_SUBMITTED, o);
    }

    fn act(&mut self, agent: u32) {
        let i = agent as usize - 1;
        // The agents only ever test bid/ask by Python truthiness, so a price of 0 counts as absent.
        let bid = self.traders[i].known_bid.filter(|&p| p != 0);
        let ask = self.traders[i].known_ask.filter(|&p| p != 0);
        let kind = self.traders[i].kind.clone();
        match kind {
            AgentKind::Noise { size_mean, size_std, offset_ticks, reference_price } => {
                let rs = &mut self.traders[i].rs;
                let size = py_round(rs.normal(size_mean, size_std)).max(1);
                let buy = rs.randint(0, 2) != 0;
                let offset = rs.randint(0, offset_ticks + 1);
                if buy {
                    let anchor = ask.or(bid).unwrap_or(reference_price);
                    self.place_limit_order(agent, Side::Bid, size, anchor + offset);
                } else {
                    let anchor = bid.or(ask).unwrap_or(reference_price);
                    self.place_limit_order(agent, Side::Ask, size, anchor - offset);
                }
            }
            AgentKind::MarketMaker { spread_ticks, depth_levels, size_per_level, reference_price } => {
                let mid = match (bid, ask) {
                    (Some(b), Some(a)) => (b + a).div_euclid(2),
                    _ => reference_price,
                };
                // TradingAgent.cancel_all_orders: every open order, in insertion order.
                let open: Vec<Order> = self.traders[i].orders.slots.iter().flatten().copied().collect();
                for o in open {
                    self.send(agent, 0, Msg::CancelOrder(o));
                }
                let half = spread_ticks.div_euclid(2);
                for lvl in 0..depth_levels {
                    self.place_limit_order(agent, Side::Bid, size_per_level, mid - half - lvl);
                    self.place_limit_order(agent, Side::Ask, size_per_level, mid + half + lvl);
                }
            }
            AgentKind::Value { size_mean, threshold_ticks, sigma_n } => {
                let mid = match (bid, ask) {
                    (Some(b), Some(a)) => (b + a) as f64 / 2.0,
                    (Some(b), None) => b as f64,
                    (None, Some(a)) => a as f64,
                    (None, None) => return,
                };
                let t = self.traders[i].current_time;
                let fundamental = {
                    let tr = &mut self.traders[i];
                    self.oracle.observe(t, &mut tr.rs, sigma_n, &mut self.global)
                };
                let size = py_round(size_mean).max(1);
                if mid < (fundamental - threshold_ticks) as f64 && ask.is_some() {
                    self.place_limit_order(agent, Side::Bid, size, ask.unwrap());
                } else if mid > (fundamental + threshold_ticks) as f64 && bid.is_some() {
                    self.place_limit_order(agent, Side::Ask, size, bid.unwrap());
                }
            }
            AgentKind::Momentum { size_mean, threshold_ticks, lookback } => {
                let mid = match (bid, ask) {
                    (Some(b), Some(a)) => (b + a) as f64 / 2.0,
                    (Some(b), None) => b as f64,
                    (None, Some(a)) => a as f64,
                    (None, None) => return,
                };
                let hist = &mut self.traders[i].mid_history;
                hist.push_back(mid);
                if hist.len() as i64 > lookback + 1 {
                    hist.pop_front();
                }
                if hist.len() as i64 <= lookback {
                    return;
                }
                let past = hist[0];
                let size = py_round(size_mean).max(1);
                if mid > past + threshold_ticks as f64 && ask.is_some() {
                    self.place_limit_order(agent, Side::Bid, size, ask.unwrap());
                } else if mid < past - threshold_ticks as f64 && bid.is_some() {
                    self.place_limit_order(agent, Side::Ask, size, bid.unwrap());
                }
            }
        }
    }

    // --- exchange ----------------------------------------------------------------------------

    fn exchange_receive(&mut self, t: i64, sender: u32, msg: Msg) {
        self.ex_time = t;
        self.comp_delay[0] = self.cfg.compute_delay;
        if t > self.cfg.mkt_close {
            match msg {
                Msg::QuerySpread => {}
                _ => {
                    self.exchange_send(sender, Msg::MarketClosed);
                    return;
                }
            }
        }
        match msg {
            Msg::MarketHoursRequest => {
                self.comp_delay[0] = 0;
                self.exchange_send(sender, Msg::MarketHours);
            }
            Msg::MarketClosePriceRequest => self.close_price_subs.push(sender),
            Msg::QuerySpread => {
                let bid = self.book.bids.first().filter(|l| l.total > 0).map(|l| (l.price, l.total));
                let ask = self.book.asks.first().filter(|l| l.total > 0).map(|l| (l.price, l.total));
                let mkt_closed = t > self.cfg.mkt_close;
                self.exchange_send(sender, Msg::QuerySpreadResponse { bid, ask, mkt_closed });
            }
            Msg::LimitOrder(o) => self.handle_limit_order(o),
            Msg::CancelOrder(o) => {
                self.cancel_order(&o);
            }
            other => panic!("exchange received unexpected {}", MSG_TYPES[other.type_idx() as usize]),
        }
    }

    /// OrderBook.handle_limit_order
    fn handle_limit_order(&mut self, mut order: Order) {
        if order.qty <= 0 || order.limit < 0 {
            return; // discarded with a warning, before any quote logging
        }
        let mut trade_qty: i64 = 0;
        let mut trade_value: i64 = 0;
        loop {
            if let Some(cancel_oldest) = self.cfg.stp_cancel_oldest {
                let opp = match order.side {
                    Side::Bid => &self.book.asks,
                    Side::Ask => &self.book.bids,
                };
                if let Some(level) = opp.first() {
                    if crosses(&order, level.price) {
                        let resting = *level.orders.front().unwrap();
                        if resting.agent == order.agent {
                            if cancel_oldest && self.cancel_order(&resting) {
                                continue;
                            }
                            if !cancel_oldest {
                                self.exchange_send(order.agent, Msg::OrderCancelled(order));
                                break;
                            }
                        }
                    }
                }
            }
            match self.execute_order(&mut order) {
                Some((q, p)) => {
                    trade_qty += q;
                    trade_value += p * q;
                    if order.qty <= 0 {
                        break;
                    }
                }
                None => {
                    self.enter_order(order);
                    self.exchange_send(order.agent, Msg::OrderAccepted(order));
                    break;
                }
            }
        }
        let t = self.ex_time;
        if let Some(l) = self.book.bids.first() {
            let (price, qty) = (l.price, l.total);
            self.log_quote(QuoteEvent { t, side: Side::Bid, price, qty });
        }
        if let Some(l) = self.book.asks.first() {
            let (price, qty) = (l.price, l.total);
            self.log_quote(QuoteEvent { t, side: Side::Ask, price, qty });
        }
        if trade_qty > 0 {
            self.book.last_trade = py_round(trade_value as f64 / trade_qty as f64);
        }
    }

    /// OrderBook.execute_order: returns (matched quantity, fill price) or None.
    fn execute_order(&mut self, order: &mut Order) -> Option<(i64, i64)> {
        let book = match order.side {
            Side::Bid => &mut self.book.asks,
            Side::Ask => &mut self.book.bids,
        };
        let level = book.first_mut()?;
        if !crosses(order, level.price) {
            return None;
        }
        let front = level.orders.front_mut().unwrap();
        let mut matched = if order.qty >= front.qty {
            let m = level.orders.pop_front().unwrap();
            level.total -= m.qty;
            if level.orders.is_empty() {
                book.remove(0);
            }
            m
        } else {
            let mut m = *front;
            m.qty = order.qty;
            front.qty -= order.qty;
            level.total -= order.qty;
            m
        };
        matched.fill_price = Some(matched.limit);
        let mut filled = *order;
        filled.qty = matched.qty;
        filled.fill_price = matched.fill_price;
        order.qty -= filled.qty;
        self.exchange_send(matched.agent, Msg::OrderExecuted(matched));
        self.exchange_send(order.agent, Msg::OrderExecuted(filled));
        Some((matched.qty, matched.limit))
    }

    /// OrderBook.enter_order (visible, non-price-to-comply orders).
    fn enter_order(&mut self, order: Order) {
        let book = match order.side {
            Side::Bid => &mut self.book.bids,
            Side::Ask => &mut self.book.asks,
        };
        let new_level = |o: Order| Level { price: o.limit, total: o.qty, orders: std::collections::VecDeque::from([o]) };
        if book.is_empty() || better(order.side, book.last().unwrap().price, order.limit) {
            // book[-1].order_has_worse_price(order): the order is worse than the worst level.
            book.push(new_level(order));
            return;
        }
        for i in 0..book.len() {
            if better(order.side, order.limit, book[i].price) {
                book.insert(i, new_level(order));
                return;
            }
            if book[i].price == order.limit {
                book[i].total += order.qty;
                book[i].orders.push_back(order);
                return;
            }
        }
    }

    /// OrderBook.cancel_order (quiet=False). Returns whether the order was found.
    fn cancel_order(&mut self, order: &Order) -> bool {
        let book = match order.side {
            Side::Bid => &mut self.book.bids,
            Side::Ask => &mut self.book.asks,
        };
        let Some(i) = book.iter().position(|l| l.price == order.limit) else {
            return false;
        };
        let level = &mut book[i];
        let Some(j) = level.orders.iter().position(|o| o.id == order.id) else {
            return false;
        };
        let cancelled = level.orders.remove(j).unwrap();
        level.total -= cancelled.qty;
        if level.orders.is_empty() {
            book.remove(i);
        }
        self.exchange_send(order.agent, Msg::OrderCancelled(cancelled));
        true
    }

    // --- outputs -----------------------------------------------------------------------------

    /// abides_fork/trace.py:extract_trace and extract_message_trace, from the pages the encoder
    /// pool made during the run: the message file is assembled on one thread while the trace's
    /// msg_type column (execution kinds are only final now) is encoded and its file assembled.
    pub fn write_outputs(&self, enc: Encoded, trace_path: &Path, msg_path: &Path) -> io::Result<Written> {
        let Encoded { msg_blocks, msg_rows, segs } = enc;
        let ledger = self.ledger;
        let write_msgs = move || -> io::Result<Option<String>> {
            if !ledger {
                return Ok(None);
            }
            let mut cols: Vec<Vec<Chunk>> = (0..MSG_COLS).map(|_| Vec::with_capacity(msg_blocks.len())).collect();
            for b in msg_blocks {
                for (c, page) in b.into_iter().enumerate() {
                    cols[c].push(page.expect("message page encoded"));
                }
            }
            let page_rows: Vec<usize> = (0..msg_rows).step_by(output::PAGE).map(|s| (msg_rows - s).min(output::PAGE)).collect();
            output::assemble_message_pages(msg_path, &MSG_DICTS, cols, &page_rows).map(Some)
        };
        let write_trace = || -> io::Result<(usize, String)> {
            let segs: Vec<SegPages> = segs.into_iter().map(|s| s.expect("trace segment encoded")).collect();
            let mut by_col: Vec<Vec<Chunk>> = (0..7).map(|_| Vec::new()).collect();
            let mut page_rows = Vec::new();
            let mut n = 0;
            for mut sp in segs {
                for &(r, k, id) in &sp.execs {
                    sp.kinds[r as usize] = if self.last_exec[id as usize] == k { 4 } else { 5 };
                }
                let kinds = &sp.kinds;
                let kind = |i: usize| kinds[i] as u32;
                let col = Col::Dict(&TRACE_TYPES, &kind);
                let mut s = 0;
                for &r in &sp.page_rows {
                    by_col[TRACE_KIND].push(output::encode_page(&col, s, s + r));
                    s += r;
                }
                for (c, pages) in sp.pages.into_iter().enumerate() {
                    if c != TRACE_KIND {
                        by_col[c].extend(pages);
                    }
                }
                n += s;
                page_rows.extend(sp.page_rows);
            }
            let sha = output::assemble_trace_pages(trace_path, &TRACE_DICTS, by_col, &page_rows)?;
            Ok((n, sha))
        };
        let (t, m) = if ledger {
            std::thread::scope(|sc| {
                let h = sc.spawn(write_msgs);
                let t = write_trace();
                (t, h.join().expect("message writer panicked"))
            })
        } else {
            (write_trace(), Ok(None))
        };
        let (n_events, trace_sha) = t?;
        let m = m?;
        Ok(Written { n_events: n_events as u64, trace_sha, n_messages: m.as_ref().map(|_| msg_rows as u64), message_sha: m })
    }
}

/// The 10 message_trace.parquet columns (schema order) over `block`, whose row 0 is `seq` `base`.
fn msg_cols<R>(block: &[MsgRow], base: usize, f: impl FnOnce(&[Col; 10]) -> R) -> R {
    let row = |i: usize| &block[i];
    let seq = |i: usize| (base + i) as i64;
    let t_recv = |i| row(i).t_recv;
    let t_send = |i| opt(row(i).t_send);
    let latency = |i| { let r = row(i); if r.t_send == NONE { 0 } else { r.t_recv - r.t_send } };
    let src = |i| row(i).src as i32;
    let dst = |i| row(i).dst as i32;
    let message_id = |i| row(i).message_id;
    let msg_type = |i| row(i).msg_type as u32;
    let order_id = |i| opt(row(i).order_id);
    let causal = |i| opt(row(i).causal);
    f(&[
        Col::I64(&seq),
        Col::I64(&t_recv),
        Col::OptI64(&t_send),
        Col::I64(&latency),
        Col::I32(&src),
        Col::I32(&dst),
        Col::I64(&message_id),
        Col::Dict(&MSG_TYPES, &msg_type),
        Col::OptI64(&order_id),
        Col::OptI64(&causal),
    ])
}
