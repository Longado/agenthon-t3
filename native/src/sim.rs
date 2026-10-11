//! The simulation: a line-by-line reproduction of the ABIDES kernel, ExchangeAgent/OrderBook,
//! TradingAgent and the four `abides_fork` agents, SparseMeanRevertingOracle and the scenario
//! latency model, restricted to the code paths those agents reach. Python references are given as
//! `file:function` in comments where the ordering of side effects matters.

use std::cmp::Ordering;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, HashMap};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};
use std::sync::{mpsc, Mutex};

use crate::output::{self, Chunk, Col};
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

/// Heap entry: Python orders `(deliver_at, (sender_id, recipient_id, message))`, and messages
/// compare by message_id. `key` packs sender/recipient/message_id as 16/16/32 bits, so the order
/// is one (i64, u64) comparison: scenario.rs refuses 2^16 or more agents, and message ids are
/// checked against 2^32 when issued. `slot` points at the payload and its send-time ledger fields.
#[derive(PartialEq, Eq, Clone, Copy)]
struct Entry {
    time: i64,
    key: u64,
    slot: u32,
}

impl Entry {
    #[inline]
    fn new(time: i64, sender: u32, recipient: u32, msg_id: i64, slot: u32) -> Self {
        Entry { time, key: (sender as u64) << 48 | (recipient as u64) << 32 | msg_id as u64, slot }
    }
    #[inline]
    fn sender(&self) -> u32 {
        (self.key >> 48) as u32
    }
    #[inline]
    fn recipient(&self) -> u32 {
        (self.key >> 32) as u32 & 0xffff
    }
    #[inline]
    fn msg_id(&self) -> i64 {
        (self.key & 0xffff_ffff) as i64
    }
}

impl Ord for Entry {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap: reverse for min-first.
        (other.time, other.key).cmp(&(self.time, self.key))
    }
}
impl PartialOrd for Entry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

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

/// trace.parquet row order: `rows[i]` is order event k, or quote j tagged `QUOTE | j`.
struct TracePrep {
    rows: Vec<u32>,
    quotes: Vec<(i64, Side, i64, i64)>,
    last_exec: Vec<u32>,
}

const QUOTE: u32 = 1 << 31;

/// A completed run of message_trace rows: (block index, rows); block b holds rows b*PAGE...
pub struct MsgBlock(usize, Vec<MsgRow>);

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

/// Multiplicative hash for order-id keys; SipHash's flood resistance buys nothing here.
#[derive(Default)]
struct IdHasher(u64);

impl std::hash::Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!("IdHasher only hashes i64 keys")
    }
    fn write_i64(&mut self, v: i64) {
        self.0 = (v as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    }
}

/// Insertion-ordered order dict (TradingAgent.orders).
#[derive(Default)]
struct OrderDict {
    slots: Vec<Option<Order>>,
    index: HashMap<i64, usize, std::hash::BuildHasherDefault<IdHasher>>,
    live: usize,
}

impl OrderDict {
    fn insert(&mut self, o: Order) {
        // Order ids are fresh on every placement, so this never overwrites.
        self.index.insert(o.id, self.slots.len());
        self.slots.push(Some(o));
        self.live += 1;
        if self.slots.len() > 64 && self.live * 4 < self.slots.len() {
            self.compact();
        }
    }
    fn get_mut(&mut self, id: i64) -> Option<&mut Order> {
        let i = *self.index.get(&id)?;
        self.slots[i].as_mut()
    }
    fn remove(&mut self, id: i64) -> bool {
        match self.index.remove(&id) {
            Some(i) => {
                self.slots[i] = None;
                self.live -= 1;
                true
            }
            None => false,
        }
    }
    fn compact(&mut self) {
        self.slots.retain(|s| s.is_some());
        self.index.clear();
        for (i, s) in self.slots.iter().enumerate() {
            self.index.insert(s.unwrap().id, i);
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
    heap: BinaryHeap<Entry>,
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
    msg_tx: Option<mpsc::Sender<MsgBlock>>,
    // exchange (agent 0)
    ex_time: i64,
    book: Book,
    close_price_subs: Vec<u32>,
    // traders: index i is agent id i + 1
    traders: Vec<Trader>,
    // trace
    order_events: Vec<OrderEvent>,
    quote_events: Vec<QuoteEvent>,
}

pub struct Written {
    pub n_events: u64,
    pub trace_sha: String,
    pub n_messages: u64,
    pub message_sha: String,
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
            heap: BinaryHeap::new(),
            slots: Vec::new(),
            slot_rows: Vec::new(),
            free_slots: Vec::new(),
            next_msg_id: 1,
            next_order_id: 0,
            causal: None,
            msgs: Vec::with_capacity(output::PAGE),
            msg_blocks: 0,
            msg_tx: None,
            ex_time: 0,
            book,
            close_price_subs: Vec::new(),
            traders,
            order_events: Vec::new(),
            quote_events: Vec::new(),
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
                self.slot_rows[s as usize] = row;
                s
            }
            None => {
                self.slots.push(m);
                self.slot_rows.push(row);
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

    fn push_msg(&mut self, row: MsgRow) {
        self.msgs.push(row);
        if self.msgs.len() == output::PAGE {
            self.flush_msgs();
        }
    }

    fn flush_msgs(&mut self) {
        if self.msgs.is_empty() {
            return;
        }
        let block = std::mem::replace(&mut self.msgs, Vec::with_capacity(output::PAGE));
        let tx = self.msg_tx.as_ref().expect("message sink");
        // The receiving side lives until run() returns (MessagePages::collect drains it).
        tx.send(MsgBlock(self.msg_blocks, block)).expect("message pages receiver alive");
        self.msg_blocks += 1;
    }

    /// Runs the simulation. message_trace rows leave in PAGE-row blocks through `msg_tx` as they
    /// are delivered, so encoders can work alongside; the sender is dropped on return.
    pub fn run(&mut self, msg_tx: mpsc::Sender<MsgBlock>) {
        self.msg_tx = Some(msg_tx);
        self.run_kernel();
        self.flush_msgs();
        self.msg_tx = None;
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
            let mut top = self.heap.peek_mut().unwrap();
            let e = *top;
            self.current_time = e.time;
            let r = e.recipient() as usize;
            if self.agent_times[r] > e.time {
                // Requeue at the agent's time: same queue contents as pop + push, one sift.
                top.time = self.agent_times[r];
                continue;
            }
            PeekMut::pop(top);
            self.agent_times[r] = e.time;
            let msg = std::mem::replace(&mut self.slots[e.slot as usize], Msg::Wakeup);
            self.free_slots.push(e.slot);
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
                let w = self.slot_rows[e.slot as usize];
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
                self.receive(e.recipient(), e.time, e.sender(), msg);
            }
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
                self.order_events.push(OrderEvent { t, kind: K_EXECUTED, o });
                let orders = &mut self.traders[i].orders;
                let remove = match orders.get_mut(o.id) {
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
                    orders.remove(o.id);
                }
            }
            Msg::OrderAccepted(o) => self.order_events.push(OrderEvent { t, kind: K_ACCEPTED, o }),
            Msg::OrderCancelled(o) => {
                self.order_events.push(OrderEvent { t, kind: K_CANCELLED, o });
                self.traders[i].orders.remove(o.id);
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
        self.traders[i].orders.insert(o);
        self.send(agent, 0, Msg::LimitOrder(o));
        let t = self.traders[i].current_time;
        self.order_events.push(OrderEvent { t, kind: K_SUBMITTED, o });
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
            self.quote_events.push(QuoteEvent { t, side: Side::Bid, price: l.price, qty: l.total });
        }
        if let Some(l) = self.book.asks.first() {
            self.quote_events.push(QuoteEvent { t, side: Side::Ask, price: l.price, qty: l.total });
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

    /// abides_fork/trace.py:extract_trace and extract_message_trace. Message pages were encoded
    /// during the run (`msg_pages`); here the message file is assembled on one thread while the
    /// trace rows are ordered and their pages encoded on the rest of the `threads` budget.
    pub fn write_outputs(&self, msg_pages: MessagePages, trace_path: &Path, msg_path: &Path, threads: usize) -> io::Result<Written> {
        let n_msgs = msg_pages.rows;
        let write_msgs = move || {
            msg_cols(&[], 0, |cols| output::assemble_message_pages(msg_path, n_msgs, cols, msg_pages.by_column()))
        };
        let write_trace = |helpers: usize| -> io::Result<(usize, String)> {
            let prep = self.trace_prep();
            let n = prep.rows.len();
            let pages = n.div_ceil(output::PAGE);
            let jobs = pages * 7;
            let out: Vec<Mutex<Option<Chunk>>> = (0..jobs).map(|_| Mutex::new(None)).collect();
            let next = AtomicUsize::new(0);
            let work = || {
                self.with_trace_cols(&prep, |cols| loop {
                    let j = next.fetch_add(1, Relaxed);
                    if j >= jobs {
                        break;
                    }
                    let (c, pg) = (j / pages, j % pages);
                    let page = output::encode_page(&cols[c], pg * output::PAGE, ((pg + 1) * output::PAGE).min(n));
                    *out[j].lock().unwrap() = Some(page);
                })
            };
            std::thread::scope(|sc| {
                for _ in 0..helpers {
                    sc.spawn(work);
                }
                work();
            });
            let mut it = out.into_iter().map(|m| m.into_inner().unwrap().expect("encoded page"));
            let by_col: Vec<Vec<Chunk>> = (0..7).map(|_| it.by_ref().take(pages).collect()).collect();
            let sha = self.with_trace_cols(&prep, |cols| output::assemble_trace_pages(trace_path, n, cols, by_col))?;
            Ok((n, sha))
        };
        let (t, m) = if threads >= 2 {
            std::thread::scope(|sc| {
                let h = sc.spawn(write_msgs);
                let t = write_trace(threads.saturating_sub(2));
                (t, h.join().expect("message writer panicked"))
            })
        } else {
            (write_trace(0), write_msgs())
        };
        let (n_events, trace_sha) = t?;
        Ok(Written { n_events: n_events as u64, trace_sha, n_messages: n_msgs as u64, message_sha: m? })
    }

    /// Row order of trace.parquet: order events sorted by (t_ns, order_id) with ties in log order,
    /// merged with the deduplicated quote rows (quotes first at equal t).
    fn trace_prep(&self) -> TracePrep {
        let ev = &self.order_events;
        // Final fill per order id = its last ORDER_EXECUTED event. Order ids are 0..next_order_id.
        let mut last_exec = vec![u32::MAX; self.next_order_id as usize];
        for (k, e) in ev.iter().enumerate() {
            if e.kind == K_EXECUTED {
                last_exec[e.o.id as usize] = k as u32;
            }
        }
        // The log is already in nondecreasing t, so only runs of equal t need sorting; anything
        // else gets a full sort.
        let mut idx: Vec<u32> = (0..ev.len() as u32).collect();
        if ev.windows(2).all(|w| w[0].t <= w[1].t) {
            let mut a = 0;
            while a < idx.len() {
                let mut b = a + 1;
                while b < idx.len() && ev[b].t == ev[a].t {
                    b += 1;
                }
                if b - a > 1 {
                    idx[a..b].sort_unstable_by_key(|&k| (ev[k as usize].o.id, k));
                }
                a = b;
            }
        } else {
            idx.sort_unstable_by_key(|&k| (ev[k as usize].t, ev[k as usize].o.id, k));
        }

        // Quote rows: one per (t_ns, side), values from the last emission, placed at the first.
        // Events are emitted in nondecreasing t, so (t, side) groups are contiguous runs of equal t.
        let mut quotes: Vec<(i64, Side, i64, i64)> = Vec::new();
        let mut run_start = 0;
        for q in &self.quote_events {
            if quotes.len() > run_start && quotes[run_start].0 != q.t {
                run_start = quotes.len();
            }
            match quotes[run_start..].iter_mut().find(|x| x.1 == q.side) {
                Some(x) => {
                    x.2 = q.price;
                    x.3 = q.qty;
                }
                None => quotes.push((q.t, q.side, q.price, q.qty)),
            }
        }

        assert!(quotes.len() < QUOTE as usize && ev.len() < QUOTE as usize);
        let mut rows: Vec<u32> = Vec::with_capacity(idx.len() + quotes.len());
        let (mut qi, mut oi) = (0, 0);
        while qi < quotes.len() || oi < idx.len() {
            let take_quote = match (quotes.get(qi), idx.get(oi)) {
                (Some(q), Some(&k)) => q.0 <= ev[k as usize].t,
                (Some(_), None) => true,
                _ => false,
            };
            if take_quote {
                rows.push(QUOTE | qi as u32);
                qi += 1;
            } else {
                rows.push(idx[oi]);
                oi += 1;
            }
        }
        TracePrep { rows, quotes, last_exec }
    }

    /// The 7 trace.parquet columns (schema order) as row accessors over `p`.
    fn with_trace_cols<R>(&self, p: &TracePrep, f: impl FnOnce(&[Col; 7]) -> R) -> R {
        let ev = &self.order_events;
        let q = |r: u32| &p.quotes[(r & !QUOTE) as usize];
        let e = |r: u32| &ev[r as usize];
        let at = |i: usize| p.rows[i];
        let t_ns = |i| { let r = at(i); if r & QUOTE != 0 { q(r).0 } else { e(r).t } };
        let agent = |i| { let r = at(i); if r & QUOTE != 0 { 0 } else { e(r).o.agent as i32 } };
        // Indices into TRACE_TYPES / SIDES.
        let msg_type = |i| {
            let r = at(i);
            if r & QUOTE != 0 {
                return 0;
            }
            match e(r).kind {
                K_SUBMITTED => 1,
                K_ACCEPTED => 2,
                K_CANCELLED => 3,
                _ if p.last_exec[e(r).o.id as usize] == r => 4,
                _ => 5,
            }
        };
        let side = |i| { let r = at(i); if r & QUOTE != 0 { q(r).1 } else { e(r).o.side }.idx() };
        let price = |i| {
            let r = at(i);
            if r & QUOTE != 0 {
                return q(r).2;
            }
            let x = e(r);
            if x.kind == K_EXECUTED { x.o.fill_price.unwrap() } else { x.o.limit }
        };
        let size = |i| { let r = at(i); if r & QUOTE != 0 { q(r).3 } else { e(r).o.qty } };
        let order_id = |i| { let r = at(i); if r & QUOTE != 0 { -1 } else { e(r).o.id } };
        f(&[Col::I64(&t_ns), Col::I32(&agent), Col::Dict(&TRACE_TYPES, &msg_type), Col::Dict(&SIDES, &side), Col::I64(&price), Col::I64(&size), Col::I64(&order_id)])
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

/// Encoded message_trace pages, collected from the blocks `Sim::run` sends.
pub struct MessagePages {
    blocks: Mutex<Vec<Option<Vec<Chunk>>>>,
    rows: usize,
}

impl MessagePages {
    /// Encodes blocks from `rx` until every sender is gone. Any number of threads may call this
    /// on the same receiver concurrently; `finish` then yields the pages.
    pub fn work(rx: &Mutex<mpsc::Receiver<MsgBlock>>, into: &Mutex<Vec<Option<Vec<Chunk>>>>, rows: &AtomicUsize) {
        loop {
            let next = rx.lock().unwrap().recv();
            let Ok(MsgBlock(b, block)) = next else { break };
            let pages = msg_cols(&block, b * output::PAGE, |cols| {
                cols.iter().map(|c| output::encode_page(c, 0, block.len())).collect::<Vec<_>>()
            });
            rows.fetch_add(block.len(), Relaxed);
            let mut v = into.lock().unwrap();
            if v.len() <= b {
                v.resize_with(b + 1, || None);
            }
            v[b] = Some(pages);
        }
    }

    pub fn finish(into: Mutex<Vec<Option<Vec<Chunk>>>>, rows: AtomicUsize) -> MessagePages {
        MessagePages { blocks: into, rows: rows.into_inner() }
    }

    /// [column][page], every block present.
    fn by_column(self) -> Vec<Vec<Chunk>> {
        let blocks = self.blocks.into_inner().unwrap();
        let mut cols: Vec<Vec<Chunk>> = (0..10).map(|_| Vec::with_capacity(blocks.len())).collect();
        for b in blocks {
            for (c, page) in b.expect("message block encoded").into_iter().enumerate() {
                cols[c].push(page);
            }
        }
        cols
    }
}
