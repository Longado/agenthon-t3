//! The kernel's event queue.

/// Heap entry: Python orders `(deliver_at, (sender_id, recipient_id, message))`, and messages
/// compare by message_id. `key` packs sender/recipient/message_id as 16/16/32 bits, so the order
/// is one (i64, u64) comparison: scenario.rs refuses 2^16 or more agents, and message ids are
/// checked against 2^32 when issued. `slot` points at the payload and its send-time ledger fields.
#[derive(PartialEq, Eq, Clone, Copy)]
pub struct Entry {
    pub time: i64,
    pub key: u64,
    pub slot: u32,
}

impl Entry {
    #[inline]
    pub fn new(time: i64, sender: u32, recipient: u32, msg_id: i64, slot: u32) -> Self {
        Entry { time, key: (sender as u64) << 48 | (recipient as u64) << 32 | msg_id as u64, slot }
    }
    #[inline]
    pub fn sender(&self) -> u32 {
        (self.key >> 48) as u32
    }
    #[inline]
    pub fn recipient(&self) -> u32 {
        (self.key >> 32) as u32 & 0xffff
    }
    #[inline]
    pub fn msg_id(&self) -> i64 {
        (self.key & 0xffff_ffff) as i64
    }
    /// (time, key) as one unsigned number with the same order.
    #[inline(always)]
    fn rank(&self) -> u128 {
        ((self.time as u64 ^ 1 << 63) as u128) << 64 | self.key as u128
    }
}

/// Min-queue on (time, key): a 4-ary heap. std's binary heap spent about a third of a run
/// guessing which of two children is smaller; here each level picks the least of four with
/// selects instead of branches, over half as many levels. Keys are unique, so the pop order
/// is the same whatever the heap shape.
///
/// The kernel takes the least entry with `take_top`, which leaves the root open: the handler's
/// first push fills it with one sift down, short because most pushes are near-term, where a
/// pop sifts to the bottom and the push sifts back up. `close` pops properly if none came.
#[derive(Default)]
pub struct Queue {
    h: Vec<Entry>,
    open: bool,
}

impl Queue {
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.h.is_empty()
    }

    /// The least entry (the queue must not be empty).
    #[inline]
    pub fn top(&self) -> Entry {
        self.h[0]
    }

    pub fn push(&mut self, x: Entry) {
        if self.open {
            self.open = false;
            return self.sift_down(x);
        }
        let r = x.rank();
        let mut i = self.h.len();
        self.h.push(x);
        while i > 0 {
            let p = (i - 1) / 4;
            if self.h[p].rank() < r {
                break;
            }
            self.h[i] = self.h[p];
            i = p;
        }
        self.h[i] = x;
    }

    /// Remove and return the least entry (the queue must not be empty).
    pub fn pop(&mut self) -> Entry {
        let last = self.h.pop().unwrap();
        if self.h.is_empty() {
            return last;
        }
        let top = self.h[0];
        self.sift_down(last);
        top
    }

    /// Take the least entry, leaving its place open for the next push (see the type doc).
    #[inline]
    pub fn take_top(&mut self) -> Entry {
        self.open = true;
        self.h[0]
    }

    /// Pop the entry `take_top` returned if no push has replaced it.
    #[inline]
    pub fn close(&mut self) {
        if self.open {
            self.open = false;
            let last = self.h.pop().unwrap();
            if !self.h.is_empty() {
                self.sift_down(last);
            }
        }
    }

    /// Give the least entry a later time and a new slot word, then restore the order.
    pub fn requeue_top(&mut self, time: i64, slot: u32) {
        self.sift_down(Entry { time, slot, ..self.h[0] });
    }

    /// Put `x` in the root's place and move it down to where it belongs.
    #[inline(always)]
    fn sift_down(&mut self, x: Entry) {
        let r = x.rank();
        let n = self.h.len();
        let mut i = 0;
        loop {
            let c = 4 * i + 1;
            let (m, rm) = if c + 3 < n {
                let h = &self.h;
                let (k0, k1, k2, k3) = (h[c].rank(), h[c + 1].rank(), h[c + 2].rank(), h[c + 3].rank());
                let (a, ka) = if k1 < k0 { (c + 1, k1) } else { (c, k0) };
                let (b, kb) = if k3 < k2 { (c + 3, k3) } else { (c + 2, k2) };
                if kb < ka { (b, kb) } else { (a, ka) }
            } else if c < n {
                let mut m = (c, self.h[c].rank());
                for j in c + 1..n {
                    let k = self.h[j].rank();
                    if k < m.1 {
                        m = (j, k);
                    }
                }
                m
            } else {
                break;
            };
            if r < rm {
                break;
            }
            self.h[i] = self.h[m];
            i = m;
        }
        self.h[i] = x;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small LCG drives pushes, pops and requeues; a sorted Vec is the reference. Pushes land
    /// at or after the current least time (as the kernel's do), or anywhere when `any_time`.
    fn pops_in_order(any_time: bool) {
        let (mut q, mut v) = (Queue::default(), Vec::<(i64, u64)>::new());
        let mut s = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            s >> 33
        };
        let base = 1_700_000_000_000_000_000i64;
        for id in 0..20_000u64 {
            v.sort();
            let floor = if any_time { base } else { v.first().map_or(base, |x| x.0) };
            let spread = [3, 50, 1 << 20][(next() % 3) as usize];
            let t = floor + (next() % spread) as i64;
            q.push(Entry { time: t, key: id ^ 0x5555, slot: 0 });
            v.push((t, id ^ 0x5555));
            v.sort();
            match next() % 4 {
                0 => {
                    let e = q.pop();
                    assert_eq!((e.time, e.key), v.remove(0));
                }
                3 => {
                    // take_top, then up to two pushes no earlier than it, then close
                    let e = q.take_top();
                    assert_eq!((e.time, e.key), v.remove(0));
                    for k in 0..next() % 3 {
                        let t = e.time + (next() % spread) as i64;
                        let key = 1 << 40 | id << 2 | k;
                        q.push(Entry { time: t, key, slot: 0 });
                        v.push((t, key));
                    }
                    q.close();
                    v.sort();
                }
                1 => {
                    let later = v[0].0 + (next() % 7) as i64;
                    q.requeue_top(later, 1);
                    v[0].0 = later;
                }
                _ => {}
            }
        }
        v.sort();
        for want in v {
            let e = q.pop();
            assert_eq!((e.time, e.key), want);
        }
        assert!(q.is_empty());
    }

    #[test]
    fn queue_pops_in_order() {
        pops_in_order(false);
        pops_in_order(true);
    }
}
