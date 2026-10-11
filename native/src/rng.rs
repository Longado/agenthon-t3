//! Bit-exact port of numpy 1.26.4 legacy `np.random.RandomState` (MT19937), scalar calls only.
//! Sources: mt19937.c, legacy-distributions.c, distributions.c, _bounded_integers.pyx.in.
#![allow(dead_code)]

use std::hint::black_box;

const N: usize = 624;
const M: usize = 397;
const MATRIX_A: u32 = 0x9908_b0df;
const UPPER_MASK: u32 = 0x8000_0000;
const LOWER_MASK: u32 = 0x7fff_ffff;

#[derive(Clone)]
pub struct RandomState {
    key: [u32; N],
    pos: usize,
    has_gauss: bool,
    gauss: f64,
}

impl RandomState {
    /// == np.random.RandomState(seed) for 0 <= seed < 2**32 (mt19937_seed).
    pub fn new(seed: u32) -> Self {
        let mut key = [0u32; N];
        let mut s = seed;
        for (pos, k) in key.iter_mut().enumerate() {
            *k = s;
            s = 1812433253u32
                .wrapping_mul(s ^ (s >> 30))
                .wrapping_add(pos as u32 + 1);
        }
        RandomState { key, pos: N, has_gauss: false, gauss: 0.0 }
    }

    fn gen(&mut self) {
        let k = &mut self.key;
        let mag = |y: u32| (y >> 1) ^ ((y & 1).wrapping_neg() & MATRIX_A);
        let mut i = 0;
        while i < N - M {
            let y = (k[i] & UPPER_MASK) | (k[i + 1] & LOWER_MASK);
            k[i] = k[i + M] ^ mag(y);
            i += 1;
        }
        while i < N - 1 {
            let y = (k[i] & UPPER_MASK) | (k[i + 1] & LOWER_MASK);
            k[i] = k[i + M - N] ^ mag(y);
            i += 1;
        }
        let y = (k[N - 1] & UPPER_MASK) | (k[0] & LOWER_MASK);
        k[N - 1] = k[M - 1] ^ mag(y);
        self.pos = 0;
    }

    pub fn next_u32(&mut self) -> u32 {
        if self.pos == N {
            self.gen();
        }
        let mut y = self.key[self.pos];
        self.pos += 1;
        y ^= y >> 11;
        y ^= (y << 7) & 0x9d2c_5680;
        y ^= (y << 15) & 0xefc6_0000;
        y ^= y >> 18;
        y
    }

    /// mt19937_next64: (next << 32) | next. Argument order is explicit (hi first); verified by parity test.
    fn next_u64(&mut self) -> u64 {
        let hi = self.next_u32() as u64;
        let lo = self.next_u32() as u64;
        (hi << 32) | lo
    }

    /// Legacy 53-bit double in [0, 1).
    pub fn random_sample(&mut self) -> f64 {
        let a = (self.next_u32() >> 5) as f64;
        let b = (self.next_u32() >> 6) as f64;
        (a * 67108864.0 + b) / 9007199254740992.0
    }

    fn gen_mask(max: u64) -> u64 {
        let mut m = max;
        m |= m >> 1;
        m |= m >> 2;
        m |= m >> 4;
        m |= m >> 8;
        m |= m >> 16;
        m |= m >> 32;
        m
    }

    /// random_bounded_uint64_fill (masked, count 1): returns off + draw in [0, rng].
    fn bounded_u64(&mut self, off: u64, rng: u64) -> u64 {
        if rng == 0 {
            off
        } else if rng <= 0xFFFF_FFFF {
            if rng == 0xFFFF_FFFF {
                off.wrapping_add(self.next_u32() as u64)
            } else {
                let mask = Self::gen_mask(rng) as u32;
                loop {
                    let v = (self.next_u32() & mask) as u64;
                    if v <= rng {
                        return off.wrapping_add(v);
                    }
                }
            }
        } else {
            let mask = Self::gen_mask(rng);
            loop {
                let v = self.next_u64() & mask;
                if v <= rng {
                    return off.wrapping_add(v);
                }
            }
        }
    }

    /// == rs.randint(low=0, high=2**32, dtype="uint64")
    pub fn randint_u32_as_u64(&mut self) -> u64 {
        self.bounded_u64(0, 0xFFFF_FFFF)
    }

    /// == rs.randint(low, high) (int64, masked rejection). Requires low < high.
    pub fn randint(&mut self, low: i64, high: i64) -> i64 {
        assert!(low < high, "randint: low >= high");
        let rng = (high.wrapping_sub(1)).wrapping_sub(low) as u64;
        self.bounded_u64(low as u64, rng) as i64
    }

    fn gauss(&mut self) -> f64 {
        if self.has_gauss {
            self.has_gauss = false;
            let t = self.gauss;
            self.gauss = 0.0;
            return t;
        }
        loop {
            let x1 = 2.0 * self.random_sample() - 1.0;
            let x2 = 2.0 * self.random_sample() - 1.0;
            let r2 = x1 * x1 + x2 * x2;
            if r2 >= 1.0 || r2 == 0.0 {
                continue;
            }
            let f = (-2.0 * black_box(r2).ln() / r2).sqrt();
            self.gauss = f * x1;
            self.has_gauss = true;
            return f * x2;
        }
    }

    /// legacy_normal: loc + scale * gauss (consumes draws even when scale == 0).
    pub fn normal(&mut self, loc: f64, scale: f64) -> f64 {
        let g = self.gauss();
        loc + scale * g
    }

    /// legacy_lognormal: exp(legacy_normal)
    pub fn lognormal(&mut self, mean: f64, sigma: f64) -> f64 {
        black_box(self.normal(mean, sigma)).exp()
    }

    /// random_uniform: low + (high - low) * next_double
    pub fn uniform(&mut self, low: f64, high: f64) -> f64 {
        let range = high - low;
        low + range * self.random_sample()
    }

    fn standard_exponential(&mut self) -> f64 {
        // legacy: -log(1 - U)
        -black_box(1.0 - self.random_sample()).ln()
    }

    /// legacy_pareto: exp(standard_exponential / a) - 1   (NOT expm1)
    pub fn pareto(&mut self, a: f64) -> f64 {
        black_box(self.standard_exponential() / a).exp() - 1.0
    }

    /// legacy_exponential: scale * standard_exponential
    pub fn exponential(&mut self, scale: f64) -> f64 {
        scale * self.standard_exponential()
    }
}
