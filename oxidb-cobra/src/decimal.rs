//! Exact base-10 fixed-point numbers — the port of `object/decimal.go`.
//!
//! The Go original uses `math/big.Int`; here an i128 mantissa + scale is
//! enough for stored-procedure arithmetic (~38 significant digits). All the
//! visible semantics match: scale-preserving add/sub, scale-summing mul,
//! division to `max(scales)` floored at 6 fractional digits with half-up
//! rounding, and a normalized dict key so 19.90 == 19.900.

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    /// Unscaled value: 1990 with scale 2 is 19.90.
    pub value: i128,
    /// Number of fractional digits (>= 0).
    pub scale: i32,
}

fn pow10(n: i64) -> i128 {
    let mut v: i128 = 1;
    for _ in 0..n {
        v = v.saturating_mul(10);
    }
    v
}

impl Decimal {
    pub fn from_int(n: i64) -> Decimal {
        Decimal {
            value: n as i128,
            scale: 0,
        }
    }

    /// Parse "19.90", "-0.5", "+42", ".5", "42." — mirrors object.ParseDecimal.
    pub fn parse(s: &str) -> Option<Decimal> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        let (neg, s) = match s.as_bytes()[0] {
            b'+' => (false, &s[1..]),
            b'-' => (true, &s[1..]),
            _ => (false, s),
        };
        let (int_part, frac) = match s.find('.') {
            Some(dot) => (&s[..dot], &s[dot + 1..]),
            None => (s, ""),
        };
        let digits: String = format!("{int_part}{frac}");
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        let mut v: i128 = digits.parse().ok()?;
        if neg {
            v = -v;
        }
        Some(Decimal {
            value: v,
            scale: frac.len() as i32,
        })
    }

    /// Value lifted to `target` scale (>= current scale).
    fn scaled(&self, target: i32) -> i128 {
        let d = target - self.scale;
        if d > 0 {
            self.value.saturating_mul(pow10(d as i64))
        } else {
            self.value
        }
    }

    pub fn add(&self, b: &Decimal) -> Decimal {
        let s = self.scale.max(b.scale);
        Decimal {
            value: self.scaled(s) + b.scaled(s),
            scale: s,
        }
    }

    pub fn sub(&self, b: &Decimal) -> Decimal {
        let s = self.scale.max(b.scale);
        Decimal {
            value: self.scaled(s) - b.scaled(s),
            scale: s,
        }
    }

    pub fn mul(&self, b: &Decimal) -> Decimal {
        Decimal {
            value: self.value * b.value,
            scale: self.scale + b.scale,
        }
    }

    #[allow(clippy::should_implement_trait)] // mirrors Go's DecimalCmp
    pub fn cmp(&self, b: &Decimal) -> std::cmp::Ordering {
        let s = self.scale.max(b.scale);
        self.scaled(s).cmp(&b.scaled(s))
    }

    /// Remainder of a/b at their common scale (truncated); None on b == 0.
    pub fn rem(&self, b: &Decimal) -> Option<Decimal> {
        if b.value == 0 {
            return None;
        }
        let s = self.scale.max(b.scale);
        Some(Decimal {
            value: self.scaled(s) % b.scaled(s),
            scale: s,
        })
    }

    /// Divide self/b to `scale` fractional digits, rounded half-up.
    /// None on b == 0.
    pub fn div(&self, b: &Decimal, scale: i32) -> Option<Decimal> {
        if b.value == 0 {
            return None;
        }
        // quotient * 10^scale = self.value*10^(scale + b.scale - self.scale) / b.value
        let exp = scale as i64 + b.scale as i64 - self.scale as i64;
        let mut num = self.value;
        let mut den = b.value;
        if exp >= 0 {
            num = num.saturating_mul(pow10(exp));
        } else {
            den = den.saturating_mul(pow10(-exp));
        }
        let q = num / den;
        let r = num % den;
        // half-up: if 2*|rem| >= |den|, round away from zero.
        let q = if r.abs().saturating_mul(2) >= den.abs() {
            if (num < 0) != (den < 0) { q - 1 } else { q + 1 }
        } else {
            q
        };
        Some(Decimal { value: q, scale })
    }

    /// Nearest f64 (lossy; only when leaving exact math).
    pub fn to_f64(&self) -> f64 {
        self.value as f64 / pow10(self.scale as i64) as f64
    }

    /// Integer part, truncated toward zero.
    pub fn to_i64(&self) -> i64 {
        if self.scale <= 0 {
            self.value as i64
        } else {
            (self.value / pow10(self.scale as i64)) as i64
        }
    }

    /// Rounded (half-up) to `scale` fractional digits.
    pub fn round(&self, scale: i32) -> Decimal {
        if scale >= self.scale {
            return Decimal {
                value: self.scaled(scale),
                scale,
            };
        }
        self.div(&Decimal { value: 1, scale: 0 }, scale)
            .expect("divisor is 1")
    }

    /// Canonical string so equal values share a dict key (19.90 → "19.9").
    pub fn normalized_key(&self) -> String {
        let s = self.inspect();
        if s.contains('.') {
            let s = s.trim_end_matches('0');
            s.trim_end_matches('.').to_string()
        } else {
            s
        }
    }

    /// Fixed-point rendering, preserving scale (19.90 stays "19.90").
    pub fn inspect(&self) -> String {
        let neg = self.value < 0;
        let mut digits = self.value.unsigned_abs().to_string();
        let out = if self.scale <= 0 {
            digits
        } else {
            while digits.len() as i32 <= self.scale {
                digits.insert(0, '0');
            }
            let cut = digits.len() - self.scale as usize;
            format!("{}.{}", &digits[..cut], &digits[cut..])
        };
        if neg { format!("-{out}") } else { out }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_inspect() {
        assert_eq!(Decimal::parse("19.90").unwrap().inspect(), "19.90");
        assert_eq!(Decimal::parse("-0.5").unwrap().inspect(), "-0.5");
        assert_eq!(Decimal::parse("+42").unwrap().inspect(), "42");
        assert_eq!(Decimal::parse(".5").unwrap().inspect(), "0.5");
        assert_eq!(Decimal::parse("42.").unwrap().inspect(), "42");
        assert!(Decimal::parse("abc").is_none());
        assert!(Decimal::parse("").is_none());
    }

    #[test]
    fn arithmetic_scales() {
        let a = Decimal::parse("19.90").unwrap();
        let b = Decimal::parse("0.1").unwrap();
        assert_eq!(a.add(&b).inspect(), "20.00");
        assert_eq!(a.sub(&b).inspect(), "19.80");
        assert_eq!(a.mul(&b).inspect(), "1.990");
    }

    #[test]
    fn division_half_up_scale6() {
        let a = Decimal::from_int(1);
        let b = Decimal::from_int(3);
        // scale = max(0,0) floored to 6
        assert_eq!(a.div(&b, 6).unwrap().inspect(), "0.333333");
        let c = Decimal::from_int(2);
        assert_eq!(c.div(&b, 6).unwrap().inspect(), "0.666667"); // half-up
    }

    #[test]
    fn round_half_up() {
        let d = Decimal::parse("2.345").unwrap();
        assert_eq!(d.round(2).inspect(), "2.35");
        assert_eq!(d.round(0).inspect(), "2");
        assert_eq!(d.round(4).inspect(), "2.3450");
        let n = Decimal::parse("-2.345").unwrap();
        assert_eq!(n.round(2).inspect(), "-2.35"); // away from zero
    }

    #[test]
    fn normalized_key_collapses() {
        assert_eq!(Decimal::parse("19.90").unwrap().normalized_key(), "19.9");
        assert_eq!(Decimal::parse("19.900").unwrap().normalized_key(), "19.9");
        assert_eq!(Decimal::parse("1.0").unwrap().normalized_key(), "1");
    }
}
