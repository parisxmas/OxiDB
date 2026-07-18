//! Exact base-10 fixed-point numbers for the SQL engine.
//!
//! A `Decimal` is an `i128` mantissa scaled by a non-negative power of ten:
//! value = `mantissa / 10^scale`. This gives ~38 significant digits of exact
//! decimal arithmetic — enough for money and other fixed-point columns where
//! IEEE-754 `f64` (e.g. `SUM` over thousands of `9.99`-style values) drifts.
//!
//! Semantics are ported from the Cobra VM's `object/decimal` (see
//! `oxidb-cobra/src/decimal.rs`): scale-preserving add/sub, scale-summing mul,
//! division to `max(scales)` floored at 6 fractional digits with half-up
//! rounding, and half-up `round`. This type is intentionally self-contained
//! (a core value type should not couple to the VM crate's internals).
//!
//! Overflow of the `i128` mantissa **saturates** rather than panicking; this
//! is a pragmatic choice — real fixed-point columns stay far below `i128::MAX`.
//!
//! The serde representation is the decimal *string* (e.g. `"19.90"`), so
//! catalog/WAL JSON is human-readable and round-trips exactly, trailing zeros
//! and all.

use std::cmp::Ordering;

use serde::de::{self, Deserialize, Deserializer, Visitor};
use serde::ser::{Serialize, Serializer};

/// An exact base-10 fixed-point number: `mantissa / 10^scale`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decimal {
    /// Unscaled value: `1990` with scale `2` is `19.90`.
    mantissa: i128,
    /// Number of fractional digits (`>= 0`).
    scale: u32,
}

/// `10^n` as an `i128`, saturating on overflow.
fn pow10(n: u32) -> i128 {
    let mut v: i128 = 1;
    for _ in 0..n {
        v = v.saturating_mul(10);
    }
    v
}

impl Decimal {
    pub fn new(mantissa: i128, scale: u32) -> Decimal {
        Decimal { mantissa, scale }
    }

    pub fn from_i64(n: i64) -> Decimal {
        Decimal {
            mantissa: n as i128,
            scale: 0,
        }
    }

    pub fn scale(&self) -> u32 {
        self.scale
    }

    pub fn mantissa(&self) -> i128 {
        self.mantissa
    }

    /// Parse an exact decimal string like `"9.99"`, `"-0.010"`, `"42"`,
    /// `"1.0"`. Scale is the number of digits after the point, preserving
    /// trailing zeros (`"19.90"` → mantissa `1990`, scale `2`).
    ///
    /// Exponent form (`1e3`) is **rejected** (returns `None`) so callers can
    /// fall back to `Double` for those literals.
    pub fn parse(s: &str) -> Option<Decimal> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        // Reject exponent notation — those stay floats.
        if s.bytes().any(|b| b == b'e' || b == b'E') {
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
            mantissa: v,
            scale: frac.len() as u32,
        })
    }

    /// Mantissa lifted to `target` scale (must be `>= self.scale`).
    fn scaled(&self, target: u32) -> i128 {
        let d = target.saturating_sub(self.scale);
        if d > 0 {
            self.mantissa.saturating_mul(pow10(d))
        } else {
            self.mantissa
        }
    }

    pub fn is_zero(&self) -> bool {
        self.mantissa == 0
    }

    pub fn neg(&self) -> Decimal {
        Decimal {
            mantissa: self.mantissa.saturating_neg(),
            scale: self.scale,
        }
    }

    pub fn add(&self, b: &Decimal) -> Decimal {
        let s = self.scale.max(b.scale);
        Decimal {
            mantissa: self.scaled(s).saturating_add(b.scaled(s)),
            scale: s,
        }
    }

    pub fn sub(&self, b: &Decimal) -> Decimal {
        let s = self.scale.max(b.scale);
        Decimal {
            mantissa: self.scaled(s).saturating_sub(b.scaled(s)),
            scale: s,
        }
    }

    pub fn mul(&self, b: &Decimal) -> Decimal {
        Decimal {
            mantissa: self.mantissa.saturating_mul(b.mantissa),
            scale: self.scale + b.scale,
        }
    }

    /// The working scale for `a / b`: `max(scales)`, floored at 6.
    pub fn div_scale(&self, b: &Decimal) -> u32 {
        self.scale.max(b.scale).max(6)
    }

    /// Divide `self / b` to `scale` fractional digits, rounded half-up.
    /// `None` on divide-by-zero.
    pub fn div(&self, b: &Decimal, scale: u32) -> Option<Decimal> {
        if b.mantissa == 0 {
            return None;
        }
        // q * 10^scale = self.mantissa * 10^(scale + b.scale - self.scale) / b.mantissa
        let exp = scale as i64 + b.scale as i64 - self.scale as i64;
        let mut num = self.mantissa;
        let mut den = b.mantissa;
        if exp >= 0 {
            num = num.saturating_mul(pow10(exp as u32));
        } else {
            den = den.saturating_mul(pow10((-exp) as u32));
        }
        let q = num / den;
        let r = num % den;
        // half-up: if 2*|rem| >= |den|, round away from zero.
        let q = if r.abs().saturating_mul(2) >= den.abs() {
            if (num < 0) != (den < 0) {
                q - 1
            } else {
                q + 1
            }
        } else {
            q
        };
        Some(Decimal { mantissa: q, scale })
    }

    /// Remainder of `self / b` at their common scale (truncated); `None` on
    /// divide-by-zero.
    pub fn rem(&self, b: &Decimal) -> Option<Decimal> {
        if b.mantissa == 0 {
            return None;
        }
        let s = self.scale.max(b.scale);
        Some(Decimal {
            mantissa: self.scaled(s) % b.scaled(s),
            scale: s,
        })
    }

    #[allow(clippy::should_implement_trait)] // exact cross-scale compare
    pub fn cmp(&self, b: &Decimal) -> Ordering {
        let s = self.scale.max(b.scale);
        self.scaled(s).cmp(&b.scaled(s))
    }

    /// Nearest `f64` (lossy; only when leaving exact math).
    pub fn to_f64(&self) -> f64 {
        self.mantissa as f64 / pow10(self.scale) as f64
    }

    /// Integer part, truncated toward zero.
    pub fn to_i64(&self) -> i64 {
        if self.scale == 0 {
            self.mantissa as i64
        } else {
            (self.mantissa / pow10(self.scale)) as i64
        }
    }

    /// Rounded (half-up, away from zero on ties) to `places` fractional
    /// digits.
    pub fn round(&self, places: u32) -> Decimal {
        if places >= self.scale {
            return Decimal {
                mantissa: self.scaled(places),
                scale: places,
            };
        }
        // Dividing by 1 at the (smaller) target scale performs the half-up cut.
        self.div(&Decimal::from_i64(1), places)
            .expect("divisor is 1")
    }

    /// Rescale to exactly `target` fractional digits (half-up if narrowing).
    pub fn rescale(&self, target: u32) -> Decimal {
        self.round(target)
    }

    /// Fixed-point rendering, preserving scale (`19.90` stays `"19.90"`).
    pub fn to_string_fixed(&self) -> String {
        let neg = self.mantissa < 0;
        let mut digits = self.mantissa.unsigned_abs().to_string();
        let out = if self.scale == 0 {
            digits
        } else {
            let scale = self.scale as usize;
            while digits.len() <= scale {
                digits.insert(0, '0');
            }
            let cut = digits.len() - scale;
            format!("{}.{}", &digits[..cut], &digits[cut..])
        };
        if neg {
            format!("-{out}")
        } else {
            out
        }
    }
}

impl std::fmt::Display for Decimal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.to_string_fixed())
    }
}

impl Serialize for Decimal {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string_fixed())
    }
}

impl<'de> Deserialize<'de> for Decimal {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        struct DecVisitor;
        impl Visitor<'_> for DecVisitor {
            type Value = Decimal;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a decimal string like \"19.90\"")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Decimal, E> {
                Decimal::parse(v).ok_or_else(|| E::custom(format!("invalid decimal {v:?}")))
            }
        }
        d.deserialize_str(DecVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_and_to_string_roundtrip() {
        for s in ["9.99", "-0.010", "42", "1.0", "19.90", "0", "-7", "100.00"] {
            assert_eq!(Decimal::parse(s).unwrap().to_string_fixed(), s);
        }
        // Sign / leading forms.
        assert_eq!(Decimal::parse("+42").unwrap().to_string_fixed(), "42");
        assert_eq!(Decimal::parse(".5").unwrap().to_string_fixed(), "0.5");
        assert_eq!(Decimal::parse("42.").unwrap().to_string_fixed(), "42");
        // Trailing zeros preserved in scale.
        assert_eq!(Decimal::parse("19.90").unwrap().scale(), 2);
        // Rejections.
        assert!(Decimal::parse("abc").is_none());
        assert!(Decimal::parse("").is_none());
        assert!(Decimal::parse("1e3").is_none());
        assert!(Decimal::parse("1.5E2").is_none());
    }

    #[test]
    fn add_sub_mul_exact() {
        let a = Decimal::parse("19.90").unwrap();
        let b = Decimal::parse("0.1").unwrap();
        assert_eq!(a.add(&b).to_string_fixed(), "20.00");
        assert_eq!(a.sub(&b).to_string_fixed(), "19.80");
        assert_eq!(a.mul(&b).to_string_fixed(), "1.990");
        // The classic float trap, exact here.
        let x = Decimal::parse("0.1").unwrap();
        let y = Decimal::parse("0.2").unwrap();
        assert_eq!(x.add(&y).to_string_fixed(), "0.3");
    }

    #[test]
    fn div_scale6_half_up() {
        let a = Decimal::from_i64(1);
        let b = Decimal::from_i64(3);
        assert_eq!(a.div(&b, 6).unwrap().to_string_fixed(), "0.333333");
        let c = Decimal::from_i64(2);
        assert_eq!(c.div(&b, 6).unwrap().to_string_fixed(), "0.666667");
        assert!(a.div(&Decimal::from_i64(0), 6).is_none());
        // Working scale helper.
        let p = Decimal::parse("10.00").unwrap();
        assert_eq!(p.div_scale(&Decimal::from_i64(3)), 6);
    }

    #[test]
    fn round_half_up() {
        assert_eq!(
            Decimal::parse("2.5").unwrap().round(0).to_string_fixed(),
            "3"
        );
        assert_eq!(
            Decimal::parse("2.45").unwrap().round(1).to_string_fixed(),
            "2.5"
        );
        assert_eq!(
            Decimal::parse("2.345").unwrap().round(2).to_string_fixed(),
            "2.35"
        );
        assert_eq!(
            Decimal::parse("2.344").unwrap().round(2).to_string_fixed(),
            "2.34"
        );
        // Widening keeps value, adds zeros.
        assert_eq!(
            Decimal::parse("2.3").unwrap().round(3).to_string_fixed(),
            "2.300"
        );
        // Negatives round away from zero on ties.
        assert_eq!(
            Decimal::parse("-2.345").unwrap().round(2).to_string_fixed(),
            "-2.35"
        );
    }

    #[test]
    fn cmp_across_scales() {
        let a = Decimal::parse("19.90").unwrap();
        let b = Decimal::parse("19.900").unwrap();
        assert_eq!(a.cmp(&b), Ordering::Equal);
        assert_eq!(
            Decimal::parse("1.5").unwrap().cmp(&Decimal::from_i64(2)),
            Ordering::Less
        );
        assert_eq!(
            Decimal::from_i64(3).cmp(&Decimal::parse("2.99").unwrap()),
            Ordering::Greater
        );
    }

    #[test]
    fn money_sum_is_exact() {
        // 1000 rows of 9.99 → exactly 9990.00 (f64 drifts to ...99999).
        let mut acc = Decimal::from_i64(0);
        let cent = Decimal::parse("9.99").unwrap();
        for _ in 0..1000 {
            acc = acc.add(&cent);
        }
        assert_eq!(acc.to_string_fixed(), "9990.00");
    }

    #[test]
    fn serde_roundtrips_as_string() {
        let d = Decimal::parse("19.90").unwrap();
        let j = serde_json::to_string(&d).unwrap();
        assert_eq!(j, "\"19.90\"");
        let back: Decimal = serde_json::from_str(&j).unwrap();
        assert_eq!(back, d);
        assert_eq!(back.scale(), 2);
    }
}
