//! Gorilla-style compression (Facebook, VLDB 2015) for a block of
//! `(timestamp, value)` samples.
//!
//! - Timestamps: delta-of-delta with variable-width buckets. Regular intervals
//!   (the common case) collapse to a single `0` bit each.
//! - Values: XOR against the previous value, storing only the changed
//!   (meaningful) bit window. Unchanged / slowly-changing floats compress hard.
//!
//! A block is self-describing: `[u32 count][i64 t0][f64 v0][bitstream]`.

use crate::bits::{BitReader, BitWriter};

/// Encode aligned `times` / `values` (equal length) into one compressed block.
pub fn encode(times: &[i64], values: &[f64]) -> Vec<u8> {
    assert_eq!(times.len(), values.len());
    let n = times.len();
    let mut out = Vec::with_capacity(16 + n);
    out.extend_from_slice(&(n as u32).to_le_bytes());
    if n == 0 {
        return out;
    }
    out.extend_from_slice(&times[0].to_le_bytes());
    out.extend_from_slice(&values[0].to_le_bytes());

    let mut w = BitWriter::new();

    // ── Timestamps: delta-of-delta ──
    let mut prev_ts = times[0];
    let mut prev_delta: i64 = 0;
    for &t in &times[1..] {
        let delta = t - prev_ts;
        let dod = delta - prev_delta;
        write_dod(&mut w, dod);
        prev_delta = delta;
        prev_ts = t;
    }

    // ── Values: XOR ──
    let mut prev_bits = values[0].to_bits();
    let mut prev_lead = 0u32;
    let mut prev_trail = 0u32;
    let mut have_prev_window = false;
    for &v in &values[1..] {
        let bits = v.to_bits();
        let xor = bits ^ prev_bits;
        if xor == 0 {
            w.write_bit(false); // value unchanged
        } else {
            w.write_bit(true);
            let lead = xor.leading_zeros().min(31);
            let trail = xor.trailing_zeros();
            if have_prev_window && lead >= prev_lead && trail >= prev_trail {
                // Reuse the previous meaningful window.
                w.write_bit(false);
                let mbits = 64 - prev_lead - prev_trail;
                w.write_bits(xor >> prev_trail, mbits);
            } else {
                w.write_bit(true);
                let mbits = 64 - lead - trail;
                w.write_bits(lead as u64, 5);
                // store (mbits - 1) in 6 bits; mbits is 1..=64
                w.write_bits((mbits - 1) as u64, 6);
                w.write_bits(xor >> trail, mbits);
                prev_lead = lead;
                prev_trail = trail;
                have_prev_window = true;
            }
        }
        prev_bits = bits;
    }

    out.extend_from_slice(&w.finish());
    out
}

fn write_dod(w: &mut BitWriter, dod: i64) {
    if dod == 0 {
        w.write_bit(false);
        return;
    }
    // '10' + 7 bits, '110' + 9, '1110' + 12, else '1111' + 64.
    let z = zigzag(dod);
    if fits(dod, 6) {
        w.write_bits(0b10, 2);
        w.write_bits(z, 7);
    } else if fits(dod, 8) {
        w.write_bits(0b110, 3);
        w.write_bits(z, 9);
    } else if fits(dod, 11) {
        w.write_bits(0b1110, 4);
        w.write_bits(z, 12);
    } else {
        w.write_bits(0b1111, 4);
        w.write_bits(dod as u64, 64);
    }
}

/// `dod` fits in a zigzag value of `bits+1` bits (i.e. |dod| range).
fn fits(dod: i64, bits: u32) -> bool {
    let lo = -(1i64 << bits);
    let hi = (1i64 << bits) - 1;
    dod >= lo && dod <= hi
}

#[inline]
fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}
#[inline]
fn unzigzag(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

/// Decode a block produced by [`encode`] back into `(times, values)`.
pub fn decode(block: &[u8]) -> (Vec<i64>, Vec<f64>) {
    if block.len() < 4 {
        return (Vec::new(), Vec::new());
    }
    let n = u32::from_le_bytes(block[0..4].try_into().unwrap()) as usize;
    if n == 0 {
        return (Vec::new(), Vec::new());
    }
    let t0 = i64::from_le_bytes(block[4..12].try_into().unwrap());
    let v0 = f64::from_bits(u64::from_le_bytes(block[12..20].try_into().unwrap()));
    let mut times = Vec::with_capacity(n);
    let mut values = Vec::with_capacity(n);
    times.push(t0);
    values.push(v0);

    let mut r = BitReader::new(&block[20..]);

    // Timestamps
    let mut prev_ts = t0;
    let mut prev_delta = 0i64;
    for _ in 1..n {
        let dod = read_dod(&mut r);
        let delta = prev_delta + dod;
        let t = prev_ts + delta;
        times.push(t);
        prev_delta = delta;
        prev_ts = t;
    }

    // Values
    let mut prev_bits = v0.to_bits();
    let mut prev_lead = 0u32;
    let mut prev_trail = 0u32;
    for _ in 1..n {
        let control = r.read_bit().unwrap_or(false);
        let bits = if !control {
            prev_bits
        } else {
            let reuse = !r.read_bit().unwrap_or(false);
            let (lead, trail, mbits);
            if reuse {
                lead = prev_lead;
                trail = prev_trail;
                mbits = 64 - lead - trail;
            } else {
                lead = r.read_bits(5).unwrap_or(0) as u32;
                mbits = (r.read_bits(6).unwrap_or(0) as u32) + 1;
                trail = 64 - lead - mbits;
                prev_lead = lead;
                prev_trail = trail;
            }
            let meaningful = r.read_bits(mbits).unwrap_or(0);
            prev_bits ^ (meaningful << trail)
        };
        values.push(f64::from_bits(bits));
        prev_bits = bits;
    }

    (times, values)
}

fn read_dod(r: &mut BitReader) -> i64 {
    if !r.read_bit().unwrap_or(false) {
        return 0;
    }
    if !r.read_bit().unwrap_or(false) {
        return unzigzag(r.read_bits(7).unwrap_or(0));
    }
    if !r.read_bit().unwrap_or(false) {
        return unzigzag(r.read_bits(9).unwrap_or(0));
    }
    if !r.read_bit().unwrap_or(false) {
        return unzigzag(r.read_bits(12).unwrap_or(0));
    }
    r.read_bits(64).unwrap_or(0) as i64
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(times: &[i64], values: &[f64]) {
        let block = encode(times, values);
        let (t, v) = decode(&block);
        assert_eq!(t, times);
        assert_eq!(v, values);
    }

    #[test]
    fn empty_and_single() {
        roundtrip(&[], &[]);
        roundtrip(&[1000], &[3.5]);
    }

    #[test]
    fn regular_series_roundtrips() {
        let times: Vec<i64> = (0..1000).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let values: Vec<f64> = (0..1000).map(|i| 20.0 + (i as f64 * 0.01).sin()).collect();
        roundtrip(&times, &values);
    }

    #[test]
    fn irregular_and_constant() {
        let times = vec![10, 25, 25, 100, 101, 5000, 5001];
        let values = vec![1.0, 1.0, 1.0, 2.5, 2.5, -9.0, -9.0];
        roundtrip(&times, &values);
    }

    #[test]
    fn compression_beats_raw_for_regular_data() {
        // Realistic gauge: 1s cadence, value rounded so it holds for stretches
        // (the common metric shape — XOR=0 → 1 bit). Regular timestamps → 1 bit.
        let times: Vec<i64> = (0..3600).map(|i| 1_700_000_000_000 + i * 1000).collect();
        let values: Vec<f64> = (0..3600)
            .map(|i| ((20.0 + (i as f64 / 600.0).sin() * 3.0) * 2.0).round() / 2.0)
            .collect();
        let block = encode(&times, &values);
        let raw = times.len() * 16; // i64 + f64 per point
        assert!(
            block.len() * 8 < raw,
            "compressed {} vs raw {} — expected >8x",
            block.len(),
            raw
        );
    }
}
