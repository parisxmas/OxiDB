//! Minimal MSB-first bit stream reader/writer for the Gorilla codec.

pub struct BitWriter {
    buf: Vec<u8>,
    cur: u8,
    nbits: u8, // bits filled in `cur` (0..=7)
}

impl BitWriter {
    pub fn new() -> Self {
        BitWriter {
            buf: Vec::new(),
            cur: 0,
            nbits: 0,
        }
    }

    #[inline]
    pub fn write_bit(&mut self, bit: bool) {
        self.cur = (self.cur << 1) | (bit as u8);
        self.nbits += 1;
        if self.nbits == 8 {
            self.buf.push(self.cur);
            self.cur = 0;
            self.nbits = 0;
        }
    }

    /// Write the low `n` bits of `val`, most-significant first.
    pub fn write_bits(&mut self, val: u64, n: u32) {
        for i in (0..n).rev() {
            self.write_bit((val >> i) & 1 == 1);
        }
    }

    /// Flush and return the byte buffer (last partial byte left-aligned).
    pub fn finish(mut self) -> Vec<u8> {
        if self.nbits > 0 {
            self.cur <<= 8 - self.nbits;
            self.buf.push(self.cur);
        }
        self.buf
    }
}

impl Default for BitWriter {
    fn default() -> Self {
        Self::new()
    }
}

pub struct BitReader<'a> {
    buf: &'a [u8],
    byte: usize,
    bit: u8, // next bit index within buf[byte], 0 = MSB
}

impl<'a> BitReader<'a> {
    pub fn new(buf: &'a [u8]) -> Self {
        BitReader {
            buf,
            byte: 0,
            bit: 0,
        }
    }

    #[inline]
    pub fn read_bit(&mut self) -> Option<bool> {
        if self.byte >= self.buf.len() {
            return None;
        }
        let b = (self.buf[self.byte] >> (7 - self.bit)) & 1 == 1;
        self.bit += 1;
        if self.bit == 8 {
            self.bit = 0;
            self.byte += 1;
        }
        Some(b)
    }

    /// Read `n` bits (MSB first) into the low bits of a u64.
    pub fn read_bits(&mut self, n: u32) -> Option<u64> {
        let mut v = 0u64;
        for _ in 0..n {
            v = (v << 1) | (self.read_bit()? as u64);
        }
        Some(v)
    }
}
