//! Variable-length integer codec, byte-compatible with
//! `org.kairosdb.util.Util.packLong`/`unpackLong` (protobuf-style LEB128 with
//! zig-zag encoding for signed values).

use crate::{Error, Result};

pub fn pack_unsigned_long(mut value: u64, out: &mut Vec<u8>) {
    while value & !0x7F != 0 {
        out.push(((value & 0x7F) | 0x80) as u8);
        value >>= 7;
    }
    out.push(value as u8);
}

pub fn unpack_unsigned_long(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut shift = 0u32;
    let mut result = 0u64;
    while shift < 64 {
        let b = *buf.get(*pos).ok_or(Error::Underflow)?;
        *pos += 1;
        result |= u64::from(b & 0x7F) << shift;
        if b & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(Error::VarintTooLong)
}

pub fn pack_long(value: i64, out: &mut Vec<u8>) {
    pack_unsigned_long(((value << 1) ^ (value >> 63)) as u64, out);
}

pub fn unpack_long(buf: &[u8], pos: &mut usize) -> Result<i64> {
    let value = unpack_unsigned_long(buf, pos)?;
    Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: i64) -> Vec<u8> {
        let mut buf = Vec::new();
        pack_long(v, &mut buf);
        let mut pos = 0;
        assert_eq!(unpack_long(&buf, &mut pos).unwrap(), v);
        assert_eq!(pos, buf.len());
        buf
    }

    #[test]
    fn zigzag_roundtrip() {
        for v in [0, 1, -1, 2, -2, 63, 64, -64, -65, 300, i64::MAX, i64::MIN] {
            roundtrip(v);
        }
    }

    /// Golden bytes derived from the Java implementation: zig-zag maps
    /// 0→0, -1→1, 1→2, -2→3, then LEB128.
    #[test]
    fn golden_bytes() {
        assert_eq!(roundtrip(0), [0x00]);
        assert_eq!(roundtrip(-1), [0x01]);
        assert_eq!(roundtrip(1), [0x02]);
        assert_eq!(roundtrip(-2), [0x03]);
        assert_eq!(roundtrip(150), [0xAC, 0x02]); // zigzag(150)=300=0b10_0101100
    }

    #[test]
    fn underflow_detected() {
        let mut pos = 0;
        assert!(unpack_long(&[0x80], &mut pos).is_err());
    }
}
