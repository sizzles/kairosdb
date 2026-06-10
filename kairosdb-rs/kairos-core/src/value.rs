//! Data point values.
//!
//! Where Java models each value kind as a `DataPoint` subclass behind an
//! interface, we use an enum: no per-point allocation or vtable dispatch, and
//! aggregators can match exhaustively. The `Custom` variant is the extension
//! escape hatch for plugin-defined types (and, later, commodity types like
//! OHLCV bars).

use std::sync::Arc;

use crate::varint::{pack_long, unpack_long};
use crate::{Error, Result};

/// Datastore type strings, identical to the Java `DataPointFactory`
/// constants. These are stored in Cassandra row keys, so they can never
/// change.
pub const DST_LONG: &str = "kairos_long";
pub const DST_DOUBLE: &str = "kairos_double";
pub const DST_STRING: &str = "kairos_string";
pub const DST_LEGACY: &str = "kairos_legacy";
pub const DST_NULL: &str = "kairos_null";

/// Group types used by `Aggregator::can_aggregate`.
pub const GROUP_NUMBER: &str = "number";
pub const GROUP_TEXT: &str = "text";

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Long(i64),
    Double(f64),
    Text(Arc<str>),
    /// Gap marker emitted by the `gaps` aggregator (Java `NullDataPoint`);
    /// renders as JSON `null` and is never stored.
    Null,
    /// Plugin-defined type: raw stored bytes plus the datastore type that
    /// knows how to decode them.
    Custom {
        data_type: Arc<str>,
        bytes: Arc<[u8]>,
    },
}

impl Value {
    pub fn datastore_type(&self) -> &str {
        match self {
            Value::Long(_) => DST_LONG,
            Value::Double(_) => DST_DOUBLE,
            Value::Text(_) => DST_STRING,
            Value::Null => DST_NULL,
            Value::Custom { data_type, .. } => data_type,
        }
    }

    pub fn group_type(&self) -> &str {
        match self {
            Value::Long(_) | Value::Double(_) => GROUP_NUMBER,
            Value::Text(_) => GROUP_TEXT,
            Value::Null => GROUP_NUMBER,
            Value::Custom { .. } => GROUP_TEXT,
        }
    }

    /// Numeric view used by aggregators; mirrors `DataPoint.getDoubleValue()`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Long(v) => Some(*v as f64),
            Value::Double(v) => Some(*v),
            _ => None,
        }
    }

    pub fn is_long(&self) -> bool {
        matches!(self, Value::Long(_))
    }

    /// Serialize the value payload exactly as the Java factories do:
    /// longs as zig-zag varints, doubles as big-endian IEEE-754, strings as
    /// `DataOutput.writeUTF` (u16 length prefix + modified UTF-8; we emit
    /// standard UTF-8, which is identical for all non-NUL BMP text).
    pub fn write_to(&self, out: &mut Vec<u8>) {
        match self {
            Value::Long(v) => pack_long(*v, out),
            Value::Double(v) => out.extend_from_slice(&v.to_bits().to_be_bytes()),
            Value::Text(s) => {
                let bytes = s.as_bytes();
                out.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
                out.extend_from_slice(bytes);
            }
            Value::Null => {}
            Value::Custom { bytes, .. } => out.extend_from_slice(bytes),
        }
    }

    /// Decode a stored value payload given its datastore type string.
    pub fn read_from(data_type: &str, buf: &[u8]) -> Result<Value> {
        match data_type {
            DST_LONG => {
                let mut pos = 0;
                Ok(Value::Long(unpack_long(buf, &mut pos)?))
            }
            DST_DOUBLE => {
                let bytes: [u8; 8] = buf
                    .get(..8)
                    .ok_or(Error::Underflow)?
                    .try_into()
                    .expect("slice is 8 bytes");
                Ok(Value::Double(f64::from_bits(u64::from_be_bytes(bytes))))
            }
            DST_NULL => Ok(Value::Null),
            DST_STRING => {
                let len_bytes: [u8; 2] = buf
                    .get(..2)
                    .ok_or(Error::Underflow)?
                    .try_into()
                    .expect("slice is 2 bytes");
                let len = u16::from_be_bytes(len_bytes) as usize;
                let text = buf.get(2..2 + len).ok_or(Error::Underflow)?;
                Ok(Value::Text(std::str::from_utf8(text)?.into()))
            }
            other => Ok(Value::Custom {
                data_type: other.into(),
                bytes: buf.into(),
            }),
        }
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::Long(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn long_roundtrip() {
        for v in [0i64, 42, -42, i64::MAX, i64::MIN] {
            let mut buf = Vec::new();
            Value::Long(v).write_to(&mut buf);
            assert_eq!(Value::read_from(DST_LONG, &buf).unwrap(), Value::Long(v));
        }
    }

    #[test]
    fn double_is_big_endian_ieee754() {
        let mut buf = Vec::new();
        Value::Double(1.0).write_to(&mut buf);
        // Java DataOutput.writeDouble(1.0) bytes
        assert_eq!(buf, [0x3F, 0xF0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            Value::read_from(DST_DOUBLE, &buf).unwrap(),
            Value::Double(1.0)
        );
    }

    #[test]
    fn string_has_u16_length_prefix() {
        let mut buf = Vec::new();
        Value::from("hi").write_to(&mut buf);
        assert_eq!(buf, [0x00, 0x02, b'h', b'i']);
        assert_eq!(Value::read_from(DST_STRING, &buf).unwrap(), Value::from("hi"));
    }
}
