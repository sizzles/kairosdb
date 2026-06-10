//! Cassandra storage codec, byte-compatible with the Java implementation so
//! the Rust server can read and write the same `data_points` / `row_keys`
//! tables as a running KairosDB cluster.
//!
//! - Row keys match `org.kairosdb.datastore.cassandra.DataPointsRowKeySerializer`.
//! - Column-time encoding matches `org.kairosdb.datastore.cassandra.RowSpec`.
//!
//! The actual CQL driver integration (`scylla` crate) lands in a follow-up;
//! this module is the format layer it will sit on.

use kairos_core::value::DST_LEGACY;
use kairos_core::Tags;

use crate::{Error, Result};

/// Three weeks in milliseconds — `RowSpec.DEFAULT_ROW_WIDTH`.
pub const DEFAULT_ROW_WIDTH_MS: i64 = 1_814_400_000;

/// A partition key in the `data_points` table, mirroring `DataPointsRowKey`:
/// one row per (metric, 3-week window, datastore type, tag set).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataPointsRowKey {
    pub metric_name: String,
    pub row_time_ms: i64,
    pub data_type: String,
    pub tags: Tags,
}

impl DataPointsRowKey {
    /// Serialize exactly as `DataPointsRowKeySerializer.toByteBuffer`:
    /// metric, NUL, big-endian i64 row time, then (unless legacy) a NUL
    /// marker + length-prefixed data type, then the escaped tag string.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(64);
        buf.extend_from_slice(self.metric_name.as_bytes());
        buf.push(0x00);
        buf.extend_from_slice(&self.row_time_ms.to_be_bytes());
        if self.data_type != DST_LEGACY {
            buf.push(0x00); // marks the beginning of the data type
            buf.push(self.data_type.len() as u8);
            buf.extend_from_slice(self.data_type.as_bytes());
        }
        buf.extend_from_slice(generate_tag_string(&self.tags).as_bytes());
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let nul = bytes
            .iter()
            .position(|b| *b == 0x00)
            .ok_or_else(|| Error::Datastore("row key missing metric terminator".into()))?;
        let metric_name = std::str::from_utf8(&bytes[..nul])
            .map_err(kairos_core::Error::from)?
            .to_string();
        let mut pos = nul + 1;

        let ts_bytes: [u8; 8] = bytes
            .get(pos..pos + 8)
            .ok_or_else(|| Error::Datastore("row key truncated at timestamp".into()))?
            .try_into()
            .expect("slice is 8 bytes");
        let row_time_ms = i64::from_be_bytes(ts_bytes);
        pos += 8;

        // A NUL here marks a data type section; anything else means a legacy
        // key where the tag string starts immediately.
        let data_type = if bytes.get(pos) == Some(&0x00) {
            pos += 1;
            let len = *bytes
                .get(pos)
                .ok_or_else(|| Error::Datastore("row key truncated at type length".into()))?
                as usize;
            pos += 1;
            let dt = bytes
                .get(pos..pos + len)
                .ok_or_else(|| Error::Datastore("row key truncated at data type".into()))?;
            pos += len;
            std::str::from_utf8(dt)
                .map_err(kairos_core::Error::from)?
                .to_string()
        } else {
            DST_LEGACY.to_string()
        };

        let tag_string = std::str::from_utf8(&bytes[pos..]).map_err(kairos_core::Error::from)?;
        Ok(DataPointsRowKey {
            metric_name,
            row_time_ms,
            data_type,
            tags: parse_tag_string(tag_string),
        })
    }
}

/// Tag string format: `key=value:` pairs in sorted key order; `:` and `=`
/// inside keys are escaped with `:`, inside values with `=`.
fn generate_tag_string(tags: &Tags) -> String {
    let mut out = String::new();
    for (key, value) in tags {
        escape_append(&mut out, key, ':');
        out.push('=');
        escape_append(&mut out, value, '=');
        out.push(':');
    }
    out
}

fn escape_append(out: &mut String, value: &str, escape: char) {
    for ch in value.chars() {
        if ch == ':' || ch == '=' {
            out.push(escape);
        }
        out.push(ch);
    }
}

fn parse_tag_string(tag_string: &str) -> Tags {
    let mut tags = Tags::new();
    let chars: Vec<char> = tag_string.chars().collect();
    let mut current = String::new();
    let mut key: Option<String> = None;
    let mut i = 0;
    while i < chars.len() {
        let ch = chars[i];
        let escape = if key.is_none() { ':' } else { '=' };
        if ch == escape && i + 1 < chars.len() && (chars[i + 1] == ':' || chars[i + 1] == '=') {
            current.push(chars[i + 1]);
            i += 2;
            continue;
        }
        match (ch, &key) {
            ('=', None) => key = Some(std::mem::take(&mut current)),
            (':', Some(_)) => {
                tags.insert(key.take().expect("key is set"), std::mem::take(&mut current));
            }
            _ => current.push(ch),
        }
        i += 1;
    }
    tags
}

/// Column-time encoding within a row, mirroring `RowSpec`. The legacy format
/// shifts the offset left one bit; the low bit was historically a long/double
/// flag.
#[derive(Debug, Clone, Copy)]
pub struct RowSpec {
    pub row_width_ms: i64,
    pub legacy: bool,
}

impl Default for RowSpec {
    fn default() -> Self {
        RowSpec {
            row_width_ms: DEFAULT_ROW_WIDTH_MS,
            legacy: true,
        }
    }
}

impl RowSpec {
    pub fn calculate_row_time(&self, timestamp_ms: i64) -> i64 {
        // Matches Java: timestamp - (Math.abs(timestamp) % rowWidth)
        timestamp_ms - (timestamp_ms.abs() % self.row_width_ms)
    }

    pub fn column_name(&self, row_time_ms: i64, timestamp_ms: i64) -> i32 {
        let offset = (timestamp_ms - row_time_ms) as i32;
        if self.legacy {
            offset << 1
        } else {
            offset
        }
    }

    pub fn column_timestamp(&self, row_time_ms: i64, column_name: i32) -> i64 {
        let offset = if self.legacy {
            // Java uses >>> (logical shift) here
            ((column_name as u32) >> 1) as i64
        } else {
            column_name as i64
        };
        row_time_ms + offset
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairos_core::value::{DST_DOUBLE, DST_LONG};

    fn tags(pairs: &[(&str, &str)]) -> Tags {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn row_key_golden_bytes() {
        let key = DataPointsRowKey {
            metric_name: "m".into(),
            row_time_ms: 1_814_400_000,
            data_type: DST_LONG.into(),
            tags: tags(&[("host", "a")]),
        };
        let bytes = key.to_bytes();
        let mut expected = Vec::new();
        expected.extend_from_slice(b"m");
        expected.push(0x00);
        expected.extend_from_slice(&1_814_400_000i64.to_be_bytes());
        expected.push(0x00);
        expected.push(11); // "kairos_long".len()
        expected.extend_from_slice(b"kairos_long");
        expected.extend_from_slice(b"host=a:");
        assert_eq!(bytes, expected);
    }

    #[test]
    fn row_key_roundtrip_with_escaped_tags() {
        let key = DataPointsRowKey {
            metric_name: "commodity.price".into(),
            row_time_ms: 3_628_800_000,
            data_type: DST_DOUBLE.into(),
            tags: tags(&[("root", "CL"), ("note", "a=b:c"), ("k:ey", "v")]),
        };
        let decoded = DataPointsRowKey::from_bytes(&key.to_bytes()).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn legacy_row_key_has_no_type_section() {
        let key = DataPointsRowKey {
            metric_name: "old".into(),
            row_time_ms: 0,
            data_type: DST_LEGACY.into(),
            tags: tags(&[("host", "a")]),
        };
        let bytes = key.to_bytes();
        // metric + NUL + 8-byte time + tag string, nothing else
        assert_eq!(bytes.len(), 3 + 1 + 8 + "host=a:".len());
        let decoded = DataPointsRowKey::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, key);
    }

    #[test]
    fn row_spec_legacy_column_encoding() {
        let spec = RowSpec::default();
        let row_time = spec.calculate_row_time(1_900_000_000);
        assert_eq!(row_time, 1_814_400_000);
        let col = spec.column_name(row_time, 1_900_000_000);
        assert_eq!(col, (1_900_000_000 - 1_814_400_000) << 1);
        assert_eq!(spec.column_timestamp(row_time, col), 1_900_000_000);
    }

    #[test]
    fn row_time_buckets_align() {
        let spec = RowSpec::default();
        assert_eq!(spec.calculate_row_time(0), 0);
        assert_eq!(spec.calculate_row_time(DEFAULT_ROW_WIDTH_MS - 1), 0);
        assert_eq!(spec.calculate_row_time(DEFAULT_ROW_WIDTH_MS), DEFAULT_ROW_WIDTH_MS);
    }
}
