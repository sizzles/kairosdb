//! Write-ahead log for ingest durability — the replacement for the Java
//! `FileQueueProcessor`/BigArray queue.
//!
//! Layout: a directory of fixed-size segments (`wal-{seq}.log`) of
//! length-prefixed, CRC-checked records, plus a `checkpoint` file recording
//! the position up to which records have been flushed into the datastore.
//! Appends go to the active segment; once a checkpoint passes the end of a
//! segment, that segment is deleted. On startup, records after the
//! checkpoint are replayed into the datastore.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use kairos_core::{DataPoint, DataPointSet, Value};

use crate::{Error, Result};

const DEFAULT_SEGMENT_BYTES: u64 = 64 * 1024 * 1024;
const RECORD_VERSION: u8 = 1;

/// Position of the end of a record: everything up to and including it can be
/// checkpointed once flushed to the datastore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct WalPosition {
    pub segment: u64,
    pub offset: u64,
}

pub struct Wal {
    inner: Mutex<WalInner>,
    dir: PathBuf,
    segment_bytes: u64,
}

struct WalInner {
    segment: u64,
    writer: BufWriter<File>,
    offset: u64,
}

impl Wal {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_segment_size(dir, DEFAULT_SEGMENT_BYTES)
    }

    pub fn open_with_segment_size(dir: impl AsRef<Path>, segment_bytes: u64) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir).map_err(|e| wal_err("create wal dir", e))?;
        let segment = list_segments(&dir)?.last().copied().unwrap_or(0);
        let path = segment_path(&dir, segment);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|e| wal_err("open segment", e))?;
        let offset = file
            .seek(SeekFrom::End(0))
            .map_err(|e| wal_err("seek segment", e))?;
        Ok(Wal {
            inner: Mutex::new(WalInner {
                segment,
                writer: BufWriter::new(file),
                offset,
            }),
            dir,
            segment_bytes,
        })
    }

    /// Append one record; returns the position to checkpoint once the set
    /// has been flushed to the datastore.
    pub fn append(&self, set: &DataPointSet) -> Result<WalPosition> {
        let payload = encode_set(set);
        let mut inner = self.inner.lock().expect("wal lock poisoned");
        if inner.offset >= self.segment_bytes {
            let next = inner.segment + 1;
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(segment_path(&self.dir, next))
                .map_err(|e| wal_err("rotate segment", e))?;
            inner
                .writer
                .flush()
                .map_err(|e| wal_err("flush on rotate", e))?;
            inner.segment = next;
            inner.writer = BufWriter::new(file);
            inner.offset = 0;
        }
        inner
            .writer
            .write_all(&(payload.len() as u32).to_le_bytes())
            .and_then(|_| inner.writer.write_all(&crc32(&payload).to_le_bytes()))
            .and_then(|_| inner.writer.write_all(&payload))
            .map_err(|e| wal_err("append", e))?;
        inner.offset += 8 + payload.len() as u64;
        Ok(WalPosition {
            segment: inner.segment,
            offset: inner.offset,
        })
    }

    /// Flush buffered records and fsync the active segment. Called on an
    /// interval by the ingest pipeline rather than per append.
    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock().expect("wal lock poisoned");
        inner.writer.flush().map_err(|e| wal_err("flush", e))?;
        inner
            .writer
            .get_ref()
            .sync_data()
            .map_err(|e| wal_err("fsync", e))
    }

    /// Persist the checkpoint and delete segments wholly behind it.
    pub fn checkpoint(&self, pos: WalPosition) -> Result<()> {
        let tmp = self.dir.join("checkpoint.tmp");
        fs::write(&tmp, format!("{} {}", pos.segment, pos.offset))
            .map_err(|e| wal_err("write checkpoint", e))?;
        fs::rename(&tmp, self.dir.join("checkpoint"))
            .map_err(|e| wal_err("rename checkpoint", e))?;
        for seq in list_segments(&self.dir)? {
            let active = self.inner.lock().expect("wal lock poisoned").segment;
            if seq < pos.segment && seq < active {
                fs::remove_file(segment_path(&self.dir, seq))
                    .map_err(|e| wal_err("remove segment", e))?;
            }
        }
        Ok(())
    }

    pub fn read_checkpoint(&self) -> Result<Option<WalPosition>> {
        let path = self.dir.join("checkpoint");
        if !path.exists() {
            return Ok(None);
        }
        let content = fs::read_to_string(&path).map_err(|e| wal_err("read checkpoint", e))?;
        let mut parts = content.split_whitespace();
        let (Some(seg), Some(off)) = (parts.next(), parts.next()) else {
            return Err(Error::Datastore("malformed wal checkpoint".into()));
        };
        Ok(Some(WalPosition {
            segment: seg.parse().map_err(|e| wal_err("checkpoint segment", e))?,
            offset: off.parse().map_err(|e| wal_err("checkpoint offset", e))?,
        }))
    }

    /// All records after the checkpoint, in order. A torn record at the tail
    /// of the newest segment (crash mid-append) ends replay; corruption
    /// anywhere else is an error.
    pub fn replay(&self) -> Result<Vec<(WalPosition, DataPointSet)>> {
        self.sync()?;
        let checkpoint = self.read_checkpoint()?;
        let segments = list_segments(&self.dir)?;
        let newest = segments.last().copied();
        let mut out = Vec::new();
        for seq in segments {
            if let Some(cp) = checkpoint {
                if seq < cp.segment {
                    continue;
                }
            }
            let mut data = Vec::new();
            File::open(segment_path(&self.dir, seq))
                .and_then(|mut f| f.read_to_end(&mut data))
                .map_err(|e| wal_err("read segment", e))?;
            let mut pos = if checkpoint.is_some_and(|cp| cp.segment == seq) {
                checkpoint.expect("checked above").offset as usize
            } else {
                0
            };
            while pos < data.len() {
                let is_tail = Some(seq) == newest;
                match decode_record(&data, pos) {
                    Ok((set, next)) => {
                        out.push((
                            WalPosition {
                                segment: seq,
                                offset: next as u64,
                            },
                            set,
                        ));
                        pos = next;
                    }
                    Err(e) if is_tail => {
                        // Torn tail write from a crash: replay what we have.
                        let _ = e;
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(out)
    }
}

fn wal_err(context: &str, e: impl std::fmt::Display) -> Error {
    Error::Datastore(format!("wal {context}: {e}"))
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("wal-{seq:016}.log"))
}

fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir).map_err(|e| wal_err("read dir", e))? {
        let name = entry.map_err(|e| wal_err("read dir entry", e))?.file_name();
        let name = name.to_string_lossy();
        if let Some(seq) = name
            .strip_prefix("wal-")
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse().ok())
        {
            segments.push(seq);
        }
    }
    segments.sort_unstable();
    Ok(segments)
}

// --- record codec -----------------------------------------------------

fn put_str(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u16).to_le_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn encode_set(set: &DataPointSet) -> Vec<u8> {
    let mut out = Vec::with_capacity(64 + set.points.len() * 12);
    out.push(RECORD_VERSION);
    put_str(&mut out, &set.name);
    out.extend_from_slice(&set.ttl.to_le_bytes());
    out.extend_from_slice(&(set.tags.len() as u16).to_le_bytes());
    for (k, v) in &set.tags {
        put_str(&mut out, k);
        put_str(&mut out, v);
    }
    out.extend_from_slice(&(set.points.len() as u32).to_le_bytes());
    for point in &set.points {
        out.extend_from_slice(&point.timestamp_ms.to_le_bytes());
        put_str(&mut out, point.value.datastore_type());
        let mut value = Vec::new();
        point.value.write_to(&mut value);
        out.extend_from_slice(&(value.len() as u32).to_le_bytes());
        out.extend_from_slice(&value);
    }
    out
}

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let slice = self
            .data
            .get(self.pos..self.pos + n)
            .ok_or_else(|| Error::Datastore("wal record truncated".into()))?;
        self.pos += n;
        Ok(slice)
    }

    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().expect("2 bytes")))
    }

    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4 bytes")))
    }

    fn str(&mut self) -> Result<String> {
        let len = self.u16()? as usize;
        Ok(std::str::from_utf8(self.take(len)?)
            .map_err(kairos_core::Error::from)?
            .to_string())
    }
}

fn decode_record(data: &[u8], start: usize) -> Result<(DataPointSet, usize)> {
    let header = data
        .get(start..start + 8)
        .ok_or_else(|| Error::Datastore("wal header truncated".into()))?;
    let len = u32::from_le_bytes(header[..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(header[4..8].try_into().expect("4 bytes"));
    let payload = data
        .get(start + 8..start + 8 + len)
        .ok_or_else(|| Error::Datastore("wal payload truncated".into()))?;
    if crc32(payload) != crc {
        return Err(Error::Datastore("wal record crc mismatch".into()));
    }

    let mut r = Reader { data: payload, pos: 0 };
    let version = r.take(1)?[0];
    if version != RECORD_VERSION {
        return Err(Error::Datastore(format!("unknown wal record version {version}")));
    }
    let mut set = DataPointSet::new(r.str()?);
    set.ttl = r.u32()?;
    let tag_count = r.u16()?;
    for _ in 0..tag_count {
        let key = r.str()?;
        let value = r.str()?;
        set.tags.insert(key, value);
    }
    let point_count = r.u32()?;
    for _ in 0..point_count {
        let ts = i64::from_le_bytes(r.take(8)?.try_into().expect("8 bytes"));
        let data_type = r.str()?;
        let len = r.u32()? as usize;
        let value = Value::read_from(&data_type, r.take(len)?)?;
        set.points.push(DataPoint {
            timestamp_ms: ts,
            value,
        });
    }
    Ok((set, start + 8 + len))
}

/// CRC-32 (IEEE 802.3), table-free bitwise form — fast enough for ingest
/// records and avoids a dependency.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for byte in data {
        crc ^= *byte as u32;
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (0u32.wrapping_sub(crc & 1)));
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(name: &str, n: usize) -> DataPointSet {
        let mut set = DataPointSet::new(name).tag("root", "CL");
        for i in 0..n {
            set = set.point(i as i64 * 1000, i as f64);
        }
        set.point(999_999, 42i64).point(1_000_000, "settled")
    }

    #[test]
    fn append_replay_roundtrip() {
        let dir = std::env::temp_dir().join(format!("kairos-wal-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let wal = Wal::open(&dir).unwrap();
        let p1 = wal.append(&sample("a", 3)).unwrap();
        let _p2 = wal.append(&sample("b", 5)).unwrap();
        wal.sync().unwrap();

        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].1, sample("a", 3));
        assert_eq!(records[1].1, sample("b", 5));

        // Checkpoint past the first record: only the second replays.
        wal.checkpoint(p1).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1.name, "b");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn survives_reopen_and_rotation() {
        let dir = std::env::temp_dir().join(format!("kairos-wal-rot-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        {
            // Tiny segments to force rotation.
            let wal = Wal::open_with_segment_size(&dir, 64).unwrap();
            for i in 0..5 {
                wal.append(&sample(&format!("m{i}"), 2)).unwrap();
            }
            wal.sync().unwrap();
        }
        let wal = Wal::open_with_segment_size(&dir, 64).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 5);
        assert!(records.iter().map(|(p, _)| p.segment).max().unwrap() >= 1);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn torn_tail_is_tolerated() {
        let dir = std::env::temp_dir().join(format!("kairos-wal-torn-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let wal = Wal::open(&dir).unwrap();
        wal.append(&sample("good", 2)).unwrap();
        wal.sync().unwrap();
        // Simulate a crash mid-append: garbage half-record at the tail.
        {
            let mut f = OpenOptions::new()
                .append(true)
                .open(segment_path(&dir, 0))
                .unwrap();
            f.write_all(&[0xFF, 0x00, 0x00, 0x00, 1, 2, 3]).unwrap();
        }
        let wal = Wal::open(&dir).unwrap();
        let records = wal.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].1.name, "good");
        let _ = fs::remove_dir_all(&dir);
    }
}
