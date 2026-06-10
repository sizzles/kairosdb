//! Core data model for the KairosDB Rust port.
//!
//! Byte-level and wire-level compatibility with the Java implementation is a
//! hard requirement of this crate: the varint value codec matches
//! `org.kairosdb.util.Util.packLong`, the value serialization matches the
//! `DataPointFactory` implementations, and the time/sampling semantics match
//! `org.kairosdb.core.aggregator.RangeAggregator`.

pub mod datapoint;
pub mod time;
pub mod value;
pub mod varint;

pub use datapoint::{DataPoint, DataPointSet, Tags};
pub use time::{Sampling, TimeUnit};
pub use value::Value;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("buffer underflow while decoding")]
    Underflow,
    #[error("varint is longer than 64 bits")]
    VarintTooLong,
    #[error("invalid UTF-8 in encoded data: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),
    #[error("invalid encoding: {0}")]
    InvalidEncoding(String),
    #[error("unknown datastore type: {0}")]
    UnknownDataType(String),
}

pub type Result<T> = std::result::Result<T, Error>;
