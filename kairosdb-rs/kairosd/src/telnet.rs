//! Telnet protocol server, wire-compatible with
//! `org.kairosdb.core.telnet.TelnetServer` (default port 4242).
//!
//! Commands: `put <metric> <ts> <value> [tag=val ...]` (second-resolution
//! timestamps below 3000000000 are scaled to ms, the Java "30-year hack"),
//! `putm` (millisecond timestamps), `puts` (string values), and `version`.
//! The special tag `kairos_opt.ttl` sets the TTL, as in Java.

use kairos_core::{DataPointSet, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::ingest::Ingest;

pub async fn serve(listener: TcpListener, ingest: Ingest) {
    loop {
        let (socket, peer) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("telnet accept failed: {e}");
                continue;
            }
        };
        let ingest = ingest.clone();
        tokio::spawn(async move {
            let (read, mut write) = socket.into_split();
            let mut lines = BufReader::new(read).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                match handle_line(&line, &ingest).await {
                    Ok(Some(reply)) => {
                        if write.write_all(reply.as_bytes()).await.is_err() {
                            break;
                        }
                    }
                    Ok(None) => {}
                    Err(e) => tracing::debug!("telnet {peer}: {e}: {line}"),
                }
            }
        });
    }
}

async fn handle_line(line: &str, ingest: &Ingest) -> Result<Option<String>, String> {
    let words: Vec<&str> = line.split_whitespace().collect();
    let Some(command) = words.first() else { return Ok(None) };
    match *command {
        "version" => Ok(Some(format!(
            "KairosDB-rs {}\n",
            env!("CARGO_PKG_VERSION")
        ))),
        "put" | "putm" | "puts" => {
            let set = parse_put(*command, &words)?;
            ingest.submit(set).await.map_err(|e| e.to_string())?;
            Ok(None)
        }
        other => Err(format!("unknown command: {other}")),
    }
}

fn parse_put(command: &str, words: &[&str]) -> Result<DataPointSet, String> {
    if words.len() < 4 {
        return Err(format!("{command} needs metric, timestamp, and value"));
    }
    let mut timestamp: i64 = words[2].parse().map_err(|e| format!("timestamp: {e}"))?;
    if command == "put" && timestamp < 3_000_000_000 {
        // Java's backwards-compatible hack: clients may send seconds.
        timestamp *= 1000;
    }
    let value = if command == "puts" {
        Value::from(words[3])
    } else if words[3].contains('.') {
        Value::Double(words[3].parse().map_err(|e| format!("value: {e}"))?)
    } else {
        Value::Long(words[3].parse().map_err(|e| format!("value: {e}"))?)
    };

    let mut set = DataPointSet::new(words[1]).point(timestamp, value);
    for tag in &words[4..] {
        let (key, val) = tag
            .split_once('=')
            .ok_or_else(|| format!("malformed tag: {tag}"))?;
        if key.is_empty() || val.is_empty() {
            return Err(format!("malformed tag: {tag}"));
        }
        if key == "kairos_opt.ttl" {
            set.ttl = val.parse().map_err(|e| format!("ttl: {e}"))?;
        } else {
            set.tags.insert(key.to_string(), val.to_string());
        }
    }
    Ok(set)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_scales_second_timestamps() {
        let set = parse_put("put", &["put", "m", "1700000000", "42", "host=a"]).unwrap();
        assert_eq!(set.points[0].timestamp_ms, 1_700_000_000_000);
        assert_eq!(set.points[0].value, Value::Long(42));
        assert_eq!(set.tags["host"], "a");
    }

    #[test]
    fn putm_keeps_milliseconds_and_parses_doubles() {
        let set = parse_put("putm", &["putm", "m", "1500", "4.5"]).unwrap();
        assert_eq!(set.points[0].timestamp_ms, 1_500);
        assert_eq!(set.points[0].value, Value::Double(4.5));
    }

    #[test]
    fn puts_takes_string_values_and_ttl_tag() {
        let set = parse_put(
            "puts",
            &["puts", "m", "1500", "settled", "kairos_opt.ttl=60", "a=b"],
        )
        .unwrap();
        assert_eq!(set.points[0].value, Value::from("settled"));
        assert_eq!(set.ttl, 60);
        assert_eq!(set.tags.len(), 1);
    }
}
