//! Ingest pipeline: WAL append → bounded queue → batched datastore writes,
//! replacing the Java event-bus + FileQueueProcessor arrangement. The HTTP
//! handler acks once the set is in the WAL and queued; a consumer task
//! drains the queue into the datastore and advances the WAL checkpoint.
//! On startup, anything in the WAL past the checkpoint is replayed.

use std::sync::Arc;
use std::time::Duration;

use futures::{FutureExt, StreamExt};
use kairos_core::DataPointSet;
use kairos_store::wal::{Wal, WalPosition};
use kairos_store::Datastore;
use tokio::sync::mpsc;

use crate::store::AnyDatastore;

const QUEUE_DEPTH: usize = 8192;
const BATCH_SIZE: usize = 256;
const BATCH_LINGER: Duration = Duration::from_millis(100);
const WAL_SYNC_INTERVAL: Duration = Duration::from_millis(100);
const WRITE_CONCURRENCY: usize = 8;

#[derive(Clone)]
pub struct Ingest {
    tx: mpsc::Sender<(DataPointSet, Option<WalPosition>)>,
    wal: Option<Arc<Wal>>,
}

impl Ingest {
    /// Replays any unflushed WAL records, then starts the consumer and the
    /// periodic WAL fsync task.
    pub async fn start(
        wal: Option<Arc<Wal>>,
        store: Arc<AnyDatastore>,
    ) -> Result<Ingest, kairos_store::Error> {
        if let Some(wal) = &wal {
            let records = wal.replay()?;
            if !records.is_empty() {
                tracing::info!("replaying {} datapoint sets from wal", records.len());
                crate::metrics::add(&crate::metrics::WAL_REPLAYED_SETS, records.len() as u64);
                let mut last = None;
                for (pos, set) in records {
                    store.write(set).await?;
                    last = Some(pos);
                }
                if let Some(pos) = last {
                    wal.checkpoint(pos)?;
                }
            }
        }

        let (tx, mut rx) = mpsc::channel::<(DataPointSet, Option<WalPosition>)>(QUEUE_DEPTH);

        let consumer_wal = wal.clone();
        tokio::spawn(async move {
            let mut batch = Vec::with_capacity(BATCH_SIZE);
            // The earliest WAL position whose datastore write failed. We must
            // never checkpoint at or past it: that record (and everything
            // after) has to stay replayable. Without this, a max-position
            // checkpoint could leap over an earlier failed-but-acked set and
            // silently lose it on restart.
            let mut first_failed: Option<WalPosition> = None;
            let mut last_checkpointed: Option<WalPosition> = None;
            loop {
                batch.clear();
                match rx.recv().await {
                    Some(item) => batch.push(item),
                    None => return, // server shutting down
                }
                let deadline = tokio::time::Instant::now() + BATCH_LINGER;
                while batch.len() < BATCH_SIZE {
                    match tokio::time::timeout_at(deadline, rx.recv()).await {
                        Ok(Some(item)) => batch.push(item),
                        Ok(None) | Err(_) => break,
                    }
                }

                // Drain concurrently; each result carries its WAL position so
                // we know exactly which set failed (buffer_unordered yields in
                // completion order, so the position must travel with it).
                let results: Vec<(Option<WalPosition>, Result<(), kairos_store::Error>)> =
                    futures::stream::iter(
                        batch
                            .drain(..)
                            .map(|(set, pos)| store.write(set).map(move |r| (pos, r))),
                    )
                    .buffer_unordered(WRITE_CONCURRENCY)
                    .collect()
                    .await;

                for (pos, result) in &results {
                    if let Err(e) = result {
                        tracing::error!("datastore write failed: {e}");
                        if let Some(p) = pos {
                            first_failed = Some(first_failed.map_or(*p, |f| f.min(*p)));
                        }
                    }
                }

                // Highest successful position safe to checkpoint: the batch
                // max, clamped to strictly before the earliest failure ever
                // seen so the failed record stays in the WAL for replay.
                let candidate = results
                    .iter()
                    .filter_map(|(p, r)| if r.is_ok() { *p } else { None })
                    .filter(|p| first_failed.is_none_or(|f| *p < f))
                    .max();

                if let (Some(wal), Some(pos)) = (&consumer_wal, candidate) {
                    if last_checkpointed.is_none_or(|c| pos > c) {
                        if let Err(e) = wal.checkpoint(pos) {
                            tracing::error!("wal checkpoint failed: {e}");
                        } else {
                            last_checkpointed = Some(pos);
                        }
                    }
                }
            }
        });

        if let Some(wal) = wal.clone() {
            tokio::spawn(async move {
                let mut tick = tokio::time::interval(WAL_SYNC_INTERVAL);
                loop {
                    tick.tick().await;
                    if let Err(e) = wal.sync() {
                        tracing::error!("wal sync failed: {e}");
                    }
                }
            });
        }

        Ok(Ingest { tx, wal })
    }

    /// Enqueue durably: append to the WAL, then hand to the consumer. The
    /// record is fsynced by the periodic sync task (within `WAL_SYNC_INTERVAL`)
    /// or by `sync_wal()` on shutdown — not per call. Applies backpressure
    /// when the queue is full.
    pub async fn submit(&self, set: DataPointSet) -> Result<(), kairos_store::Error> {
        let pos = match &self.wal {
            Some(wal) => Some(wal.append(&set)?),
            None => None,
        };
        self.tx
            .send((set, pos))
            .await
            .map_err(|_| kairos_store::Error::Datastore("ingest queue closed".into()))
    }

    /// Flush and fsync the WAL. Called on graceful shutdown so records
    /// appended in the gap since the last periodic sync reach disk before the
    /// process exits (mirrors Java FileQueueProcessor's flush-on-shutdown).
    pub fn sync_wal(&self) -> Result<(), kairos_store::Error> {
        match &self.wal {
            Some(wal) => wal.sync(),
            None => Ok(()),
        }
    }
}
