//! Ingest pipeline: WAL append → bounded queue → batched datastore writes,
//! replacing the Java event-bus + FileQueueProcessor arrangement. The HTTP
//! handler acks once the set is in the WAL and queued; a consumer task
//! drains the queue into the datastore and advances the WAL checkpoint.
//! On startup, anything in the WAL past the checkpoint is replayed.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
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

                // Sets are independent; drain the batch concurrently. Only
                // checkpoint when everything landed — failed sets stay in
                // the WAL for replay on restart.
                let batch_pos = batch.iter().filter_map(|(_, pos)| *pos).max();
                let results: Vec<Result<(), kairos_store::Error>> =
                    futures::stream::iter(batch.drain(..).map(|(set, _)| store.write(set)))
                        .buffer_unordered(WRITE_CONCURRENCY)
                        .collect()
                        .await;
                let mut all_ok = true;
                for result in results {
                    if let Err(e) = result {
                        all_ok = false;
                        tracing::error!("datastore write failed: {e}");
                    }
                }
                if all_ok {
                    if let (Some(wal), Some(pos)) = (&consumer_wal, batch_pos) {
                        if let Err(e) = wal.checkpoint(pos) {
                            tracing::error!("wal checkpoint failed: {e}");
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

    /// Durable enqueue: WAL append, then hand to the consumer. Applies
    /// backpressure when the queue is full.
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
}
