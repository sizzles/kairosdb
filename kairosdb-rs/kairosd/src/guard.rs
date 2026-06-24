//! Query guards: concurrency cap, wall-clock timeout, scan budget, and the
//! running-query registry behind `/api/v1/runningqueries` and
//! `/api/v1/killquery/{id}` (the Java admin endpoints).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use tokio::sync::Semaphore;
use tokio::task::AbortHandle;

use crate::config::LimitsConfig;

pub struct QueryGuard {
    semaphore: Option<Semaphore>,
    pub timeout: Option<Duration>,
    pub max_points: Option<u64>,
    next_id: AtomicU64,
    running: Mutex<HashMap<u64, RunningQuery>>,
}

struct RunningQuery {
    summary: String,
    started: Instant,
    abort: AbortHandle,
}

/// Held for the lifetime of a query; releasing it frees the concurrency slot.
pub struct SlotPermit<'a> {
    _permit: Option<tokio::sync::SemaphorePermit<'a>>,
}

/// Held while a query is running; on drop it deregisters from the running list.
pub struct QueryRegistration<'a> {
    guard: &'a QueryGuard,
    id: u64,
}

impl QueryGuard {
    pub fn new(limits: &LimitsConfig) -> QueryGuard {
        QueryGuard {
            semaphore: (limits.max_concurrent_queries > 0)
                .then(|| Semaphore::new(limits.max_concurrent_queries as usize)),
            timeout: (limits.query_timeout_ms > 0)
                .then(|| Duration::from_millis(limits.query_timeout_ms)),
            max_points: (limits.max_query_points > 0).then_some(limits.max_query_points),
            next_id: AtomicU64::new(1),
            running: Mutex::new(HashMap::new()),
        }
    }

    /// Acquire a concurrency slot. This must be awaited **before** the query
    /// work is spawned, so `max_concurrent_queries` bounds actual execution
    /// (not just the number of handlers waiting on a join). The wait is
    /// bounded by the query timeout, so a saturated server sheds load with a
    /// 503 instead of queueing forever.
    pub async fn acquire(&self) -> Result<SlotPermit<'_>, String> {
        let permit = match &self.semaphore {
            None => None,
            Some(semaphore) => {
                let acquire = semaphore.acquire();
                let permit = match self.timeout {
                    Some(timeout) => tokio::time::timeout(timeout, acquire)
                        .await
                        .map_err(|_| "timed out waiting for a query slot".to_string())?,
                    None => acquire.await,
                };
                Some(permit.map_err(|_| "query guard closed".to_string())?)
            }
        };
        Ok(SlotPermit { _permit: permit })
    }

    /// Register a now-running query so it appears in `/runningqueries` and can
    /// be aborted via `/killquery/{id}`. The returned guard deregisters on drop.
    pub fn register(&self, summary: String, abort: AbortHandle) -> QueryRegistration<'_> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.running.lock().expect("guard lock poisoned").insert(
            id,
            RunningQuery { summary, started: Instant::now(), abort },
        );
        QueryRegistration { guard: self, id }
    }

    pub fn running(&self) -> Vec<serde_json::Value> {
        let running = self.running.lock().expect("guard lock poisoned");
        let mut list: Vec<_> = running
            .iter()
            .map(|(id, q)| {
                serde_json::json!({
                    "id": id,
                    "query": q.summary,
                    "elapsed_ms": q.started.elapsed().as_millis() as u64,
                })
            })
            .collect();
        list.sort_by_key(|q| q["id"].as_u64());
        list
    }

    /// Aborts a running query; returns false when the id is unknown (already
    /// finished or never existed).
    pub fn kill(&self, id: u64) -> bool {
        let running = self.running.lock().expect("guard lock poisoned");
        match running.get(&id) {
            Some(q) => {
                q.abort.abort();
                true
            }
            None => false,
        }
    }
}

impl Drop for QueryRegistration<'_> {
    fn drop(&mut self) {
        self.guard
            .running
            .lock()
            .expect("guard lock poisoned")
            .remove(&self.id);
    }
}
