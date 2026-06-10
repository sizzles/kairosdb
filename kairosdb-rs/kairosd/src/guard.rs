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

pub struct QueryPermit<'a> {
    guard: &'a QueryGuard,
    id: u64,
    _permit: Option<tokio::sync::SemaphorePermit<'a>>,
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

    /// Waits for a concurrency slot (bounded by the query timeout, so a full
    /// server sheds load instead of queueing forever) and registers the
    /// query as running.
    pub async fn admit(
        &self,
        summary: String,
        abort: AbortHandle,
    ) -> Result<QueryPermit<'_>, String> {
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
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.running.lock().expect("guard lock poisoned").insert(
            id,
            RunningQuery { summary, started: Instant::now(), abort },
        );
        Ok(QueryPermit { guard: self, id, _permit: permit })
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

impl Drop for QueryPermit<'_> {
    fn drop(&mut self) {
        self.guard
            .running
            .lock()
            .expect("guard lock poisoned")
            .remove(&self.id);
    }
}
