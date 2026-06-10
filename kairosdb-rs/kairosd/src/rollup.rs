//! Rollups: continuous aggregation tasks, wire-compatible with the Java
//! `RollUpResource` API and task JSON
//! (`{name, execution_interval, rollups: [{save_as, query}]}`).
//!
//! Multi-node: tasks live in the datastore's service index under the same
//! keys as the Java `RollUpTasksStoreImpl` (service `_Rollups`, service_key
//! `Config`, key = task id, value = task JSON), so every node — and a Java
//! server on the same cluster — sees one task list. Nodes re-read the list
//! on an interval, and each execution tick is gated by a lease (service_key
//! `LeasesRs`): the holder renews, others skip, and an expired lease fails
//! over within roughly two execution intervals. Lease claims are
//! last-write-wins; a rare double execution writes identical points under
//! `save_as`, which is idempotent.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use kairos_core::time::add_units;
use kairos_core::DataPointSet;
use kairos_query::model::{QueryRequest, RelativeTime};
use kairos_store::Datastore;
use serde_json::{json, Value as JsonValue};
use tokio::task::JoinHandle;

use crate::api::run_metric_query;
use crate::ingest::Ingest;
use crate::store::AnyDatastore;

const SERVICE: &str = "_Rollups";
const SERVICE_KEY_CONFIG: &str = "Config"; // Java RollUpTasksStoreImpl
const SERVICE_KEY_LEASES: &str = "LeasesRs";
const MIN_LEASE_MS: i64 = 120_000;

pub struct RollupManager {
    store: Arc<AnyDatastore>,
    ingest: Ingest,
    file: Option<PathBuf>,
    fast_math: bool,
    node_id: String,
    tasks: Mutex<HashMap<String, TaskEntry>>,
}

struct TaskEntry {
    task: JsonValue,
    handle: JoinHandle<()>,
}

static TASK_COUNTER: AtomicU64 = AtomicU64::new(0);

fn new_task_id() -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_nanos();
    let count = TASK_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:024x}-{:08x}-{count:04x}", std::process::id())
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as i64
}

#[derive(Debug, thiserror::Error)]
pub enum RollupError {
    #[error("invalid rollup task: {0}")]
    Invalid(String),
    #[error("rollup task not found: {0}")]
    NotFound(String),
    #[error("rollup persistence failed: {0}")]
    Persist(String),
}

impl RollupManager {
    /// Loads the shared task list (importing a legacy `rollups.json` once if
    /// the index is empty), arms every task, and starts the refresh loop.
    pub async fn start(
        store: Arc<AnyDatastore>,
        ingest: Ingest,
        file: Option<PathBuf>,
        fast_math: bool,
        node_id: String,
        refresh_seconds: u64,
    ) -> Arc<RollupManager> {
        let manager = Arc::new(RollupManager {
            store,
            ingest,
            file,
            fast_math,
            node_id,
            tasks: Mutex::new(HashMap::new()),
        });

        if let Err(e) = manager.import_legacy_file().await {
            tracing::error!("rollup file import failed: {e}");
        }
        manager.refresh().await;

        if refresh_seconds > 0 {
            let refresher = Arc::downgrade(&manager);
            tokio::spawn(async move {
                let mut tick =
                    tokio::time::interval(std::time::Duration::from_secs(refresh_seconds));
                tick.tick().await;
                loop {
                    tick.tick().await;
                    let Some(manager) = refresher.upgrade() else { return };
                    manager.refresh().await;
                }
            });
        }
        manager
    }

    /// One-time migration: tasks from the pre-multi-node JSON file move into
    /// the shared index.
    async fn import_legacy_file(&self) -> Result<(), RollupError> {
        let Some(path) = &self.file else { return Ok(()) };
        let Ok(content) = std::fs::read_to_string(path) else { return Ok(()) };
        let existing = self
            .store
            .service_list_keys(SERVICE, SERVICE_KEY_CONFIG)
            .await
            .map_err(|e| RollupError::Persist(e.to_string()))?;
        if !existing.is_empty() {
            return Ok(());
        }
        let tasks: Vec<JsonValue> =
            serde_json::from_str(&content).map_err(|e| RollupError::Persist(e.to_string()))?;
        for task in &tasks {
            if let Some(id) = task.get("id").and_then(JsonValue::as_str) {
                self.store
                    .service_set(SERVICE, SERVICE_KEY_CONFIG, id, &task.to_string())
                    .await
                    .map_err(|e| RollupError::Persist(e.to_string()))?;
            }
        }
        if !tasks.is_empty() {
            tracing::info!("imported {} rollup tasks from {}", tasks.len(), path.display());
            let _ = std::fs::rename(path, path.with_extension("json.imported"));
        }
        Ok(())
    }

    /// Re-reads the shared task list: arms new/changed tasks, drops removed
    /// ones. This is how create/delete on one node reaches the others.
    pub async fn refresh(self: &Arc<Self>) {
        let keys = match self.store.service_list_keys(SERVICE, SERVICE_KEY_CONFIG).await {
            Ok(keys) => keys,
            Err(e) => {
                tracing::error!("rollup refresh failed: {e}");
                return;
            }
        };
        let mut seen = Vec::new();
        for id in keys {
            let value = match self.store.service_get(SERVICE, SERVICE_KEY_CONFIG, &id).await {
                Ok(Some(value)) => value,
                Ok(None) => continue,
                Err(e) => {
                    tracing::error!("rollup refresh get {id}: {e}");
                    continue;
                }
            };
            seen.push(id.clone());
            let task: JsonValue = match serde_json::from_str(&value) {
                Ok(task) => task,
                Err(e) => {
                    tracing::error!("rollup task {id} unparseable: {e}");
                    continue;
                }
            };
            let changed = {
                let tasks = self.tasks.lock().expect("rollup lock poisoned");
                tasks.get(&id).is_none_or(|entry| entry.task != task)
            };
            if changed {
                if let Err(e) = self.clone().arm_task(task) {
                    tracing::error!("cannot arm rollup {id}: {e}");
                }
            }
        }
        let mut tasks = self.tasks.lock().expect("rollup lock poisoned");
        tasks.retain(|id, entry| {
            let keep = seen.contains(id);
            if !keep {
                entry.handle.abort();
            }
            keep
        });
    }

    /// Validates, persists to the shared index, and arms a new task.
    pub async fn create(self: &Arc<Self>, mut task: JsonValue) -> Result<JsonValue, RollupError> {
        validate_task(&task)?;
        if task.get("id").and_then(JsonValue::as_str).is_none() {
            task["id"] = json!(new_task_id());
        }
        let id = task["id"].as_str().expect("set above").to_string();
        self.store
            .service_set(SERVICE, SERVICE_KEY_CONFIG, &id, &task.to_string())
            .await
            .map_err(|e| RollupError::Persist(e.to_string()))?;
        let stored = task.clone();
        self.clone().arm_task(task)?;
        Ok(stored)
    }

    pub fn list(&self) -> Vec<JsonValue> {
        let tasks = self.tasks.lock().expect("rollup lock poisoned");
        let mut list: Vec<JsonValue> = tasks.values().map(|e| e.task.clone()).collect();
        list.sort_by_key(|t| t["id"].as_str().unwrap_or_default().to_string());
        list
    }

    pub fn get(&self, id: &str) -> Option<JsonValue> {
        let tasks = self.tasks.lock().expect("rollup lock poisoned");
        tasks.get(id).map(|e| e.task.clone())
    }

    pub async fn delete(&self, id: &str) -> Result<(), RollupError> {
        let removed = {
            let mut tasks = self.tasks.lock().expect("rollup lock poisoned");
            tasks.remove(id)
        };
        let Some(entry) = removed else {
            return Err(RollupError::NotFound(id.to_string()));
        };
        entry.handle.abort();
        self.store
            .service_delete(SERVICE, SERVICE_KEY_CONFIG, id)
            .await
            .map_err(|e| RollupError::Persist(e.to_string()))?;
        let _ = self
            .store
            .service_delete(SERVICE, SERVICE_KEY_LEASES, id)
            .await;
        Ok(())
    }

    /// Lease check for one execution tick: claim or renew, last-write-wins.
    /// Returns false when another live node holds the task.
    pub async fn try_claim(&self, task_id: &str, interval_ms: i64) -> bool {
        let now = now_ms();
        match self.store.service_get(SERVICE, SERVICE_KEY_LEASES, task_id).await {
            Ok(Some(lease)) => {
                let lease: JsonValue = serde_json::from_str(&lease).unwrap_or(JsonValue::Null);
                let owner = lease["node"].as_str().unwrap_or_default();
                let expires = lease["expires_ms"].as_i64().unwrap_or(0);
                if owner != self.node_id && expires > now {
                    return false;
                }
            }
            Ok(None) => {}
            Err(e) => {
                tracing::error!("lease read for {task_id}: {e}");
                return false;
            }
        }
        let lease = json!({
            "node": self.node_id,
            "expires_ms": now + (2 * interval_ms).max(MIN_LEASE_MS),
        });
        match self
            .store
            .service_set(SERVICE, SERVICE_KEY_LEASES, task_id, &lease.to_string())
            .await
        {
            Ok(()) => true,
            Err(e) => {
                tracing::error!("lease write for {task_id}: {e}");
                false
            }
        }
    }

    /// Registers the task and spawns its execution loop.
    fn arm_task(self: Arc<Self>, task: JsonValue) -> Result<(), RollupError> {
        validate_task(&task)?;
        let id = task["id"]
            .as_str()
            .ok_or_else(|| RollupError::Invalid("task has no id".into()))?
            .to_string();
        let interval: RelativeTime = serde_json::from_value(task["execution_interval"].clone())
            .map_err(|e| RollupError::Invalid(format!("execution_interval: {e}")))?;
        let rollups = task["rollups"].clone();

        let manager = Arc::downgrade(&self);
        let task_id = id.clone();
        let handle = tokio::spawn(async move {
            let period_ms = (add_units(0, interval.unit, interval.value)).max(1000);
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(period_ms as u64));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // first tick fires immediately; skip it
            loop {
                tick.tick().await;
                let Some(manager) = manager.upgrade() else { return };
                if !manager.try_claim(&task_id, period_ms).await {
                    continue;
                }
                if let Err(e) = manager.execute_rollups(&rollups).await {
                    tracing::error!("rollup task {task_id} failed: {e}");
                }
            }
        });

        let mut tasks = self.tasks.lock().expect("rollup lock poisoned");
        if let Some(old) = tasks.insert(id, TaskEntry { task, handle }) {
            old.handle.abort();
        }
        Ok(())
    }

    /// Runs each rollup's query as written (Java executes the stored query
    /// verbatim each interval) and writes results under `save_as`.
    async fn execute_rollups(&self, rollups: &JsonValue) -> Result<(), String> {
        for rollup in rollups.as_array().into_iter().flatten() {
            let save_as = rollup["save_as"].as_str().ok_or("rollup missing save_as")?;
            let request: QueryRequest = serde_json::from_value(rollup["query"].clone())
                .map_err(|e| format!("rollup query: {e}"))?;
            let now_ms = chrono::Utc::now().timestamp_millis();
            let (start_ms, end_ms) = request
                .resolve_time_range(now_ms)
                .map_err(|e| e.to_string())?;
            let tz = request.parse_time_zone().map_err(|e| e.to_string())?;

            for metric in &request.metrics {
                let (_, groups, saved) =
                    run_metric_query(&self.store, metric, start_ms, end_ms, tz, self.fast_math, None)
                        .await
                        .map_err(|e| e.to_string())?;
                for set in saved {
                    self.ingest.submit(set).await.map_err(|e| e.to_string())?;
                }
                for group in groups {
                    if group.points.is_empty() {
                        continue;
                    }
                    let mut set = DataPointSet::new(save_as);
                    // Single-valued tags of the result group carry over to
                    // the rolled-up series.
                    for (k, values) in &group.tags {
                        if let [single] = values.as_slice() {
                            set.tags.insert(k.clone(), single.clone());
                        }
                    }
                    set.points = group.points;
                    self.ingest.submit(set).await.map_err(|e| e.to_string())?;
                }
            }
        }
        Ok(())
    }
}

fn validate_task(task: &JsonValue) -> Result<(), RollupError> {
    if task.get("name").and_then(JsonValue::as_str).is_none() {
        return Err(RollupError::Invalid("name is required".into()));
    }
    if task.get("execution_interval").is_none() {
        return Err(RollupError::Invalid("execution_interval is required".into()));
    }
    let rollups = task
        .get("rollups")
        .and_then(JsonValue::as_array)
        .ok_or_else(|| RollupError::Invalid("rollups array is required".into()))?;
    for rollup in rollups {
        if rollup.get("save_as").and_then(JsonValue::as_str).is_none() {
            return Err(RollupError::Invalid("rollup save_as is required".into()));
        }
        serde_json::from_value::<QueryRequest>(rollup["query"].clone())
            .map_err(|e| RollupError::Invalid(format!("rollup query: {e}")))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use kairos_store::memory::MemoryDatastore;

    fn task_json(name: &str) -> JsonValue {
        json!({
            "name": name,
            "execution_interval": {"value": 1, "unit": "hours"},
            "rollups": [{
                "save_as": "rolled",
                "query": {"start_relative": {"value": 1, "unit": "hours"},
                          "metrics": [{"name": "src"}]}
            }]
        })
    }

    async fn manager(store: &Arc<AnyDatastore>, node: &str) -> Arc<RollupManager> {
        let ingest = Ingest::start(None, store.clone()).await.unwrap();
        RollupManager::start(store.clone(), ingest, None, false, node.to_string(), 0).await
    }

    #[tokio::test]
    async fn tasks_propagate_between_nodes_via_shared_index() {
        let store = Arc::new(AnyDatastore::Memory(MemoryDatastore::new()));
        let a = manager(&store, "node-a").await;
        let b = manager(&store, "node-b").await;

        let stored = a.create(task_json("Shared")).await.unwrap();
        let id = stored["id"].as_str().unwrap().to_string();
        assert!(b.get(&id).is_none(), "not yet refreshed");
        b.refresh().await;
        assert_eq!(b.get(&id).unwrap()["name"], "Shared");

        // Delete on B propagates back to A on refresh.
        b.delete(&id).await.unwrap();
        a.refresh().await;
        assert!(a.get(&id).is_none());
    }

    #[tokio::test]
    async fn lease_grants_one_node_per_interval() {
        let store = Arc::new(AnyDatastore::Memory(MemoryDatastore::new()));
        let a = manager(&store, "node-a").await;
        let b = manager(&store, "node-b").await;

        assert!(a.try_claim("t1", 60_000).await);
        assert!(!b.try_claim("t1", 60_000).await, "lease held by a");
        assert!(a.try_claim("t1", 60_000).await, "holder renews");

        // An expired lease fails over.
        let stale = json!({"node": "node-a", "expires_ms": 1});
        store
            .service_set(SERVICE, SERVICE_KEY_LEASES, "t1", &stale.to_string())
            .await
            .unwrap();
        assert!(b.try_claim("t1", 60_000).await, "expired lease is claimable");
        assert!(!a.try_claim("t1", 60_000).await, "b now holds it");
    }
}
