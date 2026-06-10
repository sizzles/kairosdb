//! Rollups: continuous aggregation tasks, wire-compatible with the Java
//! `RollUpResource` API and task JSON
//! (`{name, execution_interval, rollups: [{save_as, query}]}`).
//!
//! Tasks persist to a JSON file in the data directory and each runs on its
//! own tokio interval; results are written back through the durable ingest
//! pipeline under the `save_as` metric name. Single-node scheduling only —
//! the Java cluster assignment machinery is out of scope for now.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use kairos_core::time::add_units;
use kairos_core::DataPointSet;
use kairos_query::model::{QueryRequest, RelativeTime};
use serde_json::{json, Value as JsonValue};
use tokio::task::JoinHandle;

use crate::api::run_metric_query;
use crate::ingest::Ingest;
use crate::store::AnyDatastore;

pub struct RollupManager {
    store: Arc<AnyDatastore>,
    ingest: Ingest,
    file: Option<PathBuf>,
    fast_math: bool,
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
    /// Creates the manager and re-arms any tasks persisted in `file`.
    pub fn start(
        store: Arc<AnyDatastore>,
        ingest: Ingest,
        file: Option<PathBuf>,
        fast_math: bool,
    ) -> Arc<RollupManager> {
        let manager = Arc::new(RollupManager {
            store,
            ingest,
            file,
            fast_math,
            tasks: Mutex::new(HashMap::new()),
        });

        if let Some(path) = &manager.file {
            if let Ok(content) = std::fs::read_to_string(path) {
                match serde_json::from_str::<Vec<JsonValue>>(&content) {
                    Ok(saved) => {
                        for task in saved {
                            if let Err(e) = manager.clone().arm_task(task) {
                                tracing::error!("skipping persisted rollup: {e}");
                            }
                        }
                    }
                    Err(e) => tracing::error!("cannot parse rollup file: {e}"),
                }
            }
        }
        manager
    }

    /// Validates, registers, schedules, and persists a new task. Returns the
    /// stored task JSON (with its assigned id).
    pub fn create(self: &Arc<Self>, mut task: JsonValue) -> Result<JsonValue, RollupError> {
        validate_task(&task)?;
        if task.get("id").and_then(JsonValue::as_str).is_none() {
            task["id"] = json!(new_task_id());
        }
        let stored = task.clone();
        self.clone().arm_task(task)?;
        self.persist()?;
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

    pub fn delete(&self, id: &str) -> Result<(), RollupError> {
        let removed = {
            let mut tasks = self.tasks.lock().expect("rollup lock poisoned");
            tasks.remove(id)
        };
        match removed {
            Some(entry) => {
                entry.handle.abort();
                self.persist()
            }
            None => Err(RollupError::NotFound(id.to_string())),
        }
    }

    fn persist(&self) -> Result<(), RollupError> {
        let Some(path) = &self.file else { return Ok(()) };
        let list = self.list();
        let content =
            serde_json::to_string_pretty(&list).map_err(|e| RollupError::Persist(e.to_string()))?;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| RollupError::Persist(e.to_string()))?;
        }
        std::fs::write(path, content).map_err(|e| RollupError::Persist(e.to_string()))
    }

    /// Registers the task and spawns its execution loop.
    fn arm_task(self: Arc<Self>, task: JsonValue) -> Result<(), RollupError> {
        validate_task(&task)?;
        let id = task["id"]
            .as_str()
            .ok_or_else(|| RollupError::Invalid("task has no id".into()))?
            .to_string();
        let interval: RelativeTime =
            serde_json::from_value(task["execution_interval"].clone())
                .map_err(|e| RollupError::Invalid(format!("execution_interval: {e}")))?;
        let rollups = task["rollups"].clone();

        let manager = self.clone();
        let task_id = id.clone();
        let handle = tokio::spawn(async move {
            let period_ms = (add_units(0, interval.unit, interval.value)).max(1000) as u64;
            let mut tick =
                tokio::time::interval(std::time::Duration::from_millis(period_ms));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            tick.tick().await; // first tick fires immediately; skip it
            loop {
                tick.tick().await;
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
            let save_as = rollup["save_as"]
                .as_str()
                .ok_or("rollup missing save_as")?;
            let request: QueryRequest = serde_json::from_value(rollup["query"].clone())
                .map_err(|e| format!("rollup query: {e}"))?;
            let now_ms = chrono::Utc::now().timestamp_millis();
            let (start_ms, end_ms) = request
                .resolve_time_range(now_ms)
                .map_err(|e| e.to_string())?;
            let tz = request.parse_time_zone().map_err(|e| e.to_string())?;

            for metric in &request.metrics {
                let (_, groups, saved) =
                    run_metric_query(&self.store, metric, start_ms, end_ms, tz, self.fast_math)
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
