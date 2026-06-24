//! `/api/v1` handlers, wire-compatible with the Java `MetricsResource`.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use kairos_core::{DataPoint, DataPointSet, Value};
use kairos_query::model::{GroupResult, MetricQuery, QueryRequest, SeriesInput};
use kairos_store::{Datastore, DatastoreQuery};
use serde_json::{json, Value as JsonValue};

use crate::guard::QueryGuard;
use crate::ingest::Ingest;
use crate::rollup::{RollupError, RollupManager};
use crate::store::AnyDatastore;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<AnyDatastore>,
    pub ingest: Ingest,
    pub rollups: Arc<RollupManager>,
    pub guard: Arc<QueryGuard>,
    /// `query_mode = "fast"` enables vectorized sum/avg/dev kernels
    /// (last-ulp float divergence from Java); `compat` (default) stays
    /// bit-identical.
    pub fast_math: bool,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/version", get(version))
        .route("/api/v1/metricnames", get(metric_names))
        .route("/api/v1/datapoints", post(add_datapoints).put(add_datapoints))
        .route("/api/v1/datapoints/query", post(query_datapoints))
        .route("/api/v1/datapoints/query/tags", post(query_metric_tags))
        .route("/api/v1/datapoints/delete", post(delete_datapoints))
        .route("/api/v1/metric/{name}", delete(delete_metric))
        .route("/api/v1/health/check", get(health_check))
        .route("/api/v1/health/status", get(health_status))
        .route("/api/v1/features", get(list_features))
        .route("/api/v1/features/{feature}", get(get_feature))
        .route("/api/v1/admin/compact", post(compact))
        .route("/api/v1/runningqueries", get(running_queries))
        .route("/api/v1/killquery/{id}", delete(kill_query).post(kill_query))
        .route("/metrics", get(prometheus_metrics))
        .route("/api/v1/rollups", post(create_rollup).get(list_rollups))
        .route(
            "/api/v1/rollups/{id}",
            delete(delete_rollup).get(get_rollup),
        )
        .with_state(state)
}

/// Errors are returned as `{"errors": [...]}` with a 400, like the Java
/// `ErrorResponse`.
struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({ "errors": [self.1] }))).into_response()
    }
}

fn bad_request(msg: impl Into<String>) -> ApiError {
    ApiError(StatusCode::BAD_REQUEST, msg.into())
}

async fn prometheus_metrics() -> ([(axum::http::HeaderName, &'static str); 1], String) {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        crate::metrics::render(),
    )
}

async fn version() -> Json<JsonValue> {
    Json(json!({ "version": concat!("KairosDB-rs ", env!("CARGO_PKG_VERSION")) }))
}

#[derive(serde::Deserialize, Default)]
struct MetricNamesParams {
    prefix: Option<String>,
}

async fn metric_names(
    State(state): State<AppState>,
    Query(params): Query<MetricNamesParams>,
) -> Result<Json<JsonValue>, ApiError> {
    let names = state
        .store
        .metric_names(params.prefix.as_deref())
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(json!({ "results": names })))
}

/// Ingest format: an array of metric objects (a single object is also
/// accepted), each carrying `tags` plus either `datapoints: [[ts, value]]`
/// or a single `timestamp`/`value` pair. The body may be gzip-compressed
/// (`Content-Encoding: gzip`), as the Java server accepts. Sets are
/// acknowledged once appended to the WAL and queued (group-commit fsync
/// within ~100ms, or on shutdown), not once stored in the datastore.
async fn add_datapoints(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
    raw_body: axum::body::Bytes,
) -> Result<StatusCode, ApiError> {
    let gzipped = headers
        .get(axum::http::header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v.eq_ignore_ascii_case("gzip"));
    let body: JsonValue = if gzipped {
        let mut decoder = flate2::read::GzDecoder::new(raw_body.as_ref());
        serde_json::from_reader(&mut decoder)
            .map_err(|e| bad_request(format!("invalid gzip json: {e}")))?
    } else {
        serde_json::from_slice(&raw_body).map_err(|e| bad_request(format!("invalid json: {e}")))?
    };

    let metrics: Vec<&JsonValue> = match &body {
        JsonValue::Array(items) => items.iter().collect(),
        JsonValue::Object(_) => vec![&body],
        _ => return Err(bad_request("metric[0].name may not be empty")),
    };

    crate::metrics::inc(&crate::metrics::INGEST_REQUESTS);
    for metric in metrics {
        let set = parse_metric(metric)?;
        crate::metrics::add(&crate::metrics::DATAPOINTS_INGESTED, set.points.len() as u64);
        state
            .ingest
            .submit(set)
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    Ok(StatusCode::NO_CONTENT)
}

fn parse_metric(metric: &JsonValue) -> Result<DataPointSet, ApiError> {
    let name = metric
        .get("name")
        .and_then(JsonValue::as_str)
        .filter(|n| !n.is_empty())
        .ok_or_else(|| bad_request("metric name may not be empty"))?;

    let mut set = DataPointSet::new(name);

    if let Some(tags) = metric.get("tags").and_then(JsonValue::as_object) {
        for (key, value) in tags {
            let value = value
                .as_str()
                .ok_or_else(|| bad_request(format!("tag {key} value must be a string")))?;
            set.tags.insert(key.clone(), value.to_string());
        }
    }
    if let Some(ttl) = metric.get("ttl").and_then(JsonValue::as_u64) {
        set.ttl = ttl as u32;
    }

    if let Some(datapoints) = metric.get("datapoints").and_then(JsonValue::as_array) {
        for entry in datapoints {
            let pair = entry
                .as_array()
                .filter(|p| p.len() == 2)
                .ok_or_else(|| bad_request("datapoints entries must be [timestamp, value]"))?;
            let ts = pair[0]
                .as_i64()
                .ok_or_else(|| bad_request("timestamp must be an integer"))?;
            set.points.push(DataPoint {
                timestamp_ms: ts,
                value: parse_value(&pair[1])?,
            });
        }
    } else if let (Some(ts), Some(value)) = (
        metric.get("timestamp").and_then(JsonValue::as_i64),
        metric.get("value"),
    ) {
        set.points.push(DataPoint {
            timestamp_ms: ts,
            value: parse_value(value)?,
        });
    } else {
        return Err(bad_request(format!(
            "metric {name} must contain datapoints or a timestamp/value pair"
        )));
    }

    Ok(set)
}

fn parse_value(value: &JsonValue) -> Result<Value, ApiError> {
    match value {
        JsonValue::Number(n) => {
            if let Some(v) = n.as_i64() {
                Ok(Value::Long(v))
            } else if let Some(v) = n.as_f64() {
                Ok(Value::Double(v))
            } else {
                Err(bad_request(format!("unsupported numeric value: {n}")))
            }
        }
        JsonValue::String(s) => Ok(Value::Text(s.as_str().into())),
        other => Err(bad_request(format!("unsupported value: {other}"))),
    }
}

/// Datastore fetch + group/aggregate pipeline for one metric query; shared
/// between the query endpoint and the rollup executor. The third element is
/// the series produced by `save_as` aggregators, which the caller must
/// submit to ingest.
pub(crate) async fn run_metric_query(
    store: &AnyDatastore,
    metric: &MetricQuery,
    start_ms: i64,
    end_ms: i64,
    tz: chrono_tz::Tz,
    fast: bool,
    max_points: Option<u64>,
) -> Result<(usize, Vec<GroupResult>, Vec<DataPointSet>), String> {
    let check_budget = |sample: usize| -> Result<(), String> {
        match max_points {
            Some(max) if sample as u64 > max => Err(format!(
                "query scanned {sample} points, exceeding the max_query_points limit of {max}"
            )),
            _ => Ok(()),
        }
    };
    let dq = DatastoreQuery {
        metric: metric.name.clone(),
        start_time_ms: start_ms,
        end_time_ms: end_ms,
        tags: metric.tag_filter(),
        limit: metric.limit,
        descending: metric.descending(),
    };

    // Zero-materialization path: aggregated scans served entirely from the
    // Parquet tier stream columns straight into the kernels. Raw and
    // limited queries need rows (value types, limit semantics).
    if metric.limit.is_none() && !metric.aggregators.is_empty() {
        if let Some(cols) = store.query_columns(&dq).await.map_err(|e| e.to_string())? {
            crate::metrics::inc(&crate::metrics::COLUMNAR_QUERIES);
            let sample_size: usize = cols.iter().map(|c| c.timestamps.len()).sum();
            check_budget(sample_size)?;
            let (groups, saved) =
                kairos_query::model::execute_columnar(metric, cols, start_ms, end_ms, tz, fast)
                    .map_err(|e| e.to_string())?;
            return Ok((sample_size, groups, saved));
        }
    }

    let series = store.query(&dq).await.map_err(|e| e.to_string())?;

    let sample_size: usize = series.iter().map(|s| s.points.len()).sum();
    check_budget(sample_size)?;
    let inputs: Vec<SeriesInput> = series
        .into_iter()
        .map(|s| SeriesInput { tags: s.tags, points: s.points })
        .collect();

    let (groups, saved) = kairos_query::model::execute(metric, inputs, start_ms, end_ms, tz, fast)
        .map_err(|e| e.to_string())?;
    Ok((sample_size, groups, saved))
}

async fn query_datapoints(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let started = std::time::Instant::now();
    crate::metrics::inc(&crate::metrics::QUERIES);
    let result = query_datapoints_guarded(state, request).await;
    if result.is_err() {
        crate::metrics::inc(&crate::metrics::QUERY_ERRORS);
    }
    crate::metrics::add(
        &crate::metrics::QUERY_MILLIS,
        started.elapsed().as_millis() as u64,
    );
    result
}

/// Applies the control plane to one query: a concurrency slot, a kill-able
/// task, and a wall-clock budget.
async fn query_datapoints_guarded(
    state: AppState,
    request: QueryRequest,
) -> Result<Json<JsonValue>, ApiError> {
    let summary = request
        .metrics
        .iter()
        .map(|m| m.name.as_str())
        .collect::<Vec<_>>()
        .join(",");

    // Acquire a concurrency slot BEFORE spawning the work, so the cap bounds
    // real execution and a saturated server sheds load (503) instead of
    // running every query at once.
    let _slot = state
        .guard
        .acquire()
        .await
        .map_err(|e| ApiError(StatusCode::SERVICE_UNAVAILABLE, e))?;

    let inner_state = state.clone();
    let task = tokio::spawn(query_datapoints_inner(inner_state, request));
    let abort = task.abort_handle();
    let _reg = state.guard.register(summary, abort.clone());

    let joined = match state.guard.timeout {
        Some(timeout) => match tokio::time::timeout(timeout, task).await {
            Ok(joined) => joined,
            Err(_) => {
                abort.abort();
                return Err(ApiError(
                    StatusCode::SERVICE_UNAVAILABLE,
                    format!("query exceeded the {}ms time budget", timeout.as_millis()),
                ));
            }
        },
        None => task.await,
    };
    match joined {
        Ok(result) => result,
        Err(e) if e.is_cancelled() => Err(ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            "query was killed".to_string(),
        )),
        Err(e) => Err(ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())),
    }
}

async fn running_queries(State(state): State<AppState>) -> Json<JsonValue> {
    Json(JsonValue::Array(state.guard.running()))
}

async fn kill_query(
    State(state): State<AppState>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    if state.guard.kill(id) {
        Ok(StatusCode::NO_CONTENT)
    } else {
        Err(ApiError(StatusCode::NOT_FOUND, format!("no running query {id}")))
    }
}

async fn query_datapoints_inner(
    state: AppState,
    request: QueryRequest,
) -> Result<Json<JsonValue>, ApiError> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (start_ms, end_ms) = request
        .resolve_time_range(now_ms)
        .map_err(|e| bad_request(e.to_string()))?;
    let tz = request
        .parse_time_zone()
        .map_err(|e| bad_request(e.to_string()))?;

    let mut queries = Vec::new();
    for metric in &request.metrics {
        let (sample_size, groups, saved) = run_metric_query(
            &state.store,
            metric,
            start_ms,
            end_ms,
            tz,
            state.fast_math,
            state.guard.max_points,
        )
        .await
        .map_err(bad_request)?;
        for set in saved {
            state
                .ingest
                .submit(set)
                .await
                .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }

        let results: Vec<JsonValue> = groups
            .into_iter()
            .map(|g| {
                let mut group_by = vec![json!({"name": "type", "type": "number"})];
                group_by.extend(g.group_by_entries.iter().cloned());
                json!({
                    "name": metric.name,
                    "group_by": group_by,
                    "tags": g.tags,
                    "values": g.points.iter().map(value_pair).collect::<Vec<_>>(),
                })
            })
            .collect();

        crate::metrics::add(&crate::metrics::QUERY_SAMPLE_POINTS, sample_size as u64);
        queries.push(json!({ "sample_size": sample_size, "results": results }));
    }

    Ok(Json(json!({ "queries": queries })))
}

/// `POST /api/v1/datapoints/query/tags`: same query JSON, but returns only
/// the matching series' tag sets (used by Grafana's tag pickers).
async fn query_metric_tags(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (start_ms, end_ms) = request
        .resolve_time_range(now_ms)
        .map_err(|e| bad_request(e.to_string()))?;

    let mut results = Vec::new();
    for metric in &request.metrics {
        let series = state
            .store
            .query(&DatastoreQuery {
                metric: metric.name.clone(),
                start_time_ms: start_ms,
                end_time_ms: end_ms,
                tags: metric.tag_filter(),
                limit: None,
                descending: false,
            })
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let mut tags: std::collections::BTreeMap<String, Vec<String>> = Default::default();
        for s in &series {
            for (k, v) in &s.tags {
                let values = tags.entry(k.clone()).or_default();
                if !values.contains(v) {
                    values.push(v.clone());
                }
            }
        }
        results.push(json!({"name": metric.name, "tags": tags}));
    }
    Ok(Json(json!({ "queries": [{ "results": results }] })))
}

/// `POST /api/v1/datapoints/delete`: same query JSON; removes the matching
/// points.
async fn delete_datapoints(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<StatusCode, ApiError> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (start_ms, end_ms) = request
        .resolve_time_range(now_ms)
        .map_err(|e| bad_request(e.to_string()))?;
    for metric in &request.metrics {
        state
            .store
            .delete(&DatastoreQuery {
                metric: metric.name.clone(),
                start_time_ms: start_ms,
                end_time_ms: end_ms,
                tags: metric.tag_filter(),
                limit: None,
                descending: false,
            })
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    Ok(StatusCode::NO_CONTENT)
}

/// `DELETE /api/v1/metric/{name}`: removes every point of the metric.
async fn delete_metric(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    state
        .store
        .delete(&DatastoreQuery {
            metric: name,
            start_time_ms: i64::MIN,
            end_time_ms: i64::MAX,
            tags: Default::default(),
            limit: None,
            descending: false,
        })
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(StatusCode::NO_CONTENT)
}

/// `GET /api/v1/health/check`: 204 when the datastore answers, 500 when not
/// (the Java `HealthCheckResource` contract).
async fn health_check(State(state): State<AppState>) -> StatusCode {
    match state.store.metric_names(Some("\u{0}")).await {
        Ok(_) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// `GET /api/v1/health/status`: the Java "Name: OK/FAIL" string array.
async fn health_status(State(state): State<AppState>) -> Json<JsonValue> {
    let datastore = match state.store.metric_names(Some("\u{0}")).await {
        Ok(_) => "Datastore-Query: OK",
        Err(_) => "Datastore-Query: FAIL",
    };
    Json(json!([datastore, "Ingest-Queue: OK"]))
}

/// `POST /api/v1/admin/compact` `{"older_than_ms": N}` (default 0: compact
/// everything up to now): moves hot points into the Parquet cold tier.
async fn compact(
    State(state): State<AppState>,
    Json(body): Json<JsonValue>,
) -> Result<Json<JsonValue>, ApiError> {
    let older_than = body.get("older_than_ms").and_then(JsonValue::as_i64).unwrap_or(0);
    let cutoff = chrono::Utc::now().timestamp_millis() - older_than;
    let moved = state
        .store
        .compact(cutoff)
        .await
        .map_err(|e| bad_request(e.to_string()))?;
    crate::metrics::inc(&crate::metrics::COMPACTIONS);
    crate::metrics::add(&crate::metrics::POINTS_COMPACTED, moved as u64);
    Ok(Json(json!({ "moved": moved, "cutoff": cutoff })))
}

async fn list_features() -> Json<JsonValue> {
    Json(crate::features::features())
}

async fn get_feature(Path(feature): Path<String>) -> Result<Json<JsonValue>, ApiError> {
    crate::features::features()
        .as_array()
        .and_then(|features| {
            features
                .iter()
                .find(|f| f["name"] == feature.as_str())
                .cloned()
        })
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("unknown feature: {feature}")))
}

impl From<RollupError> for ApiError {
    fn from(e: RollupError) -> Self {
        match e {
            RollupError::NotFound(_) => ApiError(StatusCode::NOT_FOUND, e.to_string()),
            RollupError::Invalid(_) => bad_request(e.to_string()),
            RollupError::Persist(_) => ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
        }
    }
}

/// Response shape matches the Java `RollUpResource`: the task id plus a
/// resource link.
async fn create_rollup(
    State(state): State<AppState>,
    Json(task): Json<JsonValue>,
) -> Result<Json<JsonValue>, ApiError> {
    let stored = state.rollups.create(task).await?;
    let id = stored["id"].as_str().unwrap_or_default();
    Ok(Json(json!({
        "id": id,
        "name": stored["name"],
        "attributes": {"url": format!("/api/v1/rollups/{id}")},
    })))
}

async fn list_rollups(State(state): State<AppState>) -> Json<JsonValue> {
    Json(JsonValue::Array(state.rollups.list()))
}

async fn get_rollup(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JsonValue>, ApiError> {
    state
        .rollups
        .get(&id)
        .map(Json)
        .ok_or_else(|| ApiError(StatusCode::NOT_FOUND, format!("rollup task not found: {id}")))
}

async fn delete_rollup(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    state.rollups.delete(&id).await?;
    Ok(StatusCode::NO_CONTENT)
}

fn value_pair(point: &DataPoint) -> JsonValue {
    let value = match &point.value {
        Value::Long(v) => json!(v),
        Value::Double(v) => json!(v),
        Value::Text(s) => json!(s.as_ref()),
        Value::Null | Value::Custom(_) => JsonValue::Null,
    };
    json!([point.timestamp_ms, value])
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use kairos_store::memory::MemoryDatastore;
    use tower::ServiceExt;

    async fn memory_router() -> Router {
        memory_router_with_limits(crate::config::LimitsConfig::default()).await
    }

    async fn memory_router_with_limits(limits: crate::config::LimitsConfig) -> Router {
        let store = Arc::new(AnyDatastore::Memory(MemoryDatastore::new()));
        let ingest = Ingest::start(None, store.clone()).await.unwrap();
        let rollups = RollupManager::start(
            store.clone(),
            ingest.clone(),
            None,
            false,
            "test-node".to_string(),
            0,
        )
        .await;
        router(AppState {
            store,
            ingest,
            rollups,
            guard: Arc::new(QueryGuard::new(&limits)),
            fast_math: false,
        })
    }

    /// Ingest is asynchronous; tests must let the consumer drain.
    async fn settle() {
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
    }

    async fn send(app: &Router, method: &str, uri: &str, body: JsonValue) -> (StatusCode, JsonValue) {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method(method)
                    .uri(uri)
                    .header("content-type", "application/json")
                    .body(Body::from(body.to_string()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let bytes = response.into_body().collect().await.unwrap().to_bytes();
        let json = if bytes.is_empty() {
            JsonValue::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, json)
    }

    #[tokio::test]
    async fn ingest_and_query_roundtrip() {
        let app = memory_router().await;

        let (status, _) = send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([{
                "name": "price.settle",
                "tags": {"root": "CL", "contract": "2026-12"},
                "datapoints": [[1000, 62.5], [61000, 63.5], [121000, 64.0]]
            }]),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        settle().await;

        let (status, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints/query",
            json!({
                "start_absolute": 0,
                "end_absolute": 200000,
                "metrics": [{
                    "name": "price.settle",
                    "tags": {"root": "CL"},
                    "aggregators": [{
                        "name": "avg",
                        "sampling": {"value": 2, "unit": "minutes"},
                        "align_sampling": false
                    }]
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let result = &body["queries"][0];
        assert_eq!(result["sample_size"], 3);
        // Buckets [0,120s): avg(62.5, 63.5) = 63.0 and [120s,240s): 64.0
        let values = result["results"][0]["values"].as_array().unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values[0][1], 63.0);
        assert_eq!(values[1][1], 64.0);
        assert_eq!(result["results"][0]["tags"]["root"][0], "CL");
    }

    #[tokio::test]
    async fn group_by_tag_returns_separate_results() {
        let app = memory_router().await;
        send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([
                {"name": "m", "tags": {"host": "a"}, "datapoints": [[1, 1]]},
                {"name": "m", "tags": {"host": "b"}, "datapoints": [[2, 2]]}
            ]),
        )
        .await;
        settle().await;

        let (_, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints/query",
            json!({
                "start_absolute": 0,
                "end_absolute": 10,
                "metrics": [{"name": "m", "group_by": [{"name": "tag", "tags": ["host"]}]}]
            }),
        )
        .await;
        let results = body["queries"][0]["results"].as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["group_by"][1]["group"]["host"], "a");
    }

    #[tokio::test]
    async fn rollup_executes_and_writes_save_as_metric() {
        let app = memory_router().await;
        send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([{"name": "roll.src", "tags": {"host": "a"},
                    "datapoints": [[1, 10.0], [2, 20.0]]}]),
        )
        .await;
        settle().await;

        let (status, body) = send(
            &app,
            "POST",
            "/api/v1/rollups",
            json!({
                "name": "TestRollup",
                "execution_interval": {"value": 1, "unit": "seconds"},
                "rollups": [{
                    "save_as": "roll.dst",
                    "query": {
                        "start_absolute": 0,
                        "metrics": [{
                            "name": "roll.src",
                            "aggregators": [{
                                "name": "sum",
                                "sampling": {"value": 1, "unit": "hours"},
                                "align_sampling": false
                            }]
                        }]
                    }
                }]
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let id = body["id"].as_str().unwrap().to_string();

        // The scheduler skips the immediate tick, so the first execution
        // lands after ~1s; allow a couple of cycles plus ingest settling.
        tokio::time::sleep(std::time::Duration::from_millis(2600)).await;

        let (_, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints/query",
            json!({"start_absolute": 0, "metrics": [{"name": "roll.dst"}]}),
        )
        .await;
        let values = body["queries"][0]["results"][0]["values"]
            .as_array()
            .expect("rolled-up values");
        assert_eq!(values[0][1], 30.0);
        // The source tag carried over to the rolled-up series.
        assert_eq!(body["queries"][0]["results"][0]["tags"]["host"][0], "a");

        // Delete stops the task and 404s afterwards.
        let (status, _) = send(&app, "DELETE", &format!("/api/v1/rollups/{id}"), json!({})).await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        let (status, _) = send(&app, "GET", &format!("/api/v1/rollups/{id}"), json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn tiered_compaction_via_admin_endpoint() {
        let dir = std::env::temp_dir().join(format!("kairos-api-pq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let cold = Arc::new(kairos_store::parquet_store::ParquetStore::open(&dir).unwrap());
        let store = Arc::new(AnyDatastore::TieredMemory(
            kairos_store::tiered::TieredDatastore::new(MemoryDatastore::new(), cold),
        ));
        let ingest = Ingest::start(None, store.clone()).await.unwrap();
        let rollups = RollupManager::start(
            store.clone(),
            ingest.clone(),
            None,
            false,
            "test-node".to_string(),
            0,
        )
        .await;
        let app = router(AppState {
            store,
            ingest,
            rollups,
            guard: Arc::new(QueryGuard::new(&Default::default())),
            fast_math: false,
        });

        send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([{"name": "pq.m", "tags": {"h": "a"},
                    "datapoints": [[1000, 1.5], [2000, 2.5]]}]),
        )
        .await;
        settle().await;

        let (status, body) = send(&app, "POST", "/api/v1/admin/compact", json!({})).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["moved"], 2);

        // Data still queryable, now served from the cold tier.
        let (_, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints/query",
            json!({"start_absolute": 0, "metrics": [{"name": "pq.m",
                "aggregators": [{"name": "sum", "sampling": {"value": 1, "unit": "hours"},
                                 "align_sampling": false}]}]}),
        )
        .await;
        assert_eq!(body["queries"][0]["results"][0]["values"][0][1], 4.0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn query_point_budget_enforced() {
        let app = memory_router_with_limits(crate::config::LimitsConfig {
            max_query_points: 2,
            ..Default::default()
        })
        .await;
        send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([{"name": "budget.m", "tags": {"h": "a"},
                    "datapoints": [[1, 1.0], [2, 2.0], [3, 3.0]]}]),
        )
        .await;
        settle().await;
        let (status, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints/query",
            json!({"start_absolute": 0, "metrics": [{"name": "budget.m"}]}),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
        assert!(body["errors"][0].as_str().unwrap().contains("max_query_points"));
    }

    #[tokio::test]
    async fn running_queries_and_kill_unknown() {
        let app = memory_router().await;
        let (status, body) = send(&app, "GET", "/api/v1/runningqueries", json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.as_array().unwrap().is_empty());
        let (status, _) = send(&app, "DELETE", "/api/v1/killquery/42", json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rejects_empty_metric_name() {
        let app = memory_router().await;
        let (status, body) = send(
            &app,
            "POST",
            "/api/v1/datapoints",
            json!([{"name": "", "datapoints": [[1, 1]]}]),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body["errors"][0].as_str().unwrap().contains("name"));
    }
}
