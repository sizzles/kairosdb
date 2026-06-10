//! `/api/v1` handlers, wire-compatible with the Java `MetricsResource`.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{Json, Router};
use kairos_core::{DataPoint, DataPointSet, Value};
use kairos_query::model::{GroupResult, MetricQuery, QueryRequest, SeriesInput};
use kairos_store::{Datastore, DatastoreQuery};
use serde_json::{json, Value as JsonValue};

use crate::ingest::Ingest;
use crate::rollup::{RollupError, RollupManager};
use crate::store::AnyDatastore;

#[derive(Clone)]
pub struct AppState {
    pub store: Arc<AnyDatastore>,
    pub ingest: Ingest,
    pub rollups: Arc<RollupManager>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/api/v1/version", get(version))
        .route("/api/v1/metricnames", get(metric_names))
        .route("/api/v1/datapoints", post(add_datapoints))
        .route("/api/v1/datapoints/query", post(query_datapoints))
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

async fn version() -> Json<JsonValue> {
    Json(json!({ "version": concat!("KairosDB-rs ", env!("CARGO_PKG_VERSION")) }))
}

async fn metric_names(State(state): State<AppState>) -> Result<Json<JsonValue>, ApiError> {
    let names = state
        .store
        .metric_names(None)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(json!({ "results": names })))
}

/// Ingest format: an array of metric objects (a single object is also
/// accepted), each carrying `tags` plus either `datapoints: [[ts, value]]`
/// or a single `timestamp`/`value` pair. Sets are acknowledged once durable
/// in the WAL and queued, not once stored.
async fn add_datapoints(
    State(state): State<AppState>,
    Json(body): Json<JsonValue>,
) -> Result<StatusCode, ApiError> {
    let metrics: Vec<&JsonValue> = match &body {
        JsonValue::Array(items) => items.iter().collect(),
        JsonValue::Object(_) => vec![&body],
        _ => return Err(bad_request("metric[0].name may not be empty")),
    };

    for metric in metrics {
        let set = parse_metric(metric)?;
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
/// between the query endpoint and the rollup executor.
pub(crate) async fn run_metric_query(
    store: &AnyDatastore,
    metric: &MetricQuery,
    start_ms: i64,
    end_ms: i64,
) -> Result<(usize, Vec<GroupResult>), String> {
    let series = store
        .query(&DatastoreQuery {
            metric: metric.name.clone(),
            start_time_ms: start_ms,
            end_time_ms: end_ms,
            tags: metric.tag_filter(),
            limit: metric.limit,
        })
        .await
        .map_err(|e| e.to_string())?;

    let sample_size: usize = series.iter().map(|s| s.points.len()).sum();
    let inputs: Vec<SeriesInput> = series
        .into_iter()
        .map(|s| SeriesInput { tags: s.tags, points: s.points })
        .collect();

    let groups =
        kairos_query::model::execute(metric, inputs, start_ms).map_err(|e| e.to_string())?;
    Ok((sample_size, groups))
}

async fn query_datapoints(
    State(state): State<AppState>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (start_ms, end_ms) = request
        .resolve_time_range(now_ms)
        .map_err(|e| bad_request(e.to_string()))?;

    let mut queries = Vec::new();
    for metric in &request.metrics {
        let (sample_size, groups) = run_metric_query(&state.store, metric, start_ms, end_ms)
            .await
            .map_err(bad_request)?;

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

        queries.push(json!({ "sample_size": sample_size, "results": results }));
    }

    Ok(Json(json!({ "queries": queries })))
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
    let stored = state.rollups.create(task)?;
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
    state.rollups.delete(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

fn value_pair(point: &DataPoint) -> JsonValue {
    let value = match &point.value {
        Value::Long(v) => json!(v),
        Value::Double(v) => json!(v),
        Value::Text(s) => json!(s.as_ref()),
        Value::Custom { .. } => JsonValue::Null,
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
        let store = Arc::new(AnyDatastore::Memory(MemoryDatastore::new()));
        let ingest = Ingest::start(None, store.clone()).await.unwrap();
        let rollups = RollupManager::start(store.clone(), ingest.clone(), None);
        router(AppState { store, ingest, rollups })
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
