//! `/api/v1` handlers, wire-compatible with the Java `MetricsResource`.

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use kairos_core::{DataPoint, DataPointSet, Value};
use kairos_query::model::{QueryRequest, SeriesInput};
use kairos_store::{Datastore, DatastoreQuery};
use serde_json::{json, Value as JsonValue};

use crate::store::AnyDatastore;

type Store = Arc<AnyDatastore>;

pub fn router(store: Store) -> Router {
    Router::new()
        .route("/api/v1/version", get(version))
        .route("/api/v1/metricnames", get(metric_names))
        .route("/api/v1/datapoints", post(add_datapoints))
        .route("/api/v1/datapoints/query", post(query_datapoints))
        .with_state(store)
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

async fn metric_names(State(store): State<Store>) -> Result<Json<JsonValue>, ApiError> {
    let names = store
        .metric_names(None)
        .await
        .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(json!({ "results": names })))
}

/// Ingest format: an array of metric objects (a single object is also
/// accepted), each carrying `tags` plus either `datapoints: [[ts, value]]`
/// or a single `timestamp`/`value` pair.
async fn add_datapoints(
    State(store): State<Store>,
    Json(body): Json<JsonValue>,
) -> Result<StatusCode, ApiError> {
    let metrics: Vec<&JsonValue> = match &body {
        JsonValue::Array(items) => items.iter().collect(),
        JsonValue::Object(_) => vec![&body],
        _ => return Err(bad_request("metric[0].name may not be empty")),
    };

    for metric in metrics {
        let set = parse_metric(metric)?;
        store
            .write(set)
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

async fn query_datapoints(
    State(store): State<Store>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<JsonValue>, ApiError> {
    let now_ms = chrono::Utc::now().timestamp_millis();
    let (start_ms, end_ms) = request
        .resolve_time_range(now_ms)
        .map_err(|e| bad_request(e.to_string()))?;

    let mut queries = Vec::new();
    for metric in &request.metrics {
        let series = store
            .query(&DatastoreQuery {
                metric: metric.name.clone(),
                start_time_ms: start_ms,
                end_time_ms: end_ms,
                tags: metric.tag_filter(),
                limit: metric.limit,
            })
            .await
            .map_err(|e| ApiError(StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let sample_size: usize = series.iter().map(|s| s.points.len()).sum();
        let inputs: Vec<SeriesInput> = series
            .into_iter()
            .map(|s| SeriesInput { tags: s.tags, points: s.points })
            .collect();

        let groups = kairos_query::model::execute(metric, inputs, start_ms)
            .map_err(|e| bad_request(e.to_string()))?;

        let grouped_by_tag = metric.group_by.iter().any(|g| g.name == "tag");
        let results: Vec<JsonValue> = groups
            .into_iter()
            .map(|g| {
                let mut group_by = vec![json!({"name": "type", "type": "number"})];
                if grouped_by_tag {
                    let tag_names: Vec<&String> = g.group.keys().collect();
                    group_by.push(json!({
                        "name": "tag",
                        "tags": tag_names,
                        "group": g.group,
                    }));
                }
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

    fn memory_router() -> Router {
        router(Arc::new(AnyDatastore::Memory(MemoryDatastore::new())))
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
        let app = memory_router();

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
        let app = memory_router();
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
    async fn rejects_empty_metric_name() {
        let app = memory_router();
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
