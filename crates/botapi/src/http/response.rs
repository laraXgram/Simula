//! Telegram's response envelopes. The HTTP status equals `error_code`.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{Value, json};

use crate::error::BotError;

pub fn ok(result: Value) -> Response {
    Json(json!({ "ok": true, "result": result })).into_response()
}

pub fn error(error: &BotError) -> Response {
    let mut body =
        json!({ "ok": false, "error_code": error.code, "description": error.description });
    let mut parameters = serde_json::Map::new();
    if let Some(retry_after) = error.retry_after {
        parameters.insert("retry_after".into(), retry_after.into());
    }
    if let Some(chat_id) = error.migrate_to_chat_id {
        parameters.insert("migrate_to_chat_id".into(), chat_id.into());
    }
    if !parameters.is_empty() {
        body["parameters"] = Value::Object(parameters);
    }
    let status = StatusCode::from_u16(error.code).unwrap_or(StatusCode::BAD_REQUEST);
    (status, Json(body)).into_response()
}
