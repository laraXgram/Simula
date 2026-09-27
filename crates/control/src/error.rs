//! Control API errors carry the core's MTProto error unchanged.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::json;
use simula_core::RpcError;

#[derive(Debug)]
pub struct ControlError(pub RpcError);

impl From<RpcError> for ControlError {
    fn from(error: RpcError) -> Self {
        Self(error)
    }
}

impl IntoResponse for ControlError {
    fn into_response(self) -> Response {
        let status = u16::try_from(self.0.code)
            .ok()
            .and_then(|code| StatusCode::from_u16(code).ok())
            .unwrap_or(StatusCode::BAD_REQUEST);
        let body = json!({ "ok": false, "error": { "code": self.0.code, "message": self.0.message } });
        (status, Json(body)).into_response()
    }
}

pub type ControlResult<T> = Result<T, ControlError>;
