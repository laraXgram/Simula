use actix_web::HttpResponse;
use serde_json::{json, Value};

pub type ApiResult = Result<Value, ApiError>;

#[derive(Debug)]
pub struct ApiError {
    pub code: u16,
    pub description: String,
}

impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.description)
    }
}

impl std::error::Error for ApiError {}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self {
            code: 400,
            description: format!("Bad Request: {}", msg.into()),
        }
    }

    pub fn not_found(msg: impl Into<String>) -> Self {
        Self {
            code: 404,
            description: format!("Not Found: {}", msg.into()),
        }
    }

    pub fn conflict(msg: impl Into<String>) -> Self {
        Self {
            code: 409,
            description: format!("Conflict: {}", msg.into()),
        }
    }

    pub fn internal(err: impl std::fmt::Display) -> Self {
        Self {
            code: 500,
            description: format!("Internal Server Error: {err}"),
        }
    }
}

pub fn into_telegram_response(result: ApiResult) -> HttpResponse {
    match result {
        Ok(value) => HttpResponse::Ok().json(json!({
            "ok": true,
            "result": strip_nulls(value),
        })),
        Err(err) => HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": err.code,
            "description": err.description,
        })),
    }
}

pub fn strip_nulls(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut clean = serde_json::Map::new();
            for (key, val) in map {
                if !val.is_null() {
                    clean.insert(key, strip_nulls(val));
                }
            }
            Value::Object(clean)
        }
        Value::Array(values) => Value::Array(values.into_iter().map(strip_nulls).collect()),
        other => other,
    }
}
