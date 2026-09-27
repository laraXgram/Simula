//! Request → parameters. Query strings, urlencoded and multipart bodies carry every value
//! as a string; they are coerced by the method's documented parameter kinds so the typed
//! structs can deserialize them. JSON bodies may still carry JSON-serialized strings.

use std::collections::HashMap;

use axum::extract::{FromRequest, Multipart, Request};
use axum::http::header::CONTENT_TYPE;
use serde_json::{Map, Number, Value};
use simula_botapi_types::{MethodInfo, ParamKind};

use crate::error::{BotError, BotResult};

/// A file part of a multipart request.
#[derive(Debug, Clone)]
#[expect(dead_code, reason = "read by the InputFile pipeline (P3.5)")]
pub struct Upload {
    pub file_name: Option<String>,
    pub content_type: Option<String>,
    pub bytes: axum::body::Bytes,
}

#[derive(Debug, Default)]
pub struct DecodedRequest {
    pub params: Map<String, Value>,
    /// File parts by field name, for `attach://<name>` references.
    pub files: HashMap<String, Upload>,
}

pub async fn decode(request: Request, info: Option<&MethodInfo>) -> BotResult<DecodedRequest> {
    let mut decoded = DecodedRequest::default();
    if let Some(query) = request.uri().query() {
        for (key, value) in form_pairs(query)? {
            decoded
                .params
                .insert(key.clone(), coerce(info, &key, value));
        }
    }

    let content_type = request
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| {
            v.split(';')
                .next()
                .unwrap_or("")
                .trim()
                .to_ascii_lowercase()
        })
        .unwrap_or_default();

    match content_type.as_str() {
        "application/json" => {
            let body = body_bytes(request).await?;
            if body.is_empty() {
                return Ok(decoded);
            }
            let Value::Object(object) = serde_json::from_slice(&body)
                .map_err(|_| BotError::bad_request("can't parse JSON encoded request"))?
            else {
                return Err(BotError::bad_request("can't parse JSON encoded request"));
            };
            for (key, value) in object {
                let value = match value {
                    Value::String(s) => coerce(info, &key, s),
                    other => other,
                };
                decoded.params.insert(key, value);
            }
        }
        "application/x-www-form-urlencoded" => {
            let body = body_bytes(request).await?;
            for (key, value) in form_pairs(std::str::from_utf8(&body).unwrap_or(""))? {
                decoded
                    .params
                    .insert(key.clone(), coerce(info, &key, value));
            }
        }
        "multipart/form-data" => {
            let mut multipart = Multipart::from_request(request, &())
                .await
                .map_err(|_| BotError::bad_request("can't parse multipart/form-data request"))?;
            while let Some(field) = multipart
                .next_field()
                .await
                .map_err(|_| BotError::bad_request("can't parse multipart/form-data request"))?
            {
                let name = field.name().unwrap_or_default().to_owned();
                let file_name = field.file_name().map(str::to_owned);
                let content_type = field.content_type().map(str::to_owned);
                let bytes = field
                    .bytes()
                    .await
                    .map_err(|_| BotError::bad_request("request body is too big"))?;
                if file_name.is_some() {
                    // A file sent directly as a parameter is an `attach://` to itself.
                    if kind(info, &name) == Some(ParamKind::InputFile) {
                        decoded
                            .params
                            .insert(name.clone(), Value::String(format!("attach://{name}")));
                    }
                    decoded.files.insert(
                        name,
                        Upload {
                            file_name,
                            content_type,
                            bytes,
                        },
                    );
                } else {
                    let text = String::from_utf8_lossy(&bytes).into_owned();
                    decoded
                        .params
                        .insert(name.clone(), coerce(info, &name, text));
                }
            }
        }
        _ => {}
    }
    Ok(decoded)
}

async fn body_bytes(request: Request) -> BotResult<axum::body::Bytes> {
    axum::body::to_bytes(request.into_body(), 64 * 1024 * 1024)
        .await
        .map_err(|_| BotError::bad_request("request body is too big"))
}

fn form_pairs(input: &str) -> BotResult<Vec<(String, String)>> {
    serde_urlencoded::from_str(input)
        .map_err(|_| BotError::bad_request("can't parse request parameters"))
}

fn kind(info: Option<&MethodInfo>, name: &str) -> Option<ParamKind> {
    info?.params.iter().find(|p| p.name == name).map(|p| p.kind)
}

/// Turn a string value into the JSON type its parameter expects. Values that don't parse
/// stay strings, so deserialization reports them precisely.
fn coerce(info: Option<&MethodInfo>, name: &str, value: String) -> Value {
    match kind(info, name) {
        Some(ParamKind::Integer) => value
            .trim()
            .parse::<i64>()
            .map(Value::from)
            .unwrap_or(Value::String(value)),
        Some(ParamKind::Float) => value
            .trim()
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map(Value::Number)
            .unwrap_or(Value::String(value)),
        Some(ParamKind::Boolean) => match value.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Value::Bool(true),
            "false" | "0" | "" => Value::Bool(false),
            _ => Value::String(value),
        },
        Some(ParamKind::Json) => serde_json::from_str(&value).unwrap_or(Value::String(value)),
        _ => Value::String(value),
    }
}
