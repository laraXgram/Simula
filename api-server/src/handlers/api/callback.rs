use actix_web::web::Data;
use chrono::Utc;
use reqwest::Url;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::AnswerCallbackQueryRequest;

use crate::handlers::utils::updates::value_to_optional_bool_loose;

use crate::handlers::parse_request;

const CALLBACK_QUERY_ANSWER_TEXT_MAX_LEN: usize = 200;

pub fn handle_answer_callback_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let mut normalized = params.clone();
    if let Some(raw) = params.get("show_alert") {
        if let Some(loose) = value_to_optional_bool_loose(raw) {
            normalized.insert("show_alert".to_string(), Value::Bool(loose));
        }
    }

    let request: AnswerCallbackQueryRequest = parse_request(&normalized)?;
    if request.callback_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("callback_query_id is empty"));
    }

    if let Some(text) = request.text.as_deref() {
        if text.chars().count() > CALLBACK_QUERY_ANSWER_TEXT_MAX_LEN {
            return Err(ApiError::bad_request("text is too long"));
        }
    }

    if let Some(cache_time) = request.cache_time {
        if cache_time < 0 {
            return Err(ApiError::bad_request("cache_time must be non-negative"));
        }
    }

    if let Some(url) = request.url.as_deref() {
        let trimmed = url.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("url is empty"));
        }

        let parsed = Url::parse(trimmed)
            .map_err(|_| ApiError::bad_request("url is invalid"))?;
        let scheme = parsed.scheme();
        if scheme != "http" && scheme != "https" && scheme != "tg" {
            return Err(ApiError::bad_request("url protocol is unsupported"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let callback_row: Option<String> = conn
        .query_row(
            "SELECT id FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.callback_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if callback_row.is_none() {
        return Err(ApiError::not_found("callback query not found"));
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE callback_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.callback_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}
