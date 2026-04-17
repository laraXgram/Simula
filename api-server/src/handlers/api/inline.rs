use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::AnswerInlineQueryRequest;

use crate::handlers::parse_request;

const INLINE_QUERY_MAX_RESULTS: usize = 50;
const INLINE_QUERY_MAX_NEXT_OFFSET_BYTES: usize = 64;
const INLINE_QUERY_RESULT_ID_MAX_BYTES: usize = 64;
const INLINE_QUERY_BUTTON_START_PARAMETER_MAX_LEN: usize = 64;

fn validate_inline_query_results(
    results: &[crate::generated::types::InlineQueryResult],
) -> Result<(), ApiError> {
    if results.len() > INLINE_QUERY_MAX_RESULTS {
        return Err(ApiError::bad_request("results must contain at most 50 items"));
    }

    let mut seen_ids: HashSet<String> = HashSet::new();
    for result in results {
        let Some(result_obj) = result.extra.as_object() else {
            return Err(ApiError::bad_request("inline query result must be an object"));
        };

        let result_type = result_obj
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| ApiError::bad_request("inline query result type is required"))?;

        let result_id = result_obj
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("inline query result id is required"))?;

        let id_len = result_id.as_bytes().len();
        if id_len == 0 || id_len > INLINE_QUERY_RESULT_ID_MAX_BYTES {
            return Err(ApiError::bad_request(
                "inline query result id must be 1-64 bytes",
            ));
        }

        if !seen_ids.insert(result_id.to_string()) {
            return Err(ApiError::bad_request("inline query result ids must be unique"));
        }

        if result_type.eq_ignore_ascii_case("game") {
            let has_game_short_name = result_obj
                .get("game_short_name")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
            if !has_game_short_name {
                return Err(ApiError::bad_request(
                    "inline game result must include game_short_name",
                ));
            }
        }
    }

    Ok(())
}

fn validate_inline_query_button(
    button: &crate::generated::types::InlineQueryResultsButton,
) -> Result<(), ApiError> {
    if button.text.trim().is_empty() {
        return Err(ApiError::bad_request("button.text is empty"));
    }

    let has_web_app = button
        .web_app
        .as_ref()
        .map(|web_app| !web_app.url.trim().is_empty())
        .unwrap_or(false);

    let start_parameter = button.start_parameter.as_deref().map(str::trim);
    let has_start_parameter = start_parameter
        .map(|value| !value.is_empty())
        .unwrap_or(false);

    if has_web_app == has_start_parameter {
        return Err(ApiError::bad_request(
            "exactly one of button.web_app or button.start_parameter must be specified",
        ));
    }

    if let Some(parameter) = start_parameter {
        if !parameter.is_empty() {
            if parameter.len() > INLINE_QUERY_BUTTON_START_PARAMETER_MAX_LEN {
                return Err(ApiError::bad_request(
                    "button.start_parameter must be 1-64 characters",
                ));
            }

            if !parameter
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == '-')
            {
                return Err(ApiError::bad_request(
                    "button.start_parameter contains invalid characters",
                ));
            }
        }
    }

    Ok(())
}

pub fn handle_answer_inline_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerInlineQueryRequest = parse_request(params)?;

    let inline_query_id = request.inline_query_id.trim();
    if inline_query_id.is_empty() {
        return Err(ApiError::bad_request("inline_query_id is empty"));
    }

    validate_inline_query_results(&request.results)?;

    if let Some(next_offset) = request.next_offset.as_deref() {
        if next_offset.as_bytes().len() > INLINE_QUERY_MAX_NEXT_OFFSET_BYTES {
            return Err(ApiError::bad_request(
                "next_offset must not exceed 64 bytes",
            ));
        }
    }

    let cache_time = request.cache_time.unwrap_or(300);
    if cache_time < 0 {
        return Err(ApiError::bad_request("cache_time must be non-negative"));
    }

    if let Some(button) = request.button.as_ref() {
        validate_inline_query_button(button)?;
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT query, from_user_id FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((query_text, from_user_id)) = query_row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE inline_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), inline_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let is_personal = request.is_personal.unwrap_or(false);
    let query_offset: String = conn
        .query_row(
            "SELECT offset FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![inline_query_id, bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    if cache_time > 0 {
        let expires_at = now + cache_time;
        let cache_user_id = if is_personal { from_user_id } else { -1 };
        conn.execute(
            "INSERT INTO inline_query_cache
             (bot_id, query, offset, from_user_id, answer_json, cache_time, expires_at, is_personal, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(bot_id, query, offset, from_user_id)
             DO UPDATE SET answer_json = excluded.answer_json,
                           cache_time = excluded.cache_time,
                           expires_at = excluded.expires_at,
                           is_personal = excluded.is_personal,
                           created_at = excluded.created_at",
            params![
                bot.id,
                query_text,
                query_offset,
                cache_user_id,
                answer_payload.to_string(),
                cache_time,
                expires_at,
                if is_personal { 1 } else { 0 },
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}
