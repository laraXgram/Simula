use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::AnswerInlineQueryRequest;

use crate::handlers::parse_request;

pub fn handle_answer_inline_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerInlineQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT query, from_user_id FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
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
        params![now, answer_payload.to_string(), request.inline_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let cache_time = request.cache_time.unwrap_or(300).max(0);
    let is_personal = request.is_personal.unwrap_or(false);
    let query_offset: String = conn
        .query_row(
            "SELECT offset FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
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
