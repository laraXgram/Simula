use actix_web::web::Data;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

pub fn handle_sim_get_callback_query_answer(
    state: &Data<AppState>,
    token: &str,
    callback_query_id: &str,
) -> ApiResult {
    if callback_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("callback_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![callback_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("callback query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "callback_query_id": callback_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

