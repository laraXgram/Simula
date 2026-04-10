use super::*;
use crate::generated::methods::AnswerCallbackQueryRequest;

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
