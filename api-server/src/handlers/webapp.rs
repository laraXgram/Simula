use super::*;
use crate::generated::methods::{
    AnswerWebAppQueryRequest, SavePreparedInlineMessageRequest, SavePreparedKeyboardButtonRequest,
};

pub fn handle_answer_web_app_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerWebAppQueryRequest = parse_request(params)?;
    let web_app_query_id = request.web_app_query_id.trim();
    if web_app_query_id.is_empty() {
        return Err(ApiError::bad_request("web_app_query_id is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_web_app_query_answers_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let inline_message_id = generate_telegram_numeric_id();
    let result_json = serde_json::to_string(&request.result).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_web_app_query_answers
         (bot_id, web_app_query_id, inline_message_id, result_json, answered_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, web_app_query_id)
         DO UPDATE SET
            inline_message_id = excluded.inline_message_id,
            result_json = excluded.result_json,
            answered_at = excluded.answered_at",
        params![
            bot.id,
            web_app_query_id,
            inline_message_id,
            result_json,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(SentWebAppMessage {
        inline_message_id: Some(inline_message_id),
    })
    .map_err(ApiError::internal)
}

pub fn handle_save_prepared_inline_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SavePreparedInlineMessageRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let allow_user_chats = request.allow_user_chats.unwrap_or(true);
    let allow_bot_chats = request.allow_bot_chats.unwrap_or(true);
    let allow_group_chats = request.allow_group_chats.unwrap_or(true);
    let allow_channel_chats = request.allow_channel_chats.unwrap_or(true);

    if !(allow_user_chats || allow_bot_chats || allow_group_chats || allow_channel_chats) {
        return Err(ApiError::bad_request(
            "at least one allow_* chat target must be true",
        ));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_prepared_inline_messages_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let prepared_id = generate_telegram_numeric_id();
    let expiration_date = now + (24 * 60 * 60);

    conn.execute(
        "INSERT INTO sim_prepared_inline_messages
         (bot_id, id, user_id, result_json,
          allow_user_chats, allow_bot_chats, allow_group_chats, allow_channel_chats,
          expiration_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?10)",
        params![
            bot.id,
            &prepared_id,
            request.user_id,
            serde_json::to_string(&request.result).map_err(ApiError::internal)?,
            if allow_user_chats { 1 } else { 0 },
            if allow_bot_chats { 1 } else { 0 },
            if allow_group_chats { 1 } else { 0 },
            if allow_channel_chats { 1 } else { 0 },
            expiration_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(PreparedInlineMessage {
        id: prepared_id,
        expiration_date,
    })
    .map_err(ApiError::internal)
}

pub fn handle_save_prepared_keyboard_button(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SavePreparedKeyboardButtonRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.button.text.trim().is_empty() {
        return Err(ApiError::bad_request("button.text is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_prepared_keyboard_buttons_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let prepared_id = generate_telegram_numeric_id();

    conn.execute(
        "INSERT INTO sim_prepared_keyboard_buttons
         (bot_id, id, user_id, button_json, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5)",
        params![
            bot.id,
            &prepared_id,
            request.user_id,
            serde_json::to_string(&request.button).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(PreparedKeyboardButton { id: prepared_id }).map_err(ApiError::internal)
}
