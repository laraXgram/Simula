use actix_web::web::Data;
use chrono::Utc;
use rusqlite::params;
use serde_json::Value;
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    AnswerWebAppQueryRequest, SavePreparedInlineMessageRequest, SavePreparedKeyboardButtonRequest,
};

use crate::generated::types::{
    InlineQueryResult, KeyboardButton, PreparedInlineMessage, PreparedKeyboardButton,
    SentWebAppMessage,
};

use crate::handlers::client::{users, webapp};

use crate::handlers::{parse_request, generate_telegram_numeric_id};

const INLINE_QUERY_RESULT_TYPES: &[&str] = &[
    "cached_audio",
    "cached_document",
    "cached_gif",
    "cached_mpeg4_gif",
    "cached_photo",
    "cached_sticker",
    "cached_video",
    "cached_voice",
    "article",
    "audio",
    "contact",
    "game",
    "document",
    "gif",
    "location",
    "mpeg4_gif",
    "photo",
    "venue",
    "video",
    "voice",
];

fn ensure_int32_range(value: i64, field_name: &str) -> Result<(), ApiError> {
    if value < i32::MIN as i64 || value > i32::MAX as i64 {
        return Err(ApiError::bad_request(format!(
            "{} must fit in signed 32-bit range",
            field_name,
        )));
    }

    Ok(())
}

fn inline_result_has_inline_keyboard(result: &InlineQueryResult) -> bool {
    result
        .extra
        .get("reply_markup")
        .and_then(Value::as_object)
        .and_then(|reply_markup| reply_markup.get("inline_keyboard"))
        .and_then(Value::as_array)
        .map(|rows| !rows.is_empty())
        .unwrap_or(false)
}

fn validate_inline_query_result(result: &InlineQueryResult) -> Result<(), ApiError> {
    let object = result
        .extra
        .as_object()
        .ok_or_else(|| ApiError::bad_request("result must be a JSON object"))?;

    let result_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if result_type.is_empty() {
        return Err(ApiError::bad_request("result.type is required"));
    }
    if !INLINE_QUERY_RESULT_TYPES
        .iter()
        .any(|candidate| *candidate == result_type)
    {
        return Err(ApiError::bad_request("result.type is invalid"));
    }

    let result_id = object
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");
    if result_id.is_empty() {
        return Err(ApiError::bad_request("result.id is required"));
    }
    if result_id.as_bytes().len() > 64 {
        return Err(ApiError::bad_request("result.id must be 1-64 bytes"));
    }

    Ok(())
}

fn validate_prepared_keyboard_button(button: &KeyboardButton) -> Result<(), ApiError> {
    if button.web_app.is_some()
        || button.request_contact.unwrap_or(false)
        || button.request_location.unwrap_or(false)
        || button.request_poll.is_some()
    {
        return Err(ApiError::bad_request(
            "button must be of type request_users, request_chat, or request_managed_bot",
        ));
    }

    let mut selected_button_types = 0;

    if let Some(request_users) = button.request_users.as_ref() {
        selected_button_types += 1;
        ensure_int32_range(request_users.request_id, "button.request_users.request_id")?;
        if let Some(max_quantity) = request_users.max_quantity {
            if !(1..=10).contains(&max_quantity) {
                return Err(ApiError::bad_request(
                    "button.request_users.max_quantity must be between 1 and 10",
                ));
            }
        }
    }

    if let Some(request_chat) = button.request_chat.as_ref() {
        selected_button_types += 1;
        ensure_int32_range(request_chat.request_id, "button.request_chat.request_id")?;
    }

    if let Some(request_managed_bot) = button.request_managed_bot.as_ref() {
        selected_button_types += 1;
        ensure_int32_range(
            request_managed_bot.request_id,
            "button.request_managed_bot.request_id",
        )?;
    }

    if selected_button_types != 1 {
        return Err(ApiError::bad_request(
            "button must contain exactly one of request_users, request_chat, or request_managed_bot",
        ));
    }

    Ok(())
}

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
    validate_inline_query_result(&request.result)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    webapp::ensure_sim_web_app_query_answers_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let inline_message_id = if inline_result_has_inline_keyboard(&request.result) {
        Some(generate_telegram_numeric_id())
    } else {
        None
    };
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
            inline_message_id.as_deref(),
            result_json,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    serde_json::to_value(SentWebAppMessage {
        inline_message_id,
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
    validate_inline_query_result(&request.result)?;

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
    let _ = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    webapp::ensure_sim_prepared_inline_messages_storage(&mut conn)?;

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
    validate_prepared_keyboard_button(&request.button)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    webapp::ensure_sim_prepared_keyboard_buttons_storage(&mut conn)?;

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
