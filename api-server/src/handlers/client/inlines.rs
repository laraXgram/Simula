use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{strip_nulls, ApiError, ApiResult};

pub fn handle_sim_choose_inline_result(
    state: &Data<AppState>,
    token: &str,
    body: SimChooseInlineResultRequest,
) -> ApiResult {
    if body.inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }
    if body.result_id.trim().is_empty() {
        return Err(ApiError::bad_request("result_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, i64, String, Option<String>)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, query, answer_json FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![body.inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, query_text, answer_json)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let answer_value: Value = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .ok_or_else(|| ApiError::bad_request("inline query has no answer yet"))?;

    let results = answer_value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("inline query answer has no results"))?;

    let selected = results
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(body.result_id.as_str()))
        .or_else(|| results.first())
        .ok_or_else(|| ApiError::bad_request("inline query answer has empty results"))?;

    let message_text = selected
        .get("input_message_content")
        .and_then(|c| c.get("message_text"))
        .and_then(Value::as_str)
        .map(|v| v.to_string())
        .or_else(|| selected.get("title").and_then(Value::as_str).map(|v| v.to_string()))
        .or_else(|| selected.get("description").and_then(Value::as_str).map(|v| v.to_string()))
        .unwrap_or_else(|| "inline result".to_string());

    ensure_chat(&mut conn, &chat_key)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, from_user_id, message_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(&chat_key));

    let user_info: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT first_name, username FROM users WHERE id = ?1",
            params![from_user_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let (first_name, username) = user_info.unwrap_or_else(|| ("User".to_string(), None));

    let message_payload = json!({
        "message_id": message_id,
        "date": now,
        "chat": {
            "id": chat_id,
            "type": "private"
        },
        "from": {
            "id": from_user_id,
            "is_bot": false,
            "first_name": first_name,
            "username": username
        },
        "text": message_text,
        "via_bot": {
            "id": bot.id,
            "is_bot": true,
            "first_name": bot.first_name,
            "username": bot.username
        }
    });
    let message_for_update: Message = serde_json::from_value(message_payload).map_err(ApiError::internal)?;
    let message_update = serde_json::to_value(Update {
        update_id: 0,
        message: Some(message_for_update),
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, message_update)?;

    let chosen_from = User {
        id: from_user_id,
        is_bot: false,
        first_name: first_name.clone(),
        last_name: None,
        username: username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };
    let inline_message_id = generate_telegram_numeric_id();
    conn.execute(
        "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![inline_message_id, bot.id, chat_key, message_id, now],
    )
    .map_err(ApiError::internal)?;

    let chosen_inline_result_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: Some(ChosenInlineResult {
            result_id: body.result_id.clone(),
            from: chosen_from,
            location: None,
            inline_message_id: Some(inline_message_id),
            query: query_text,
        }),
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, chosen_inline_result_update)?;

    Ok(json!({
        "message_id": message_id,
        "result_id": body.result_id,
    }))
}

pub fn handle_sim_press_inline_button(
    state: &Data<AppState>,
    token: &str,
    body: SimPressInlineButtonRequest,
) -> ApiResult {
    if body.callback_data.trim().is_empty() {
        return Err(ApiError::bad_request("callback_data is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let mut message_value = load_message_value(&mut conn, &bot, body.message_id)?;
    enrich_message_with_linked_channel_context(
        &mut conn,
        bot.id,
        &chat_key,
        body.message_id,
        &mut message_value,
    )?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let callback_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();

    let callback_from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    let is_inline_origin = message_value
        .get("via_bot")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_i64)
        == Some(bot.id);

    let inline_message_id = if is_inline_origin {
        let existing: Option<String> = conn
            .query_row(
                "SELECT inline_message_id FROM inline_messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot.id, chat_key, body.message_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(existing_id) = existing {
            Some(existing_id)
        } else {
            let generated = generate_telegram_numeric_id();
            conn.execute(
                "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![generated, bot.id, chat_key, body.message_id, now],
            )
            .map_err(ApiError::internal)?;
            Some(generated)
        }
    } else {
        None
    };

    let callback_message: Option<MaybeInaccessibleMessage> = if inline_message_id.is_some() {
        None
    } else {
        Some(serde_json::from_value(message_value).map_err(ApiError::internal)?)
    };

    let callback_query = CallbackQuery {
        id: callback_query_id.clone(),
        from: callback_from,
        message: callback_message,
        inline_message_id,
        chat_instance: generate_telegram_numeric_id(),
        data: Some(body.callback_data.clone()),
        game_short_name: None,
    };

    conn.execute(
        "INSERT INTO callback_queries (id, bot_id, chat_key, message_id, from_user_id, data, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![callback_query_id, bot.id, chat_key, body.message_id, user.id, body.callback_data, now],
    )
    .map_err(ApiError::internal)?;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: None,
        chosen_inline_result: None,
        callback_query: Some(callback_query),
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "ok": true,
        "callback_query_id": callback_query_id,
    }))
}

pub fn handle_sim_send_inline_query(
    state: &Data<AppState>,
    token: &str,
    body: SimSendInlineQueryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let chat_key = chat_id.to_string();
    ensure_chat(&mut conn, &chat_key)?;

    let inline_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();
    let query_text = body.query;
    let offset = body.offset.unwrap_or_default();

    let cached_answer_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT answer_json, expires_at
             FROM inline_query_cache
             WHERE bot_id = ?1 AND query = ?2 AND offset = ?3
                             AND (from_user_id = -1 OR from_user_id = ?4)
                         ORDER BY CASE WHEN from_user_id = ?4 THEN 0 ELSE 1 END
             LIMIT 1",
            params![bot.id, query_text, offset, user.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some((cached_answer_json, expires_at)) = cached_answer_row {
        if expires_at >= now {
            conn.execute(
                "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at, answered_at, answer_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    inline_query_id,
                    bot.id,
                    chat_key,
                    user.id,
                    query_text,
                    offset,
                    now,
                    now,
                    cached_answer_json,
                ],
            )
            .map_err(ApiError::internal)?;

            return Ok(json!({
                "inline_query_id": inline_query_id,
                "cached": true,
            }));
        }
    }

    let inline_from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    };

    let inline_query = InlineQuery {
        id: inline_query_id.clone(),
        from: inline_from,
        query: query_text.clone(),
        offset: offset.clone(),
        chat_type: Some("private".to_string()),
        location: None,
    };

    conn.execute(
        "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            inline_query_id,
            bot.id,
            chat_key,
            user.id,
            query_text,
            offset,
            now
        ],
    )
    .map_err(ApiError::internal)?;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: None,
        message_reaction_count: None,
        inline_query: Some(inline_query.clone()),
        chosen_inline_result: None,
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "inline_query_id": inline_query_id,
        "cached": false,
    }))
}

pub fn handle_sim_get_inline_query_answer(
    state: &Data<AppState>,
    token: &str,
    inline_query_id: &str,
) -> ApiResult {
    if inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "inline_query_id": inline_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

pub fn apply_inline_reply_markup(target: &mut Value, reply_markup: Option<InlineKeyboardMarkup>) {
    if let Some(markup) = reply_markup {
        if let Ok(value) = serde_json::to_value(markup) {
            target["reply_markup"] = value;
        }
    } else {
        target.as_object_mut().map(|obj| obj.remove("reply_markup"));
    }
}
