use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    GetForumTopicIconStickersRequest,
    CreateForumTopicRequest, EditForumTopicRequest, CloseForumTopicRequest,
    ReopenForumTopicRequest, DeleteForumTopicRequest, UnpinAllForumTopicMessagesRequest,
    EditGeneralForumTopicRequest, CloseGeneralForumTopicRequest,
    ReopenGeneralForumTopicRequest, HideGeneralForumTopicRequest,
    UnhideGeneralForumTopicRequest, UnpinAllGeneralForumTopicMessagesRequest,
};
use crate::generated::types::{ForumTopic, Sticker};

use crate::handlers::utils::updates::current_request_actor_user_id;

use crate::handlers::client::{chats, channels, groups, messages};

use crate::handlers::parse_request;

pub fn handle_get_forum_topic_icon_stickers(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetForumTopicIconStickersRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let mut stmt = conn
        .prepare(
            "SELECT file_id, file_unique_id, sticker_type, width, height, is_animated, is_video,
                    emoji, set_name, mask_position_json, custom_emoji_id, needs_repainting
             FROM stickers
             WHERE bot_id = ?1 AND custom_emoji_id IS NOT NULL
             ORDER BY updated_at DESC
             LIMIT 64",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id], |row| {
            Ok(Sticker {
                file_id: row.get(0)?,
                file_unique_id: row.get(1)?,
                r#type: row.get(2)?,
                width: row.get(3)?,
                height: row.get(4)?,
                is_animated: row.get::<_, i64>(5)? == 1,
                is_video: row.get::<_, i64>(6)? == 1,
                thumbnail: None,
                emoji: row.get(7)?,
                set_name: row.get(8)?,
                premium_animation: None,
                mask_position: row
                    .get::<_, Option<String>>(9)?
                    .and_then(|raw| serde_json::from_str(&raw).ok()),
                custom_emoji_id: row.get(10)?,
                needs_repainting: Some(row.get::<_, i64>(11)? == 1),
                file_size: None,
            })
        })
        .map_err(ApiError::internal)?;

    let mut stickers: Vec<Sticker> = Vec::new();
    for row in rows {
        stickers.push(row.map_err(ApiError::internal)?);
    }

    serde_json::to_value(stickers).map_err(ApiError::internal)
}

pub fn handle_create_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateForumTopicRequest = parse_request(params)?;
    let name = request.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let icon_color = request.icon_color.unwrap_or_else(groups::forum_topic_default_icon_color);
    if !groups::is_allowed_forum_topic_icon_color(icon_color) {
        return Err(ApiError::bad_request("icon_color is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let mut message_thread_id =
        ((Utc::now().timestamp_millis().unsigned_abs() % 2_000_000_000) as i64).max(2);
    for _ in 0..8 {
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM forum_topics
                 WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
                params![bot.id, &chat_key, message_thread_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            break;
        }
        message_thread_id += 1;
    }

    let now = Utc::now().timestamp();
    let icon_custom_emoji_id = request
        .icon_custom_emoji_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    conn.execute(
        "INSERT INTO forum_topics
         (bot_id, chat_key, message_thread_id, name, icon_color, icon_custom_emoji_id, is_closed, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?7)",
        params![
            bot.id,
            &chat_key,
            message_thread_id,
            name,
            icon_color,
            icon_custom_emoji_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(message_thread_id));
    service_fields.insert(
        "forum_topic_created".to_string(),
        json!({
            "name": name,
            "icon_color": icon_color,
            "icon_custom_emoji_id": icon_custom_emoji_id,
        }),
    );
    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} created the topic \"{}\"", messages::display_name_for_service_user(&actor), name),
        service_fields,
    )?;

    let topic = ForumTopic {
        message_thread_id,
        name: name.to_string(),
        icon_color,
        icon_custom_emoji_id,
        is_name_implicit: None,
    };

    serde_json::to_value(topic).map_err(ApiError::internal)
}

pub fn handle_edit_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(current_topic) = groups::load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let next_name = if let Some(raw_name) = request.name.as_deref() {
        let trimmed = raw_name.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("name is empty"));
        }
        trimmed.to_string()
    } else {
        current_topic.name.clone()
    };

    let next_icon_custom_emoji_id = if let Some(raw_icon) = request.icon_custom_emoji_id.as_deref() {
        let trimmed = raw_icon.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    } else {
        current_topic.icon_custom_emoji_id.clone()
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET name = ?1,
             icon_custom_emoji_id = ?2,
             updated_at = ?3
         WHERE bot_id = ?4 AND chat_key = ?5 AND message_thread_id = ?6",
        params![
            next_name,
            next_icon_custom_emoji_id,
            now,
            bot.id,
            &chat_key,
            request.message_thread_id,
        ],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert(
        "forum_topic_edited".to_string(),
        json!({
            "name": if next_name != current_topic.name { Some(next_name.clone()) } else { None::<String> },
            "icon_custom_emoji_id": if request.icon_custom_emoji_id.is_some() {
                next_icon_custom_emoji_id.clone()
            } else {
                None
            },
        }),
    );

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} edited topic \"{}\"",
            messages::display_name_for_service_user(&actor),
            next_name
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_close_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CloseForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = groups::load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET is_closed = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND message_thread_id = ?4",
        params![now, bot.id, &chat_key, request.message_thread_id],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert("forum_topic_closed".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} closed a topic", messages::display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_reopen_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReopenForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = groups::load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE forum_topics
         SET is_closed = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3 AND message_thread_id = ?4",
        params![now, bot.id, &chat_key, request.message_thread_id],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert(
        "message_thread_id".to_string(),
        Value::from(request.message_thread_id),
    );
    service_fields.insert("forum_topic_reopened".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!("{} reopened a topic", messages::display_name_for_service_user(&actor)),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_delete_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, sim_chat) = chats::resolve_non_private_sim_chat(&mut conn, bot.id, &request.chat_id)?;

    if channels::is_direct_messages_chat(&sim_chat) {
        if request.message_thread_id <= 0 {
            return Err(ApiError::bad_request("message_thread_id is invalid"));
        }

        let _topic = channels::load_direct_messages_topic_record(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?
        .ok_or_else(|| ApiError::not_found("direct messages topic not found"))?;

        let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
        let parent_channel_chat_id = sim_chat
            .parent_channel_chat_id
            .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
        let parent_channel_chat_key = parent_channel_chat_id.to_string();
        let actor_can_manage_direct_messages = channels::ensure_channel_member_can_manage_direct_messages(
            &mut conn,
            bot.id,
            &parent_channel_chat_key,
            actor_user_id,
        )
        .is_ok();

        if !actor_can_manage_direct_messages {
            return Err(ApiError::bad_request(
                "not enough rights to delete this direct messages topic",
            ));
        }

        let topic_message_ids = groups::collect_message_ids_for_thread(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?;
        let _ = messages::delete_messages_with_dependencies(
            &mut conn,
            bot.id,
            &chat_key,
            sim_chat.chat_id,
            &topic_message_ids,
        )?;

        conn.execute(
            "DELETE FROM sim_direct_message_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND topic_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "DELETE FROM sim_message_drafts
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;

        return Ok(Value::Bool(true));
    }

    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        return Err(ApiError::bad_request(
            "forum topics are available only in forum supergroups",
        ));
    }

    let _actor = chats::resolve_chat_admin_actor(&mut conn, &bot, &chat_key)?;

    let deleted = conn
        .execute(
            "DELETE FROM forum_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, request.message_thread_id],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("forum topic not found"));
    }

    let topic_message_ids = groups::collect_message_ids_for_thread(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let _ = messages::delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        sim_chat.chat_id,
        &topic_message_ids,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_unpin_all_forum_topic_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllForumTopicMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, _chat, _actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    let Some(_) = groups::load_forum_topic(&mut conn, bot.id, &chat_key, request.message_thread_id)? else {
        return Err(ApiError::not_found("forum topic not found"));
    };

    Ok(Value::Bool(true))
}

pub fn handle_edit_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditGeneralForumTopicRequest = parse_request(params)?;
    let name = request.name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    let (current_name, _, _) = groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET name = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![name, now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert(
        "forum_topic_edited".to_string(),
        json!({
            "name": if name != current_name {
                Some(name.to_string())
            } else {
                None::<String>
            },
            "icon_custom_emoji_id": None::<String>,
        }),
    );

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} edited topic \"{}\"",
            messages::display_name_for_service_user(&actor),
            name
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_close_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CloseGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_closed = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("forum_topic_closed".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} closed the General topic",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_reopen_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReopenGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_closed = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("forum_topic_reopened".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} reopened the General topic",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_hide_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: HideGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_hidden = 1, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("general_forum_topic_hidden".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} hid the General topic",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_unhide_general_forum_topic(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnhideGeneralForumTopicRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, chat, actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;
    let now = Utc::now().timestamp();

    groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    conn.execute(
        "UPDATE forum_topic_general_states
         SET is_hidden = 0, updated_at = ?1
         WHERE bot_id = ?2 AND chat_key = ?3",
        params![now, bot.id, &chat_key],
    )
    .map_err(ApiError::internal)?;

    let mut service_fields = Map::new();
    service_fields.insert("is_topic_message".to_string(), Value::Bool(true));
    service_fields.insert("message_thread_id".to_string(), Value::from(1_i64));
    service_fields.insert("general_forum_topic_unhidden".to_string(), json!({}));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &chat_key,
        &chat,
        &actor,
        now,
        format!(
            "{} unhid the General topic",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(Value::Bool(true))
}

pub fn handle_unpin_all_general_forum_topic_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UnpinAllGeneralForumTopicMessagesRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, _sim_chat, _chat, _actor) =
        chats::resolve_forum_supergroup_chat(&mut conn, &bot, &request.chat_id)?;

    groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
    Ok(Value::Bool(true))
}
