use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};

use crate::database::{ensure_chat, AppState};

use crate::types::ApiError;
use crate::handlers::client::types::users::{SimUserRecord};
use crate::handlers::client::types::channels::{LinkedDiscussionTransportContext};

use crate::generated::types::{
    Chat, User, ReplyKeyboardMarkup, ReplyKeyboardRemove
};

use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id, value_to_optional_bool_loose};

use super::chats::ChatSendKind;
use super::{chats, groups, users, webhook};

pub fn is_service_message_for_transport(message: &Value) -> bool {
    [
        "new_chat_members",
        "left_chat_member",
        "new_chat_title",
        "new_chat_photo",
        "delete_chat_photo",
        "group_chat_created",
        "supergroup_chat_created",
        "channel_chat_created",
        "message_auto_delete_timer_changed",
        "pinned_message",
        "forum_topic_created",
        "forum_topic_edited",
        "forum_topic_closed",
        "forum_topic_reopened",
        "general_forum_topic_hidden",
        "general_forum_topic_unhidden",
        "write_access_allowed",
        "users_shared",
        "chat_shared",
        "giveaway_created",
        "video_chat_started",
        "video_chat_ended",
        "video_chat_participants_invited",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

pub fn message_has_transportable_content(message: &Value) -> bool {
    [
        "text",
        "photo",
        "video",
        "audio",
        "voice",
        "document",
        "animation",
        "video_note",
        "sticker",
        "poll",
        "dice",
        "game",
        "contact",
        "location",
        "venue",
        "invoice",
        "paid_media",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

pub fn copy_message_internal(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    to_chat_id_value: &Value,
    source_message_id: i64,
    message_thread_id: Option<i64>,
    caption_override: Option<String>,
    caption_entities_override: Option<Value>,
    remove_caption: bool,
    show_caption_above_media: Option<bool>,
    reply_markup_override: Option<Value>,
    protect_content: Option<bool>,
    reply_to_message_id_override: Option<i64>,
    sender_chat_override: Option<Value>,
    is_automatic_forward_override: Option<bool>,
    linked_discussion_context: Option<LinkedDiscussionTransportContext>,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_message = resolve_source_message_for_transport(
        conn,
        bot,
        from_chat_id_value,
        source_message_id,
        skip_source_membership_check,
    )?;

    let send_kind = send_kind_from_transport_source_message(&source_message);
    let (destination_chat_key, destination_chat) =
        chats::resolve_bot_outbound_chat(conn, bot.id, to_chat_id_value, send_kind)?;
    let sender_user = users::resolve_transport_sender_user(
        conn,
        bot,
        &destination_chat_key,
        &destination_chat,
        send_kind,
    )?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        conn,
        bot.id,
        &destination_chat_key,
        message_thread_id,
    )?;

    let normalized_reply_markup = match reply_markup_override {
        Some(markup) => {
            handle_reply_markup_state(conn, bot.id, &destination_chat_key, Some(&markup))?
        }
        None => None,
    };
    let reply_to_message_value = reply_to_message_id_override
        .map(|reply_id| load_reply_message_for_chat(conn, bot, &destination_chat_key, reply_id))
        .transpose()?;

    let mut message_value = source_message;
    let source_has_media = message_has_media(&message_value);
    let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("copied message payload is invalid"))?;
    object.remove("forward_origin");
    object.remove("is_automatic_forward");
    object.remove("reply_to_message");
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");
    object.remove("sender_chat");
    object.insert("from".to_string(), sender_user_value);

    if let Some(sender_chat_value) = sender_chat_override {
        object.insert("sender_chat".to_string(), sender_chat_value);
    }
    if let Some(is_automatic_forward) = is_automatic_forward_override {
        object.insert(
            "is_automatic_forward".to_string(),
            Value::Bool(is_automatic_forward),
        );
    }

    if source_has_media {
        if remove_caption {
            object.remove("caption");
            object.remove("caption_entities");
        } else if let Some(caption) = caption_override {
            object.insert("caption".to_string(), Value::String(caption));
            if let Some(entities) = caption_entities_override {
                object.insert("caption_entities".to_string(), entities);
            } else {
                object.remove("caption_entities");
            }
        }

        if let Some(show_above) = show_caption_above_media {
            object.insert("show_caption_above_media".to_string(), Value::Bool(show_above));
        }
    }

    if let Some(markup) = normalized_reply_markup {
        object.insert("reply_markup".to_string(), markup);
    }
    if let Some(reply_value) = reply_to_message_value {
        object.insert("reply_to_message".to_string(), reply_value);
    }

    if let Some(thread_id) = resolved_thread_id {
        object.insert("message_thread_id".to_string(), Value::from(thread_id));
        object.insert("is_topic_message".to_string(), Value::Bool(true));
    } else {
        object.remove("message_thread_id");
        object.remove("is_topic_message");
    }

    if let Some(should_protect) = protect_content {
        object.insert(
            "has_protected_content".to_string(),
            Value::Bool(should_protect),
        );
    }

    persist_transported_message(
        state,
        conn,
        token,
        bot,
        &destination_chat_key,
        &destination_chat,
        &sender_user,
        message_value,
        linked_discussion_context.as_ref(),
    )
}

pub fn resolve_source_message_for_transport(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    source_message_id: i64,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_chat_key = value_to_chat_key(from_chat_id_value)?;
    ensure_chat(conn, &source_chat_key)?;

    if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot.id, &source_chat_key)? {
        if sim_chat.chat_type != "private" && !skip_source_membership_check {
            chats::ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, bot.id)?;
            if let Some(actor_user_id) = current_request_actor_user_id() {
                if actor_user_id != bot.id {
                    chats::ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, actor_user_id)?;
                }
            }
        }
    }

    let source_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &source_chat_key, source_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if source_exists.is_none() {
        return Err(ApiError::bad_request("source message was not found"));
    }

    let source_message = load_message_value(conn, bot, source_message_id)?;

    if is_service_message_for_transport(&source_message) {
        return Err(ApiError::bad_request(
            "service messages can't be forwarded or copied",
        ));
    }

    if !message_has_transportable_content(&source_message) {
        return Err(ApiError::bad_request(
            "message content can't be forwarded or copied",
        ));
    }

    if source_message
        .get("has_protected_content")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "message has protected content and can't be forwarded or copied",
        ));
    }

    Ok(source_message)
}

pub fn send_kind_from_transport_source_message(message: &Value) -> ChatSendKind {
    if message.get("text").is_some() {
        return ChatSendKind::Text;
    }
    if message.get("photo").is_some() {
        return ChatSendKind::Photo;
    }
    if message.get("video").is_some() {
        return ChatSendKind::Video;
    }
    if message.get("audio").is_some() {
        return ChatSendKind::Audio;
    }
    if message.get("voice").is_some() {
        return ChatSendKind::Voice;
    }
    if message.get("document").is_some() {
        return ChatSendKind::Document;
    }
    if message.get("video_note").is_some() {
        return ChatSendKind::VideoNote;
    }
    if message.get("poll").is_some() {
        return ChatSendKind::Poll;
    }
    if message.get("invoice").is_some() {
        return ChatSendKind::Invoice;
    }

    ChatSendKind::Other
}

pub fn handle_reply_markup_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    reply_markup: Option<&Value>,
) -> Result<Option<Value>, ApiError> {
    let Some(markup_value) = reply_markup else {
        return Ok(None);
    };

    if markup_value.get("keyboard").is_some() {
        let normalized_markup_value = normalize_legacy_reply_keyboard_markup(markup_value);
        let parsed: ReplyKeyboardMarkup = serde_json::from_value(normalized_markup_value)
            .map_err(|_| ApiError::bad_request("reply_markup keyboard is invalid"))?;

        if parsed.keyboard.is_empty() {
            return Err(ApiError::bad_request("reply_markup keyboard must have at least one row"));
        }

        if parsed
            .keyboard
            .iter()
            .any(|row| row.is_empty() || row.iter().any(|button| button.text.trim().is_empty()))
        {
            return Err(ApiError::bad_request("keyboard rows/buttons must not be empty"));
        }

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO chat_reply_keyboards (bot_id, chat_key, markup_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(bot_id, chat_key)
             DO UPDATE SET markup_json = excluded.markup_json, updated_at = excluded.updated_at",
            params![bot_id, chat_key, normalized.to_string(), now],
        )
        .map_err(ApiError::internal)?;

        return Ok(Some(normalized));
    }

    if markup_value.get("remove_keyboard").is_some() {
        let parsed: ReplyKeyboardRemove = serde_json::from_value(markup_value.clone())
            .map_err(|_| ApiError::bad_request("reply_markup remove_keyboard is invalid"))?;

        if !parsed.remove_keyboard {
            return Err(ApiError::bad_request("remove_keyboard must be true"));
        }

        conn.execute(
            "DELETE FROM chat_reply_keyboards WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
        )
        .map_err(ApiError::internal)?;

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        return Ok(Some(normalized));
    }

    Ok(Some(markup_value.clone()))
}

pub fn normalize_legacy_reply_keyboard_markup(markup_value: &Value) -> Value {
    let mut normalized = markup_value.clone();
    let Some(rows) = normalized
        .get_mut("keyboard")
        .and_then(Value::as_array_mut)
    else {
        return normalized;
    };

    for row in rows {
        let Some(buttons) = row.as_array_mut() else {
            continue;
        };

        for button in buttons {
            let Some(button_obj) = button.as_object_mut() else {
                continue;
            };

            if button_obj.contains_key("request_users") {
                continue;
            }

            let Some(legacy_request_user) = button_obj.get("request_user") else {
                continue;
            };

            if let Some(request_users) = normalize_legacy_request_user_payload(legacy_request_user)
            {
                button_obj.insert("request_users".to_string(), request_users);
            }
        }
    }

    normalized
}

pub fn normalize_legacy_request_user_payload(legacy_request_user: &Value) -> Option<Value> {
    let legacy = legacy_request_user.as_object()?;
    let request_id = legacy.get("request_id").and_then(|raw| {
        raw.as_i64()
            .or_else(|| raw.as_str().and_then(|v| v.trim().parse::<i64>().ok()))
    })?;

    let mut normalized = Map::new();
    normalized.insert("request_id".to_string(), Value::from(request_id));
    normalized.insert("max_quantity".to_string(), Value::from(10));

    let mappings = [
        ("user_is_bot", "user_is_bot"),
        ("user_is_premium", "user_is_premium"),
        ("request_name", "request_name"),
        ("request_username", "request_username"),
        ("request_photo", "request_photo"),
    ];

    for (legacy_key, target_key) in mappings {
        if let Some(value) = legacy
            .get(legacy_key)
            .and_then(value_to_optional_bool_loose)
        {
            normalized.insert(target_key.to_string(), Value::Bool(value));
        }
    }

    Some(Value::Object(normalized))
}

pub fn load_reply_message_for_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
    reply_message_id: i64,
) -> Result<Value, ApiError> {
    let reply_chat_key: Option<String> = conn
        .query_row(
            "SELECT chat_key FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, reply_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(reply_chat_key) = reply_chat_key else {
        return Err(ApiError::not_found("reply message not found"));
    };

    if reply_chat_key != chat_key {
        return Err(ApiError::bad_request("reply message not found in this chat"));
    }

    load_message_value(conn, bot, reply_message_id)
}

pub fn message_has_media(message: &Value) -> bool {
    ["photo", "video", "audio", "voice", "document", "animation", "video_note"]
        .iter()
        .any(|key| message.get(*key).is_some())
}

pub fn persist_transported_message(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    sender_user: &User,
    mut message_value: Value,
    linked_discussion_context: Option<&LinkedDiscussionTransportContext>,
) -> Result<Value, ApiError> {
    let now = Utc::now().timestamp();
    let persisted_text = message_value
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message_value.get("caption").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, destination_chat_key, sender_user.id, persisted_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let destination_chat_value = serde_json::to_value(destination_chat).map_err(ApiError::internal)?;
    let sender_user_value = serde_json::to_value(sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("transported message payload is invalid"))?;
    object.insert("message_id".to_string(), Value::from(message_id));
    object.insert("date".to_string(), Value::from(now));
    object.insert("chat".to_string(), destination_chat_value);
    object.insert("from".to_string(), sender_user_value);
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");

    if let Some(context) = linked_discussion_context {
        let discussion_root_message_id = context
            .discussion_root_message_id
            .unwrap_or(message_id);
        object.insert(
            "linked_channel_chat_id".to_string(),
            Value::String(context.channel_chat_key.clone()),
        );
        object.insert(
            "linked_channel_message_id".to_string(),
            Value::from(context.channel_message_id),
        );
        object.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
    }

    let update_value = if destination_chat.r#type == "channel" {
        json!({
            "update_id": 0,
            "channel_post": message_value.clone(),
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_value.clone(),
        })
    };

    webhook::persist_and_dispatch_update(state, conn, token, bot.id, update_value)?;
    Ok(message_value)
}

pub fn load_message_value(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    message_id: i64,
) -> Result<Value, ApiError> {
    let row: Option<(String, i64, String, i64)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, text, date FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, text, date)) = row else {
        return Err(ApiError::not_found("message not found"));
    };

    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| chats::fallback_chat_id(&chat_key));
    let (is_bot, first_name, username) = if from_user_id == bot.id {
        (true, bot.first_name.clone(), Some(bot.username.clone()))
    } else {
        let user: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT first_name, username FROM users WHERE id = ?1",
                params![from_user_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let (first, uname) = user.unwrap_or_else(|| ("User".to_string(), None));
        (false, first, uname)
    };

    let mut message = find_message_snapshot(conn, bot.id, message_id).unwrap_or_else(|| {
        json!({
            "message_id": message_id,
            "date": date,
            "from": {
                "id": from_user_id,
                "is_bot": is_bot,
                "first_name": first_name,
                "username": username
            }
        })
    });

    let chat = if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot.id, &chat_key).ok().flatten() {
        let sender = SimUserRecord {
            id: from_user_id,
            first_name: first_name.clone(),
            username: username.clone(),
            last_name: None,
            is_premium: false,
        };
        chats::chat_from_sim_record(&sim_chat, &sender)
    } else {
        Chat {
            id: chat_id,
            r#type: "private".to_string(),
            title: None,
            username: username.clone(),
            first_name: Some(first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        }
    };

    message["message_id"] = Value::from(message_id);
    message["date"] = Value::from(date);
    message.as_object_mut().map(|obj| obj.remove("edit_date"));
    message["chat"] = serde_json::to_value(chat).map_err(ApiError::internal)?;
    message["from"] = json!({
        "id": from_user_id,
        "is_bot": is_bot,
        "first_name": first_name,
        "username": username
    });

    if message_has_media(&message) {
        message.as_object_mut().map(|obj| obj.remove("text"));
        message["caption"] = Value::String(text);
    } else {
        message.as_object_mut().map(|obj| obj.remove("caption"));
        message["text"] = Value::String(text);
    }

    Ok(message)
}

pub fn find_message_snapshot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    message_id: i64,
) -> Option<Value> {
    let mut stmt = conn
        .prepare(
            "SELECT update_json FROM updates WHERE bot_id = ?1 ORDER BY update_id DESC LIMIT 5000",
        )
        .ok()?;

    let rows = stmt
        .query_map(params![bot_id], |row| row.get::<_, String>(0))
        .ok()?;

    for row in rows {
        let raw = row.ok()?;
        let update_value: Value = serde_json::from_str(&raw).ok()?;

        if let Some(msg) = update_value
            .get("edited_channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("edited_message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }
    }

    None
}

