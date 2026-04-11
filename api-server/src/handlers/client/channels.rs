use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};

use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};
use crate::generated::methods::PromoteChatMemberRequest;
use crate::generated::types::{
    DirectMessagesTopic, Chat
};
use crate::handlers::utils::updates::{
    value_to_chat_key, current_request_actor_user_id, with_request_actor_user_id
};
use crate::handlers::generate_telegram_numeric_id;

use super::types::chats::SimChatRecord;
use super::types::users::SimUserRecord;
use super::types::channels::{
    LinkedDiscussionTransportContext, ChannelAdminRights, SimMarkChannelMessageViewRequest,
    SimDirectMessagesTopicRecord, SimOpenChannelDirectMessagesRequest
};

use super::{bot, business, chats, channels, groups, messages, users};

const CHANNEL_VIEW_WINDOW_SECONDS: i64 = 24 * 60 * 60;

pub fn channel_admin_can_publish(rights: &ChannelAdminRights) -> bool {
    rights.can_manage_chat || rights.can_post_messages
}

pub fn parse_channel_admin_rights_json(raw: Option<&str>) -> ChannelAdminRights {
    raw.and_then(|value| serde_json::from_str::<ChannelAdminRights>(value).ok())
        .unwrap_or_else(channel_admin_rights_full_access)
}

pub fn channel_admin_has_direct_messages_permission(raw: Option<&str>) -> bool {
    raw.and_then(|value| serde_json::from_str::<ChannelAdminRights>(value).ok())
        .map(|rights| rights.can_manage_direct_messages)
        .unwrap_or(false)
}

pub fn channel_admin_rights_full_access() -> ChannelAdminRights {
    ChannelAdminRights {
        can_manage_chat: true,
        can_post_messages: true,
        can_edit_messages: true,
        can_delete_messages: true,
        can_invite_users: true,
        can_change_info: true,
        can_manage_direct_messages: true,
    }
}

impl Default for ChannelAdminRights {
    fn default() -> Self {
        channel_admin_rights_full_access()
    }
}

pub fn channel_admin_rights_from_promote_request(request: &PromoteChatMemberRequest) -> ChannelAdminRights {
    ChannelAdminRights {
        can_manage_chat: request.can_manage_chat.unwrap_or(false),
        can_post_messages: request.can_post_messages.unwrap_or(false),
        can_edit_messages: request.can_edit_messages.unwrap_or(false),
        can_delete_messages: request.can_delete_messages.unwrap_or(false),
        can_invite_users: request.can_invite_users.unwrap_or(false),
        can_change_info: request.can_change_info.unwrap_or(false),
        can_manage_direct_messages: request.can_manage_direct_messages.unwrap_or(false),
    }
}

pub fn load_channel_owner_user_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
) -> Result<Option<i64>, ApiError> {
    conn.query_row(
        "SELECT user_id
         FROM sim_chat_members
         WHERE bot_id = ?1 AND chat_key = ?2 AND status = 'owner'
         LIMIT 1",
        params![bot_id, channel_chat_key],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn ensure_channel_actor_can_manage_invite_links(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    actor_user_id: i64,
) -> Result<(), ApiError> {
    let Some(actor_record) = chats::load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin can manage invite links",
        ));
    };

    if actor_record.status == "owner" {
        return Ok(());
    }

    if actor_record.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin can manage invite links",
        ));
    }

    let rights = parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
    if rights.can_manage_chat || rights.can_invite_users {
        return Ok(());
    }

    if actor_user_id == bot_id {
        return Err(ApiError::bad_request(
            "bot is not allowed to manage invite links in this channel",
        ));
    }

    Err(ApiError::bad_request(
        "not enough rights to manage invite links in this channel",
    ))
}

// --- Posts ---
pub fn enrich_channel_post_payloads(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update_value: &mut Value,
) -> Result<(), ApiError> {
    let Some(update_obj) = update_value.as_object_mut() else {
        return Ok(());
    };

    for field in ["channel_post", "edited_channel_post"] {
        let Some(message_value) = update_obj.get_mut(field) else {
            continue;
        };
        let Some(message_obj) = message_value.as_object_mut() else {
            continue;
        };

        let Some(chat_obj) = message_obj.get("chat").and_then(Value::as_object) else {
            continue;
        };
        let Some(chat_id_value) = chat_obj.get("id") else {
            continue;
        };
        let Ok(chat_key) = value_to_chat_key(chat_id_value) else {
            continue;
        };

        let is_channel_chat = chat_obj
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| kind == "channel")
            .unwrap_or(false);
        if !is_channel_chat {
            continue;
        }

        if !message_obj.contains_key("sender_chat") {
            message_obj.insert(
                "sender_chat".to_string(),
                Value::Object(chat_obj.clone()),
            );
        }

        let show_author_signature = channel_show_author_signature_enabled(conn, bot_id, &chat_key)?;

        let has_signature = message_obj
            .get("author_signature")
            .and_then(Value::as_str)
            .map(str::trim)
            .map(|value| !value.is_empty())
            .unwrap_or(false);
        if show_author_signature && !has_signature {
            if let Some(signature) = derive_channel_author_signature(conn, bot_id, &chat_key, message_obj) {
                message_obj.insert("author_signature".to_string(), Value::String(signature));
            }
        } else if !show_author_signature {
            message_obj.remove("author_signature");
        }

        if let Some(message_id) = message_obj.get("message_id").and_then(Value::as_i64) {
            let views = load_channel_post_views(conn, bot_id, &chat_key, message_id)?;
            message_obj.insert("views".to_string(), Value::from(views));
        }
    }

    Ok(())
}

pub fn channel_show_author_signature_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<bool, ApiError> {
    Ok(chats::load_sim_chat_record(conn, bot_id, chat_key)?
        .map(|record| record.channel_show_author_signature)
        .unwrap_or(false))
}

pub fn derive_channel_author_signature(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_obj: &Map<String, Value>,
) -> Option<String> {
    if let Some(actor_user_id) = current_request_actor_user_id() {
        let actor_record = chats::load_chat_member_record(conn, bot_id, chat_key, actor_user_id)
            .ok()
            .flatten();
        if let Some(record) = actor_record {
            let actor_can_publish = record.status == "owner"
                || (record.status == "admin"
                    && channel_admin_can_publish(&parse_channel_admin_rights_json(
                        record.admin_rights_json.as_deref(),
                    )));
            if actor_can_publish {
                if let Ok(Some(user)) = users::load_sim_user_record(conn, actor_user_id) {
                    if !user.first_name.trim().is_empty() {
                        return Some(user.first_name);
                    }
                }
            }
        }
    }

    if let Some(from_first_name) = message_obj
        .get("from")
        .and_then(Value::as_object)
        .and_then(|from| from.get("first_name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return Some(from_first_name.to_string());
    }

    message_obj
        .get("sender_chat")
        .and_then(Value::as_object)
        .and_then(|sender_chat| sender_chat.get("title"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn load_channel_post_views(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<i64, ApiError> {
    let views: Option<i64> = conn
        .query_row(
            "SELECT views
             FROM sim_channel_post_stats
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    Ok(views.unwrap_or(0).max(0))
}

pub fn handle_sim_mark_channel_message_view(
    state: &Data<AppState>,
    token: &str,
    body: SimMarkChannelMessageViewRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("chat is not a channel"));
    }

    let viewer = users::ensure_user(
        &mut conn,
        body.user_id,
        body.first_name,
        body.username,
    )?;
    chats::ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, viewer.id)?;

    let message_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id
             FROM messages
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, body.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if message_exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let (views, incremented) = mark_channel_post_view_for_user(
        &mut conn,
        bot.id,
        &chat_key,
        body.message_id,
        viewer.id,
    )?;

    Ok(json!({
        "chat_id": body.chat_id,
        "chat_type": sim_chat.chat_type,
        "message_id": body.message_id,
        "user_id": viewer.id,
        "views": views,
        "incremented": incremented,
        "window_seconds": CHANNEL_VIEW_WINDOW_SECONDS,
    }))
}

pub fn store_channel_post_views(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    views: i64,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_channel_post_stats (bot_id, chat_key, message_id, views, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, chat_key, message_id)
         DO UPDATE SET views = excluded.views, updated_at = excluded.updated_at",
        params![bot_id, chat_key, message_id, views.max(0), updated_at],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn mark_channel_post_view_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    viewer_user_id: i64,
) -> Result<(i64, bool), ApiError> {
    let now = Utc::now().timestamp();
    let last_viewed_at: Option<i64> = conn
        .query_row(
            "SELECT viewed_at
             FROM sim_channel_post_viewers
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND viewer_user_id = ?4",
            params![bot_id, chat_key, message_id, viewer_user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let should_increment = last_viewed_at
        .map(|seen_at| now.saturating_sub(seen_at) >= CHANNEL_VIEW_WINDOW_SECONDS)
        .unwrap_or(true);
    if !should_increment {
        return Ok((load_channel_post_views(conn, bot_id, chat_key, message_id)?, false));
    }

    conn.execute(
        "INSERT INTO sim_channel_post_viewers (bot_id, chat_key, message_id, viewer_user_id, viewed_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, chat_key, message_id, viewer_user_id)
         DO UPDATE SET viewed_at = excluded.viewed_at",
        params![bot_id, chat_key, message_id, viewer_user_id, now],
    )
    .map_err(ApiError::internal)?;

    let next_views = load_channel_post_views(conn, bot_id, chat_key, message_id)?
        .saturating_add(1);
    store_channel_post_views(conn, bot_id, chat_key, message_id, next_views, now)?;

    Ok((next_views, true))
}

pub fn ensure_request_actor_can_publish_to_channel(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(), ApiError> {
    let Some(actor_user_id) = current_request_actor_user_id() else {
        return Ok(());
    };

    let Some(actor_record) = chats::load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    };

    if actor_record.status == "owner" {
        return Ok(());
    }

    if actor_record.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    }

    let rights = parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
    if !channel_admin_can_publish(&rights) {
        if actor_user_id == bot_id {
            return Err(ApiError::bad_request(
                "bot is not allowed to publish messages in this channel",
            ));
        }
        return Err(ApiError::bad_request(
            "not enough rights to publish messages in this channel",
        ));
    }

    Ok(())
}

// --- Disscusion ---

pub fn forward_channel_post_to_linked_discussion_best_effort(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    channel_chat_key: &str,
    channel_message_value: &Value,
) {
    if let Err(error) = ensure_linked_discussion_forward_for_channel_post(
        state,
        conn,
        token,
        bot,
        channel_chat_key,
        channel_message_value,
    ) {
        eprintln!(
            "linked discussion auto-forward failed for chat {}: {}",
            channel_chat_key, error.description
        );
    }
}

pub fn ensure_linked_discussion_forward_for_channel_post(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    channel_chat_key: &str,
    channel_message_value: &Value,
) -> Result<(), ApiError> {
    let Some(channel_sim_chat) = chats::load_sim_chat_record(conn, bot.id, channel_chat_key)? else {
        return Ok(());
    };
    if channel_sim_chat.chat_type != "channel" {
        return Ok(());
    }

    let Some(linked_discussion_chat_id) = channel_sim_chat.linked_discussion_chat_id else {
        return Ok(());
    };
    let linked_discussion_chat_key = linked_discussion_chat_id.to_string();
    let Some(linked_discussion_chat) = chats::load_sim_chat_record(conn, bot.id, &linked_discussion_chat_key)? else {
        return Ok(());
    };
    if linked_discussion_chat.chat_type != "group" && linked_discussion_chat.chat_type != "supergroup" {
        return Ok(());
    }

    let channel_message_id = channel_message_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("channel_post missing message_id"))?;

    if messages::is_service_message_for_transport(channel_message_value)
        || !messages::message_has_transportable_content(channel_message_value)
    {
        return Ok(());
    }

    let existing_forward: Option<(i64, i64)> = conn
        .query_row(
            "SELECT discussion_message_id, discussion_root_message_id
             FROM sim_linked_discussion_messages
             WHERE bot_id = ?1
               AND channel_chat_key = ?2
               AND channel_message_id = ?3
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, channel_chat_key, channel_message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if existing_forward.is_some() {
        return Ok(());
    }

    let sender_chat_override = channel_message_value
        .get("sender_chat")
        .cloned()
        .or_else(|| channel_message_value.get("chat").cloned());
    let linked_discussion_context = LinkedDiscussionTransportContext {
        channel_chat_key: channel_chat_key.to_string(),
        channel_message_id,
        discussion_root_message_id: None,
    };
    let discussion_reply_to_message_id = channel_message_value
        .get("reply_to_message")
        .and_then(Value::as_object)
        .and_then(|reply| reply.get("message_id"))
        .and_then(Value::as_i64)
        .map(|parent_channel_message_id| {
            conn.query_row(
                "SELECT discussion_root_message_id
                 FROM sim_linked_discussion_messages
                 WHERE bot_id = ?1
                   AND discussion_chat_key = ?2
                   AND channel_chat_key = ?3
                   AND channel_message_id = ?4
                 ORDER BY updated_at DESC
                 LIMIT 1",
                params![
                    bot.id,
                    &linked_discussion_chat_key,
                    channel_chat_key,
                    parent_channel_message_id,
                ],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)
        })
        .transpose()?
        .flatten();

    let forwarded_value = with_request_actor_user_id(Some(bot.id), || {
        messages::copy_message_internal(
            state,
            conn,
            token,
            bot,
            &Value::String(channel_chat_key.to_string()),
            &Value::from(linked_discussion_chat_id),
            channel_message_id,
            None,
            None,
            None,
            false,
            None,
            None,
            None,
            discussion_reply_to_message_id,
            sender_chat_override,
            Some(true),
            Some(linked_discussion_context),
            true,
        )
    })?;

    let discussion_message_id = forwarded_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("forwarded discussion message missing message_id"))?;
    let now = Utc::now().timestamp();

    save_linked_discussion_mapping(
        conn,
        bot.id,
        &linked_discussion_chat_key,
        discussion_message_id,
        discussion_message_id,
        channel_chat_key,
        channel_message_id,
        now,
    )?;

    Ok(())
}

pub fn save_linked_discussion_mapping(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    discussion_root_message_id: i64,
    channel_chat_key: &str,
    channel_message_id: i64,
    now: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_linked_discussion_messages
         (bot_id, discussion_chat_key, discussion_message_id, discussion_root_message_id, channel_chat_key, channel_message_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)
         ON CONFLICT(bot_id, discussion_chat_key, discussion_message_id)
         DO UPDATE SET
             discussion_root_message_id = excluded.discussion_root_message_id,
             channel_chat_key = excluded.channel_chat_key,
             channel_message_id = excluded.channel_message_id,
             updated_at = excluded.updated_at",
        params![
            bot_id,
            discussion_chat_key,
            discussion_message_id,
            discussion_root_message_id,
            channel_chat_key,
            channel_message_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn load_linked_discussion_mapping_for_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
) -> Result<Option<(String, i64, i64)>, ApiError> {
    conn.query_row(
        "SELECT channel_chat_key, channel_message_id, discussion_root_message_id
         FROM sim_linked_discussion_messages
         WHERE bot_id = ?1 AND discussion_chat_key = ?2 AND discussion_message_id = ?3",
        params![bot_id, discussion_chat_key, discussion_message_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn is_reply_to_linked_discussion_root_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    reply_to_message_id: Option<i64>,
) -> Result<bool, ApiError> {
    let Some(reply_id) = reply_to_message_id else {
        return Ok(false);
    };

    Ok(load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_id,
    )?
    .is_some())
}

pub fn enrich_reply_with_linked_channel_context(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    reply_to_message_id: i64,
    message_json: &mut Value,
) -> Result<(), ApiError> {
    let reply_mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_to_message_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = reply_mapping else {
        return Ok(());
    };

    if let Some(reply_obj) = message_json
        .get_mut("reply_to_message")
        .and_then(Value::as_object_mut)
    {
        reply_obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        reply_obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        reply_obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key.clone()));
    }

    if let Some(obj) = message_json.as_object_mut() {
        obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key));
    }

    Ok(())
}

pub fn enrich_message_with_linked_channel_context(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    message_json: &mut Value,
) -> Result<(), ApiError> {
    let mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        discussion_message_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = mapping else {
        return Ok(());
    };

    if let Some(obj) = message_json.as_object_mut() {
        obj.insert("linked_channel_message_id".to_string(), Value::from(channel_message_id));
        obj.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
        obj.insert("linked_channel_chat_id".to_string(), Value::String(channel_chat_key));
    }

    Ok(())
}

pub fn map_discussion_message_to_channel_post_if_needed(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    discussion_chat_key: &str,
    discussion_message_id: i64,
    reply_to_message_id: Option<i64>,
) -> Result<(), ApiError> {
    let Some(reply_id) = reply_to_message_id else {
        return Ok(());
    };

    let reply_mapping = load_linked_discussion_mapping_for_message(
        conn,
        bot_id,
        discussion_chat_key,
        reply_id,
    )?;

    let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = reply_mapping else {
        return Ok(());
    };

    let now = Utc::now().timestamp();
    save_linked_discussion_mapping(
        conn,
        bot_id,
        discussion_chat_key,
        discussion_message_id,
        discussion_root_message_id,
        &channel_chat_key,
        channel_message_id,
        now,
    )?;

    Ok(())
}

// --- Direct Messages ---

pub fn is_direct_messages_chat(sim_chat: &SimChatRecord) -> bool {
    sim_chat.is_direct_messages && sim_chat.parent_channel_chat_id.is_some()
}

pub fn load_direct_messages_chat_for_request(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    let chat = chats::load_sim_chat_record(conn, bot_id, &chat_key)?
        .or(chats::load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)?)
        .ok_or_else(|| ApiError::not_found("chat not found"))?;

    if !is_direct_messages_chat(&chat) {
        return Err(ApiError::bad_request(
            "chat_id must be a channel direct messages chat",
        ));
    }

    Ok(chat)
}

pub fn direct_messages_star_count_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
) -> Result<i64, ApiError> {
    if !is_direct_messages_chat(sim_chat) {
        return Ok(0);
    }

    let parent_channel_chat_id = sim_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
    let parent_channel_chat = chats::load_sim_chat_record(conn, bot_id, &parent_channel_chat_id.to_string())?
        .ok_or_else(|| ApiError::not_found("parent channel not found"))?;
    if !parent_channel_chat.direct_messages_enabled {
        return Err(ApiError::bad_request("channel direct messages are disabled"));
    }

    Ok(parent_channel_chat.direct_messages_star_count.max(0))
}

pub fn ensure_channel_member_can_manage_direct_messages(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let Some(member) = chats::load_chat_member_record(conn, bot_id, channel_chat_key, user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin with direct-messages rights can access channel direct messages",
        ));
    };

    if member.status == "owner" {
        return Ok(());
    }

    if member.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin with direct-messages rights can access channel direct messages",
        ));
    }

    if channel_admin_has_direct_messages_permission(member.admin_rights_json.as_deref()) {
        return Ok(());
    }

    Err(ApiError::bad_request(
        "not enough rights to manage channel direct messages",
    ))
}

pub fn ensure_channel_direct_messages_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat: &SimChatRecord,
) -> Result<SimChatRecord, ApiError> {
    if channel_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("channel direct messages are available only for channels"));
    }

    let chat_key = direct_messages_chat_key(channel_chat.chat_id);
    ensure_chat(conn, &chat_key)?;
    let now = Utc::now().timestamp();
    let chat_id = direct_messages_chat_id_for_channel(channel_chat.chat_id);
    let channel_title = channel_chat
        .title
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("Channel");
    let dm_title = format!("{} Direct Messages", channel_title);

    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id,
          is_forum, is_direct_messages, parent_channel_chat_id, channel_show_author_signature,
          linked_discussion_chat_id, message_history_visible, slow_mode_delay, permissions_json,
          sticker_set_name, pinned_message_id, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'supergroup', ?4, NULL, NULL, NULL,
             0, 1, ?5, 0,
                 NULL, 1, 0, NULL,
                 NULL, NULL, NULL, ?6, ?6)
         ON CONFLICT(bot_id, chat_key)
         DO UPDATE SET
                chat_id = excluded.chat_id,
            title = excluded.title,
            is_forum = 0,
            is_direct_messages = 1,
            parent_channel_chat_id = excluded.parent_channel_chat_id,
            updated_at = excluded.updated_at",
        params![bot_id, &chat_key, chat_id, dm_title, channel_chat.chat_id, now],
    )
    .map_err(ApiError::internal)?;

    chats::upsert_chat_member_record(
        conn,
        bot_id,
        &chat_key,
        bot_id,
        "admin",
        "admin",
        Some(now),
        None,
        None,
        None,
        None,
        now,
    )?;

    chats::load_sim_chat_record(conn, bot_id, &chat_key)?
        .ok_or_else(|| ApiError::internal("failed to create channel direct messages chat"))
}

pub fn load_direct_messages_topic_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    topic_id: i64,
) -> Result<Option<SimDirectMessagesTopicRecord>, ApiError> {
    conn.query_row(
        "SELECT topic_id, user_id, created_at, updated_at, last_message_id, last_message_date
         FROM sim_direct_message_topics
         WHERE bot_id = ?1 AND chat_key = ?2 AND topic_id = ?3",
        params![bot_id, chat_key, topic_id],
        |row| {
            Ok(SimDirectMessagesTopicRecord {
                topic_id: row.get(0)?,
                user_id: row.get(1)?,
                created_at: row.get(2)?,
                updated_at: row.get(3)?,
                last_message_id: row.get(4)?,
                last_message_date: row.get(5)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn upsert_direct_messages_topic(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    topic_id: i64,
    user_id: i64,
    last_message_id: Option<i64>,
    last_message_date: Option<i64>,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_direct_message_topics
         (bot_id, chat_key, topic_id, user_id, created_at, updated_at, last_message_id, last_message_date)
         VALUES (?1, ?2, ?3, ?4, ?5, ?5, ?6, ?7)
         ON CONFLICT(bot_id, chat_key, topic_id)
         DO UPDATE SET
            user_id = excluded.user_id,
            updated_at = excluded.updated_at,
            last_message_id = COALESCE(excluded.last_message_id, sim_direct_message_topics.last_message_id),
            last_message_date = COALESCE(excluded.last_message_date, sim_direct_message_topics.last_message_date)",
        params![
            bot_id,
            chat_key,
            topic_id,
            user_id,
            now,
            last_message_id,
            last_message_date,
        ],
    )
    .map_err(ApiError::internal)?;
    Ok(())
}

pub fn direct_messages_topic_value(
    conn: &mut rusqlite::Connection,
    user_id: i64,
    topic_id: i64,
) -> Result<Value, ApiError> {
    let user = users::load_sim_user_record(conn, user_id)?
        .map(|record| users::build_user_from_sim_record(&record, false));
    let topic = DirectMessagesTopic {
        topic_id,
        user,
    };
    serde_json::to_value(topic).map_err(ApiError::internal)
}

pub fn load_direct_messages_topics_for_chat_json(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Vec<Value>, ApiError> {
    let mut stmt = conn
        .prepare(
            "SELECT t.topic_id, t.user_id, t.updated_at, u.first_name, u.username
             FROM sim_direct_message_topics t
             LEFT JOIN users u ON u.id = t.user_id
             WHERE t.bot_id = ?1 AND t.chat_key = ?2
             ORDER BY t.updated_at DESC, t.topic_id ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot_id, chat_key], |row| {
            let topic_id: i64 = row.get(0)?;
            let user_id: i64 = row.get(1)?;
            let updated_at: i64 = row.get(2)?;
            let first_name: Option<String> = row.get(3)?;
            let username: Option<String> = row.get(4)?;
            let label = first_name
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
                .unwrap_or_else(|| {
                    username
                        .as_deref()
                        .map(str::trim)
                        .filter(|value| !value.is_empty())
                        .map(|value| format!("@{}", value))
                        .unwrap_or_else(|| format!("User {}", user_id))
                });

            Ok(json!({
                "topic_id": topic_id,
                "user_id": user_id,
                "name": label,
                "updated_at": updated_at,
            }))
        })
        .map_err(ApiError::internal)?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row.map_err(ApiError::internal)?);
    }

    Ok(result)
}

pub fn resolve_direct_messages_topic_for_sender(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    sender: &SimUserRecord,
    requested_topic_id: Option<i64>,
) -> Result<(i64, Value, Option<Chat>), ApiError> {
    let parent_channel_chat_id = sim_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
    let parent_channel_key = parent_channel_chat_id.to_string();
    let parent_channel_chat = chats::load_sim_chat_record(conn, bot_id, &parent_channel_key)?
        .ok_or_else(|| ApiError::not_found("parent channel not found"))?;

    let manager_allowed = chats::load_chat_member_record(conn, bot_id, &parent_channel_key, sender.id)?
        .map(|record| {
            if record.status == "owner" {
                true
            } else if record.status == "admin" {
                channels::channel_admin_has_direct_messages_permission(record.admin_rights_json.as_deref())
            } else {
                false
            }
        })
        .unwrap_or(false);

    if manager_allowed {
        let topic_id = requested_topic_id.unwrap_or(sender.id);
        if topic_id <= 0 {
            return Err(ApiError::bad_request("direct_messages_topic_id is invalid"));
        }

        let topic_record = load_direct_messages_topic_record(conn, bot_id, &sim_chat.chat_key, topic_id)?
            .ok_or_else(|| ApiError::not_found("direct messages topic not found"))?;
        if topic_record.user_id == sender.id {
            return Err(ApiError::bad_request(
                "direct-messages managers can't send messages to their own topic",
            ));
        }
        let topic_user_id = topic_record.user_id;

        let topic_value = direct_messages_topic_value(conn, topic_user_id, topic_id)?;
        return Ok((
            topic_id,
            topic_value,
            Some(groups::build_chat_from_group_record(&parent_channel_chat)),
        ));
    }

    let topic_id = requested_topic_id.unwrap_or(sender.id);
    if topic_id != sender.id {
        return Err(ApiError::bad_request(
            "only channel admins with direct-messages rights can select direct_messages_topic_id",
        ));
    }

    let existing = load_direct_messages_topic_record(conn, bot_id, &sim_chat.chat_key, topic_id)?;
    if let Some(record) = existing {
        if record.user_id != sender.id {
            return Err(ApiError::bad_request("direct messages topic does not belong to sender"));
        }
    } else {
        upsert_direct_messages_topic(conn, bot_id, &sim_chat.chat_key, topic_id, sender.id, None, None)?;
    }

    let topic_value = direct_messages_topic_value(conn, sender.id, topic_id)?;
    Ok((topic_id, topic_value, None))
}

pub fn direct_messages_chat_key(channel_chat_id: i64) -> String {
    format!("dm:{}", channel_chat_id)
}

pub fn direct_messages_chat_id_for_channel(channel_chat_id: i64) -> i64 {
    let channel_abs = channel_chat_id.saturating_abs();
    -(1_000_000_000_000_000i64.saturating_add(channel_abs))
}

pub fn handle_sim_open_channel_direct_messages(
    state: &Data<AppState>,
    token: &str,
    body: SimOpenChannelDirectMessagesRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let actor_user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let channel_chat_key = body.channel_chat_id.to_string();
    let channel_chat = chats::load_sim_chat_record(&mut conn, bot.id, &channel_chat_key)?
        .ok_or_else(|| ApiError::not_found("channel not found"))?;
    if channel_chat.chat_type != "channel" {
        return Err(ApiError::bad_request("channel_chat_id must point to a channel"));
    }
    if !channel_chat.direct_messages_enabled {
        return Err(ApiError::bad_request("channel direct messages are disabled"));
    }

    let channel_member = chats::load_chat_member_record(&mut conn, bot.id, &channel_chat_key, actor_user.id)?
        .ok_or_else(|| ApiError::bad_request("join channel first to open direct messages"))?;
    if !groups::is_active_chat_member_status(&channel_member.status) {
        return Err(ApiError::bad_request("join channel first to open direct messages"));
    }

    let can_manage_inbox = channel_member.status == "owner"
        || (
            channel_member.status == "admin"
                && channel_admin_has_direct_messages_permission(
                    channel_member.admin_rights_json.as_deref(),
                )
        );

    let dm_chat = ensure_channel_direct_messages_chat(&mut conn, bot.id, &channel_chat)?;
    let now = Utc::now().timestamp();
    chats::upsert_chat_member_record(
        &mut conn,
        bot.id,
        &dm_chat.chat_key,
        actor_user.id,
        if can_manage_inbox { "admin" } else { "member" },
        if can_manage_inbox { "admin" } else { "member" },
        Some(now),
        None,
        None,
        None,
        None,
        now,
    )?;

    if !can_manage_inbox {
        upsert_direct_messages_topic(
            &mut conn,
            bot.id,
            &dm_chat.chat_key,
            actor_user.id,
            actor_user.id,
            None,
            None,
        )?;
    }

    let dm_topics = load_direct_messages_topics_for_chat_json(&mut conn, bot.id, &dm_chat.chat_key)?;
    Ok(json!({
        "chat": groups::build_chat_from_group_record(&dm_chat),
        "parent_chat": groups::build_chat_from_group_record(&channel_chat),
        "topics": dm_topics,
    }))
}

// --- Sugested Posts ---

pub fn ensure_channel_member_can_approve_suggested_posts(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    channel_chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let Some(member) = chats::load_chat_member_record(conn, bot_id, channel_chat_key, user_id)? else {
        return Err(ApiError::bad_request(
            "only channel owner/admin with post rights can approve suggested posts",
        ));
    };

    if member.status == "owner" {
        return Ok(());
    }

    if member.status != "admin" {
        return Err(ApiError::bad_request(
            "only channel owner/admin with post rights can approve suggested posts",
        ));
    }

    let rights = channels::parse_channel_admin_rights_json(member.admin_rights_json.as_deref());
    if rights.can_post_messages {
        return Ok(());
    }

    Err(ApiError::bad_request(
        "not enough rights to approve suggested posts",
    ))
}

pub fn handle_auto_publish_due_suggested_posts(state: &Data<AppState>) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let mut conn = lock_db(state)?;
    channels::ensure_sim_suggested_posts_storage(&mut conn)?;

    let due_rows: Vec<(String, i64, String, i64)> = {
        let mut stmt = conn
            .prepare(
                "SELECT b.token, sp.bot_id, sp.chat_key, sp.message_id
                 FROM sim_suggested_posts sp
                 INNER JOIN bots b ON b.id = sp.bot_id
                 WHERE sp.state = 'approved'
                   AND COALESCE(sp.send_date, 0) <= ?1
                 ORDER BY sp.updated_at ASC
                 LIMIT 256",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![now], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected = Vec::new();
        for row in rows {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    for (token, bot_id, chat_key, message_id) in due_rows {
        let bot = ensure_bot(&mut conn, &token)?;
        if bot.id != bot_id {
            continue;
        }

        let Some(direct_messages_chat) = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
            continue;
        };
        if !is_direct_messages_chat(&direct_messages_chat) {
            continue;
        }

        let actor_user_id = direct_messages_chat
            .parent_channel_chat_id
            .and_then(|channel_chat_id| {
                load_channel_owner_user_id(&mut conn, bot.id, &channel_chat_id.to_string())
                    .ok()
                    .flatten()
            })
            .unwrap_or(bot.id);

        if let Err(error) = finalize_due_suggested_post_if_ready(
            state,
            &mut conn,
            &token,
            &bot,
            &direct_messages_chat,
            message_id,
            actor_user_id,
        ) {
            let failure_reason = match error.description.as_str() {
                "source message was not found" => Some("source_message_missing"),
                "suggested post message was not found" => Some("suggested_post_message_missing"),
                "message is not a suggested post" => Some("invalid_suggested_post_source"),
                _ => None,
            };

            if let Some(reason) = failure_reason {
                let existing_send_date = load_suggested_post_state(
                    &mut conn,
                    bot.id,
                    &direct_messages_chat.chat_key,
                    message_id,
                )?
                .and_then(|(_, send_date)| send_date)
                .or(Some(now));

                upsert_suggested_post_state(
                    &mut conn,
                    bot.id,
                    &direct_messages_chat.chat_key,
                    message_id,
                    "approval_failed",
                    existing_send_date,
                    Some(reason),
                    now,
                )?;
            }

            eprintln!(
                "auto-publish suggested post failed for bot {} message {}: {}",
                bot.id, message_id, error.description
            );
        }
    }

    Ok(())
}

pub fn settle_suggested_post_price_for_publication(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    proposer_user_id: i64,
    channel_owner_user_id: i64,
    price: Option<(String, i64)>,
) -> Result<Option<(String, i64, i64, i64)>, ApiError> {
    let Some((currency, gross_amount)) = price else {
        return Ok(None);
    };

    if gross_amount <= 0 {
        return Ok(None);
    }

    if currency != "XTR" {
        return Ok(Some((currency, gross_amount, gross_amount, 0)));
    }

    let now = Utc::now().timestamp();
    let proposer_connection =
        business::ensure_sim_business_connection_for_user(conn, bot_id, proposer_user_id)?;
    let owner_connection =
        business::ensure_sim_business_connection_for_user(conn, bot_id, channel_owner_user_id)?;

    if proposer_connection.star_balance < gross_amount {
        let top_up = gross_amount.saturating_sub(proposer_connection.star_balance);
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance + ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![
                top_up,
                now,
                bot_id,
                proposer_connection.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_auto_topup')",
            params![
                format!("suggested_post_topup_{}", generate_telegram_numeric_id()),
                bot_id,
                proposer_user_id,
                format!("suggested_post_topup_{}", generate_telegram_numeric_id()),
                top_up,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance - ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![
            gross_amount,
            now,
            bot_id,
            proposer_connection.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    let payout_amount = gross_amount.saturating_mul(80) / 100;
    let fee_amount = gross_amount.saturating_sub(payout_amount);

    if payout_amount > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance + ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![
                payout_amount,
                now,
                bot_id,
                owner_connection.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_payment')",
        params![
            format!("suggested_post_debit_{}", generate_telegram_numeric_id()),
            bot_id,
            proposer_user_id,
            format!("suggested_post_payment_{}", generate_telegram_numeric_id()),
            -gross_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if payout_amount > 0 {
        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'suggested_post_payout')",
            params![
                format!("suggested_post_credit_{}", generate_telegram_numeric_id()),
                bot_id,
                channel_owner_user_id,
                format!("suggested_post_payout_{}", generate_telegram_numeric_id()),
                payout_amount,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(Some((currency, gross_amount, payout_amount, fee_amount)))
}

pub fn publish_suggested_post_to_parent_channel(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    source_message_id: i64,
    actor_user_id: i64,
) -> Result<Value, ApiError> {
    let parent_channel_chat_id = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;

    with_request_actor_user_id(Some(actor_user_id), || {
        let source_message = messages::resolve_source_message_for_transport(
            conn,
            bot,
            &Value::String(direct_messages_chat.chat_key.clone()),
            source_message_id,
            false,
        )?;

        let send_kind = messages::send_kind_from_transport_source_message(&source_message);
        let (destination_chat_key, destination_chat) = chats::resolve_bot_outbound_chat(
            conn,
            bot.id,
            &Value::from(parent_channel_chat_id),
            send_kind,
        )?;
        let sender_user = users::resolve_transport_sender_user(
            conn,
            bot,
            &destination_chat_key,
            &destination_chat,
            send_kind,
        )?;

        let mut message_value = source_message;
        let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;
        let object = message_value
            .as_object_mut()
            .ok_or_else(|| ApiError::internal("suggested post payload is invalid"))?;

        object.remove("forward_origin");
        object.remove("is_automatic_forward");
        object.remove("reply_to_message");
        object.remove("edit_date");
        object.remove("views");
        object.remove("author_signature");
        object.remove("sender_chat");
        object.remove("message_thread_id");
        object.remove("is_topic_message");
        object.remove("direct_messages_topic");
        object.remove("business_connection_id");
        object.remove("paid_message_star_count");
        object.remove("suggested_post_info");
        object.remove("suggested_post_parameters");
        object.remove("suggested_post_approved");
        object.remove("suggested_post_approval_failed");
        object.remove("suggested_post_declined");
        object.remove("suggested_post_paid");
        object.remove("suggested_post_refunded");
        object.insert("from".to_string(), sender_user_value);

        messages::persist_transported_message(
            state,
            conn,
            token,
            bot,
            &destination_chat_key,
            &destination_chat,
            &sender_user,
            message_value,
            None,
        )
    })
}

pub fn finalize_due_suggested_post_if_ready(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    message_id: i64,
    actor_user_id: i64,
) -> Result<bool, ApiError> {
    let now = Utc::now().timestamp();
    let Some((current_state, send_date)) = load_suggested_post_state(
        conn,
        bot.id,
        &direct_messages_chat.chat_key,
        message_id,
    )?
    else {
        return Ok(false);
    };

    if current_state != "approved" {
        return Ok(false);
    }

    let effective_send_date = send_date.unwrap_or(now);
    if effective_send_date > now {
        return Ok(false);
    }

    let suggested_message = load_suggested_post_message_for_service(
        conn,
        bot,
        direct_messages_chat.chat_id,
        message_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let channel_owner_user_id = load_channel_owner_user_id(conn, bot.id, &parent_channel_chat_key)?
        .unwrap_or(actor_user_id);
    let proposer_user_id = suggested_message
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or(actor_user_id);

    let channel_post_message = publish_suggested_post_to_parent_channel(
        state,
        conn,
        token,
        bot,
        direct_messages_chat,
        message_id,
        actor_user_id,
    )?;

    let payment = settle_suggested_post_price_for_publication(
        conn,
        bot.id,
        proposer_user_id,
        channel_owner_user_id,
        extract_suggested_post_price_currency_amount(&suggested_message),
    )?;

    channels::upsert_suggested_post_state(
        conn,
        bot.id,
        &direct_messages_chat.chat_key,
        message_id,
        "paid",
        Some(effective_send_date),
        None,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        bot::build_bot_user(bot)
    } else {
        let actor_record = users::ensure_user(conn, Some(actor_user_id), None, None)?;
        users::build_user_from_sim_record(&actor_record, false)
    };

    let mut paid_payload = Map::<String, Value>::new();
    paid_payload.insert(
        "suggested_post_message".to_string(),
        suggested_message,
    );
    paid_payload.insert(
        "published_channel_post".to_string(),
        channel_post_message,
    );
    paid_payload.insert(
        "send_date".to_string(),
        Value::from(effective_send_date),
    );

    if let Some((currency, gross_amount, payout_amount, fee_amount)) = payment {
        paid_payload.insert("currency".to_string(), Value::String(currency));
        paid_payload.insert("amount".to_string(), Value::from(payout_amount));
        paid_payload.insert("gross_amount".to_string(), Value::from(gross_amount));
        paid_payload.insert("fee_amount".to_string(), Value::from(fee_amount));
        paid_payload.insert(
            "proposer_user_id".to_string(),
            Value::from(proposer_user_id),
        );
        paid_payload.insert(
            "channel_owner_user_id".to_string(),
            Value::from(channel_owner_user_id),
        );
    }

    let mut paid_service_fields = Map::<String, Value>::new();
    paid_service_fields.insert(
        "suggested_post_paid".to_string(),
        Value::Object(paid_payload),
    );
    let direct_messages_chat_obj = groups::build_chat_from_group_record(direct_messages_chat);
    messages::emit_service_message_update(
        state,
        conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} published a suggested post",
            messages::display_name_for_service_user(&actor)
        ),
        paid_service_fields,
    )?;

    Ok(true)
}

pub fn emit_suggested_post_refunded_updates_before_delete(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat: &SimChatRecord,
    message_ids: &[i64],
) -> Result<(), ApiError> {
    if message_ids.is_empty() {
        return Ok(());
    }

    ensure_sim_suggested_posts_storage(conn)?;

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    let actor = if actor_user_id == bot.id {
        bot::build_bot_user(bot)
    } else {
        let actor_record = users::ensure_user(conn, Some(actor_user_id), None, None)?;
        users::build_user_from_sim_record(&actor_record, false)
    };

    let now = Utc::now().timestamp();
    let direct_messages_chat_obj = groups::build_chat_from_group_record(direct_messages_chat);

    for message_id in message_ids {
        let Some((current_state, send_date)) = channels::load_suggested_post_state(
            conn,
            bot.id,
            &direct_messages_chat.chat_key,
            *message_id,
        )?
        else {
            continue;
        };

        if current_state != "paid" {
            continue;
        }

        let Ok(suggested_message) = load_suggested_post_message_for_service(
            conn,
            bot,
            direct_messages_chat.chat_id,
            *message_id,
        )
        else {
            continue;
        };

        upsert_suggested_post_state(
            conn,
            bot.id,
            &direct_messages_chat.chat_key,
            *message_id,
            "refunded",
            send_date,
            Some("deleted_message"),
            now,
        )?;

        let mut refunded_payload = Map::<String, Value>::new();
        refunded_payload.insert("suggested_post_message".to_string(), suggested_message);
        refunded_payload.insert("reason".to_string(), Value::String("deleted_message".to_string()));

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert(
            "suggested_post_refunded".to_string(),
            Value::Object(refunded_payload),
        );
        messages::emit_service_message_update(
            state,
            conn,
            token,
            bot.id,
            &direct_messages_chat.chat_key,
            &direct_messages_chat_obj,
            &actor,
            now,
            format!(
                "{} refunded a suggested post",
                messages::display_name_for_service_user(&actor)
            ),
            service_fields,
        )?;
    }

    Ok(())
}

pub fn load_suggested_post_message_for_service(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    direct_messages_chat_id: i64,
    message_id: i64,
) -> Result<Value, ApiError> {
    let message_value = messages::load_message_value(conn, bot, message_id)?;
    let belongs_to_chat = message_value
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(Value::as_i64)
        .map(|chat_id| chat_id == direct_messages_chat_id)
        .unwrap_or(false);

    if !belongs_to_chat {
        return Err(ApiError::bad_request("suggested post message was not found"));
    }

    let is_suggested_post = message_value
        .get("suggested_post_info")
        .is_some()
        || message_value
            .get("suggested_post_parameters")
            .is_some();
    if !is_suggested_post {
        return Err(ApiError::bad_request("message is not a suggested post"));
    }

    Ok(message_value)
}

pub fn extract_suggested_post_price_from_message(message_value: &Value) -> Option<Value> {
    let info_price = message_value
        .get("suggested_post_info")
        .and_then(|info| info.get("price"));
    if info_price.is_some() {
        return info_price.cloned();
    }

    message_value
        .get("suggested_post_parameters")
        .and_then(|params| params.get("price"))
        .cloned()
}

pub fn extract_suggested_post_send_date_from_message(message_value: &Value) -> Option<i64> {
    let info_send_date = message_value
        .get("suggested_post_info")
        .and_then(|info| info.get("send_date"))
        .and_then(Value::as_i64);
    if info_send_date.is_some() {
        return info_send_date;
    }

    message_value
        .get("suggested_post_parameters")
        .and_then(|params| params.get("send_date"))
        .and_then(Value::as_i64)
}

pub fn extract_suggested_post_price_currency_amount(
    message_value: &Value,
) -> Option<(String, i64)> {
    let price = extract_suggested_post_price_from_message(message_value)?;
    let currency = price
        .get("currency")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())?
        .to_ascii_uppercase();
    let amount = price.get("amount").and_then(Value::as_i64)?;
    Some((currency, amount))
}

pub fn ensure_sim_suggested_posts_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_suggested_posts (
            bot_id INTEGER NOT NULL,
            chat_key TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            state TEXT NOT NULL,
            send_date INTEGER,
            comment TEXT,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (bot_id, chat_key, message_id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn load_suggested_post_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<Option<(String, Option<i64>)>, ApiError> {
    conn.query_row(
        "SELECT state, send_date
         FROM sim_suggested_posts
         WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
        params![bot_id, chat_key, message_id],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn upsert_suggested_post_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    state: &str,
    send_date: Option<i64>,
    comment: Option<&str>,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_suggested_posts
         (bot_id, chat_key, message_id, state, send_date, comment, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(bot_id, chat_key, message_id)
         DO UPDATE SET
            state = excluded.state,
            send_date = excluded.send_date,
            comment = excluded.comment,
            updated_at = excluded.updated_at",
        params![
            bot_id,
            chat_key,
            message_id,
            state,
            send_date,
            comment,
            updated_at,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}