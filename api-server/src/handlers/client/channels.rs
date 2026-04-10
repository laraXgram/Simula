use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{Map, Value};

use crate::database::AppState;

use crate::types::ApiError;
use crate::handlers::client::types::channels::{LinkedDiscussionTransportContext, ChannelAdminRights};
use crate::handlers::client::types::chats::SimChatRecord;
use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id, with_request_actor_user_id};

use super::{chats, messages, users};

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

// --- Direct Messages ---

pub fn is_direct_messages_chat(sim_chat: &SimChatRecord) -> bool {
    sim_chat.is_direct_messages && sim_chat.parent_channel_chat_id.is_some()
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
