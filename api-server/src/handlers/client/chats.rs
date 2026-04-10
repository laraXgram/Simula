use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::Value;

use crate::database::ensure_chat;

use crate::types::ApiError;
use crate::handlers::client::types::users::SimUserRecord;
use crate::handlers::client::types::chats::{SimChatRecord, SimChatMemberRecord};

use crate::generated::types::{Chat, ChatPermissions};

use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id};

use super::{bot, channels, groups, users};

#[derive(Debug, Clone, Copy)]
pub enum ChatSendKind {
    Text,
    Photo,
    Video,
    Audio,
    Voice,
    Document,
    VideoNote,
    Poll,
    Invoice,
    Other,
}

pub fn chat_id_as_i64(chat_id: &Value, chat_key: &str) -> i64 {
    match chat_id {
        Value::Number(n) => n.as_i64().unwrap_or_else(|| fallback_chat_id(chat_key)),
        Value::String(s) => s
            .parse::<i64>()
            .unwrap_or_else(|_| fallback_chat_id(s)),
        _ => fallback_chat_id(chat_key),
    }
}

pub fn fallback_chat_id(input: &str) -> i64 {
    let mut acc: i64 = 0;
    for b in input.as_bytes() {
        acc = acc.wrapping_mul(31).wrapping_add(*b as i64);
    }
    -acc.abs().max(1)
}

pub fn permission_enabled(flag: Option<bool>, fallback: bool) -> bool {
    flag.unwrap_or(fallback)
}

pub fn is_send_kind_allowed_by_permissions(permissions: &ChatPermissions, send_kind: ChatSendKind) -> bool {
    if !permission_enabled(permissions.can_send_messages, true) {
        return false;
    }

    match send_kind {
        ChatSendKind::Text => true,
        ChatSendKind::Photo => permission_enabled(permissions.can_send_photos, true),
        ChatSendKind::Video => permission_enabled(permissions.can_send_videos, true),
        ChatSendKind::Audio => permission_enabled(permissions.can_send_audios, true),
        ChatSendKind::Voice => permission_enabled(permissions.can_send_voice_notes, true),
        ChatSendKind::Document => permission_enabled(permissions.can_send_documents, true),
        ChatSendKind::VideoNote => permission_enabled(permissions.can_send_video_notes, true),
        ChatSendKind::Poll => permission_enabled(permissions.can_send_polls, true),
        ChatSendKind::Invoice | ChatSendKind::Other => permission_enabled(permissions.can_send_other_messages, true),
    }
}

pub fn send_kind_label(send_kind: ChatSendKind) -> &'static str {
    match send_kind {
        ChatSendKind::Text => "messages",
        ChatSendKind::Photo => "photos",
        ChatSendKind::Video => "videos",
        ChatSendKind::Audio => "audio messages",
        ChatSendKind::Voice => "voice messages",
        ChatSendKind::Document => "documents",
        ChatSendKind::VideoNote => "video notes",
        ChatSendKind::Poll => "polls",
        ChatSendKind::Invoice | ChatSendKind::Other => "this type of messages",
    }
}

pub fn load_sim_chat_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<SimChatRecord>, ApiError> {
    conn.query_row(
        "SELECT chat_id, chat_type, title, username, is_forum, is_direct_messages, parent_channel_chat_id, direct_messages_enabled, direct_messages_star_count, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id
         FROM sim_chats
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| {
            Ok(SimChatRecord {
                chat_key: chat_key.to_string(),
                chat_id: row.get(0)?,
                chat_type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                is_forum: row.get::<_, i64>(4)? == 1,
                is_direct_messages: row.get::<_, i64>(5)? == 1,
                parent_channel_chat_id: row.get(6)?,
                direct_messages_enabled: row.get::<_, i64>(7)? == 1,
                direct_messages_star_count: row.get::<_, i64>(8)?,
                channel_show_author_signature: row.get::<_, i64>(9)? == 1,
                channel_paid_reactions_enabled: row.get::<_, i64>(10)? == 1,
                linked_discussion_chat_id: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn load_chat_member_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<Option<SimChatMemberRecord>, ApiError> {
    conn.query_row(
        "SELECT status, role, permissions_json, admin_rights_json, until_date, custom_title, tag, joined_at
         FROM sim_chat_members
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
        |row| {
            Ok(SimChatMemberRecord {
                status: row.get(0)?,
                role: row.get(1)?,
                permissions_json: row.get(2)?,
                admin_rights_json: row.get(3)?,
                until_date: row.get(4)?,
                custom_title: row.get(5)?,
                tag: row.get(6)?,
                joined_at: row.get(7)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn load_sim_chat_record_by_chat_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
) -> Result<Option<SimChatRecord>, ApiError> {
    conn.query_row(
        "SELECT chat_key, chat_type, title, username, is_forum, is_direct_messages, parent_channel_chat_id, direct_messages_enabled, direct_messages_star_count, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id
         FROM sim_chats
         WHERE bot_id = ?1 AND chat_id = ?2
         ORDER BY updated_at DESC
         LIMIT 1",
        params![bot_id, chat_id],
        |row| {
            Ok(SimChatRecord {
                chat_key: row.get(0)?,
                chat_id,
                chat_type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                is_forum: row.get::<_, i64>(4)? == 1,
                is_direct_messages: row.get::<_, i64>(5)? == 1,
                parent_channel_chat_id: row.get(6)?,
                direct_messages_enabled: row.get::<_, i64>(7)? == 1,
                direct_messages_star_count: row.get::<_, i64>(8)?,
                channel_show_author_signature: row.get::<_, i64>(9)? == 1,
                channel_paid_reactions_enabled: row.get::<_, i64>(10)? == 1,
                linked_discussion_chat_id: row.get(11)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn ensure_sender_is_chat_member(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot_id, chat_key, user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(member_status) = status else {
        return Err(ApiError::bad_request("user is not a member of this chat"));
    };

    if !groups::is_active_chat_member_status(member_status.as_str()) {
        return Err(ApiError::bad_request("user is not allowed to send messages in this chat"));
    }

    Ok(())
}

pub fn ensure_sender_can_send_in_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    send_kind: ChatSendKind,
) -> Result<(), ApiError> {
    let Some(mut member_record) = load_chat_member_record(conn, bot_id, chat_key, user_id)? else {
        return Err(ApiError::bad_request("user is not a member of this chat"));
    };

    if member_record.status == "restricted" {
        let now = Utc::now().timestamp();
        if let Some(until_date) = member_record.until_date {
            if until_date > 0 && until_date <= now {
                upsert_chat_member_record(
                    conn,
                    bot_id,
                    chat_key,
                    user_id,
                    "member",
                    "member",
                    member_record.joined_at.or(Some(now)),
                    None,
                    None,
                    None,
                    member_record.tag.as_deref(),
                    now,
                )?;
                member_record.status = "member".to_string();
                member_record.role = "member".to_string();
                member_record.permissions_json = None;
                member_record.until_date = None;
                member_record.custom_title = None;
            }
        }
    }

    let member_status = member_record.status.as_str();

    if !groups::is_active_chat_member_status(member_status) {
        return Err(ApiError::bad_request("user is not allowed to send messages in this chat"));
    }

    let Some(settings) = groups::load_group_runtime_settings(conn, bot_id, chat_key)? else {
        return Ok(());
    };

    if settings.chat_type == "channel" {
        if member_status == "owner" {
            return Ok(());
        }

        if member_status == "admin" {
            let rights = channels::parse_channel_admin_rights_json(member_record.admin_rights_json.as_deref());
            if channels::channel_admin_can_publish(&rights) {
                return Ok(());
            }
            return Err(ApiError::bad_request(
                "not enough rights to publish messages in this channel",
            ));
        }

        return Err(ApiError::bad_request(
            "only channel owner/admin can publish messages",
        ));
    }

    if groups::is_group_admin_or_owner_status(member_status) {
        return Ok(());
    }

    let effective_permissions = if member_status == "restricted" {
        member_record
            .permissions_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
            .unwrap_or_else(groups::default_group_permissions)
    } else {
        settings.permissions.clone()
    };

    if !is_send_kind_allowed_by_permissions(&effective_permissions, send_kind) {
        return Err(ApiError::bad_request(format!(
            "not enough rights to send {} to the chat",
            send_kind_label(send_kind),
        )));
    }

    if settings.slow_mode_delay > 0 {
        let now = Utc::now().timestamp();
        let last_message_date: Option<i64> = conn
            .query_row(
                "SELECT date
                 FROM messages
                 WHERE bot_id = ?1 AND chat_key = ?2 AND from_user_id = ?3
                 ORDER BY date DESC, message_id DESC
                 LIMIT 1",
                params![bot_id, chat_key, user_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(last_date) = last_message_date {
            let remaining = (last_date + settings.slow_mode_delay) - now;
            if remaining > 0 {
                return Err(ApiError::bad_request(format!(
                    "Too Many Requests: retry after {}",
                    remaining,
                )));
            }
        }
    }

    Ok(())
}

pub fn chat_from_sim_record(record: &SimChatRecord, user: &SimUserRecord) -> Chat {
    if record.chat_type == "private" {
        Chat {
            id: record.chat_id,
            r#type: "private".to_string(),
            title: None,
            username: user.username.clone(),
            first_name: Some(user.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
        }
    } else {
        Chat {
            id: record.chat_id,
            r#type: record.chat_type.clone(),
            title: record.title.clone(),
            username: record.username.clone(),
            first_name: None,
            last_name: None,
            is_forum: if record.chat_type == "supergroup" {
                Some(record.is_forum)
            } else {
                None
            },
            is_direct_messages: if record.is_direct_messages { Some(true) } else { None },
        }
    }
}

pub fn load_chat_member_status(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<Option<String>, ApiError> {
    conn.query_row(
        "SELECT status FROM sim_chat_members WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn upsert_chat_member_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    status: &str,
    role: &str,
    joined_at: Option<i64>,
    permissions_json: Option<&str>,
    until_date: Option<i64>,
    custom_title: Option<&str>,
    tag: Option<&str>,
    updated_at: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO sim_chat_members
         (bot_id, chat_key, user_id, status, role, permissions_json, until_date, custom_title, tag, joined_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
         ON CONFLICT(bot_id, chat_key, user_id)
         DO UPDATE SET
            status = excluded.status,
            role = excluded.role,
            permissions_json = excluded.permissions_json,
            until_date = excluded.until_date,
            custom_title = excluded.custom_title,
            tag = excluded.tag,
            joined_at = COALESCE(excluded.joined_at, sim_chat_members.joined_at),
            updated_at = excluded.updated_at",
        params![
            bot_id,
            chat_key,
            user_id,
            status,
            role,
            permissions_json,
            until_date,
            custom_title,
            tag,
            joined_at,
            updated_at,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn resolve_bot_outbound_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id_value: &Value,
    send_kind: ChatSendKind,
) -> Result<(String, Chat), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id_value)?;
    ensure_chat(conn, &requested_chat_key)?;
    let chat_id = chat_id_as_i64(chat_id_value, &requested_chat_key);

    let sim_chat = load_sim_chat_record(conn, bot_id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)?);

    if let Some(sim_chat) = sim_chat {
        let sim_chat_key = sim_chat.chat_key.clone();
        if sim_chat.chat_type != "private" {
            let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
            let outbound_sender_user_id = if sim_chat.chat_type == "channel" {
                actor_user_id
            } else {
                bot_id
            };

            if channels::is_direct_messages_chat(&sim_chat) {
                let parent_channel_chat_id = sim_chat
                    .parent_channel_chat_id
                    .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?;
                channels::ensure_channel_member_can_manage_direct_messages(
                    conn,
                    bot_id,
                    &parent_channel_chat_id.to_string(),
                    actor_user_id,
                )?;
                let _ = channels::direct_messages_star_count_for_chat(conn, bot_id, &sim_chat)?;
            } else if sim_chat.chat_type == "channel" {
                if actor_user_id == bot_id {
                    bot::ensure_bot_is_chat_admin_or_owner(conn, bot_id, &sim_chat_key)?;
                }
                channels::ensure_request_actor_can_publish_to_channel(conn, bot_id, &sim_chat_key)?;
            }
            ensure_sender_can_send_in_chat(
                conn,
                bot_id,
                &sim_chat_key,
                outbound_sender_user_id,
                send_kind,
            )?;
            let is_supergroup = sim_chat.chat_type == "supergroup";
            return Ok((
                sim_chat_key,
                Chat {
                    id: sim_chat.chat_id,
                    r#type: sim_chat.chat_type,
                    title: sim_chat.title,
                    username: sim_chat.username,
                    first_name: None,
                    last_name: None,
                    is_forum: if is_supergroup && !sim_chat.is_direct_messages {
                        Some(sim_chat.is_forum)
                    } else {
                        None
                    },
                    is_direct_messages: if sim_chat.is_direct_messages {
                        Some(true)
                    } else {
                        None
                    },
                },
            ));
        }

        let recipient = users::load_sim_user_record(conn, sim_chat.chat_id)?;
        return Ok((
            sim_chat_key,
            Chat {
                id: sim_chat.chat_id,
                r#type: "private".to_string(),
                title: None,
                username: recipient.as_ref().and_then(|user| user.username.clone()),
                first_name: recipient.as_ref().map(|user| user.first_name.clone()),
                last_name: None,
                is_forum: None,
                is_direct_messages: None,
            },
        ));
    }

    let recipient = users::load_sim_user_record(conn, chat_id)?;
    Ok((
        requested_chat_key,
        Chat {
            id: chat_id,
            r#type: "private".to_string(),
            title: None,
            username: recipient.as_ref().and_then(|user| user.username.clone()),
            first_name: recipient.as_ref().map(|user| user.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        },
    ))
}

