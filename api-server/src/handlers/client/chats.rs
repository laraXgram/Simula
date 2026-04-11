use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use serde::Serialize;
use std::collections::HashSet;

use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::types::{
    Chat, ChatPermissions, User, ChatMember, ChatMemberAdministrator, ChatMemberOwner,
    ChatMemberMember, ChatMemberRestricted, ChatMemberBanned, ChatMemberLeft, ChatMemberUpdated,
    Update, ChatInviteLink,
};

use crate::handlers::{ensure_default_user, generate_telegram_numeric_id};
use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id};
use crate::handlers::utils::storage::ensure_sim_verifications_storage;

use super::types::users::{SimUserRecord, SimAddUserChatBoostsRequest, SimRemoveUserChatBoostsRequest};
use super::types::chats::{SimChatRecord, SimChatMemberRecord};
use super::types::channels::{ChannelAdminRights, SimResolveJoinRequestRequest};
use super::types::groups::SimChatInviteLinkRecord;

use super::{bot, channels, groups, users, webhook};

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

pub fn resolve_chat_key_and_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: &Value,
) -> Result<(String, i64), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id)?;
    let requested_chat_id = chat_id_as_i64(chat_id, &requested_chat_key);

    if let Some(sim_chat) = load_sim_chat_record(conn, bot_id, &requested_chat_key)? {
        return Ok((sim_chat.chat_key, sim_chat.chat_id));
    }

    if let Some(sim_chat) = load_sim_chat_record_by_chat_id(conn, bot_id, requested_chat_id)? {
        return Ok((sim_chat.chat_key, sim_chat.chat_id));
    }

    Ok((requested_chat_key, requested_chat_id))
}

pub fn generate_sim_invite_link() -> String {
    let code = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    let compact = code.chars().take(22).collect::<String>();
    format!("https://t.me/+{}", compact)
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

pub fn is_supported_chat_action(action: &str) -> bool {
    matches!(
        action,
        "typing"
            | "upload_photo"
            | "record_video"
            | "upload_video"
            | "record_voice"
            | "upload_voice"
            | "upload_document"
            | "choose_sticker"
            | "find_location"
            | "record_video_note"
            | "upload_video_note"
    )
}

pub fn resolve_sender_for_bot_outbound_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    send_kind: ChatSendKind,
) -> Result<User, ApiError> {
    if destination_chat.r#type == "channel" {
        return users::resolve_transport_sender_user(
            conn,
            bot,
            destination_chat_key,
            destination_chat,
            send_kind,
        );
    }

    Ok(bot::build_bot_user(bot))
}

pub fn normalize_verification_custom_description(
    value: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let normalized = value
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);

    if let Some(text) = normalized.as_deref() {
        if text.chars().count() > 70 {
            return Err(ApiError::bad_request(
                "custom_description must be at most 70 characters",
            ));
        }
    }

    Ok(normalized)
}

pub fn load_chat_verification_description(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<String>, ApiError> {
    conn.query_row(
        "SELECT custom_description
         FROM sim_chat_verifications
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| row.get(0),
    )
    .optional()
    .map_err(ApiError::internal)
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

pub fn load_latest_pinned_message_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<i64>, ApiError> {
    conn
        .query_row(
            "SELECT message_id
             FROM sim_chat_pinned_messages
             WHERE bot_id = ?1 AND chat_key = ?2
             ORDER BY pinned_at DESC, message_id DESC
             LIMIT 1",
            params![bot_id, chat_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)
}

pub fn sync_latest_pinned_message_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    updated_at: i64,
) -> Result<Option<i64>, ApiError> {
    let latest = load_latest_pinned_message_id(conn, bot_id, chat_key)?;
    conn.execute(
        "UPDATE sim_chats
         SET pinned_message_id = ?1, updated_at = ?2
         WHERE bot_id = ?3 AND chat_key = ?4",
        params![latest, updated_at, bot_id, chat_key],
    )
    .map_err(ApiError::internal)?;
    Ok(latest)
}

pub fn resolve_forum_supergroup_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_id: &Value,
) -> Result<(String, SimChatRecord, Chat, User), ApiError> {
    let (chat_key, sim_chat) = resolve_non_private_sim_chat(conn, bot.id, chat_id)?;
    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        return Err(ApiError::bad_request(
            "forum topics are available only in forum supergroups",
        ));
    }

    let actor = resolve_chat_admin_actor(conn, bot, &chat_key)?;
    let chat = groups::build_chat_from_group_record(&sim_chat);
    Ok((chat_key, sim_chat, chat, actor))
}

pub fn resolve_non_private_sim_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: &Value,
) -> Result<(String, SimChatRecord), ApiError> {
    let requested_chat_key = value_to_chat_key(chat_id)?;
    let requested_chat_id = chat_id_as_i64(chat_id, &requested_chat_key);
    let Some(sim_chat) = load_sim_chat_record(conn, bot_id, &requested_chat_key)?
        .or(load_sim_chat_record_by_chat_id(conn, bot_id, requested_chat_id)?) else {
        return Err(ApiError::not_found("chat not found"));
    };
    if sim_chat.chat_type == "private" {
        return Err(ApiError::bad_request(
            "chat must be a group, supergroup or channel",
        ));
    }

    Ok((sim_chat.chat_key.clone(), sim_chat))
}

pub fn resolve_chat_admin_actor(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
) -> Result<User, ApiError> {
    if let Some(actor_user_id) = current_request_actor_user_id() {
        if actor_user_id == bot.id {
            bot::ensure_bot_is_chat_admin_or_owner(conn, bot.id, chat_key)?;
            return Ok(bot::build_bot_user(bot));
        }

        let actor_status = load_chat_member_status(conn, bot.id, chat_key, actor_user_id)?;
        if !actor_status
            .as_deref()
            .map(groups::is_group_admin_or_owner_status)
            .unwrap_or(false)
        {
            return Err(ApiError::bad_request("not enough rights to manage chat"));
        }

        let actor_record = users::ensure_sim_user_record(conn, actor_user_id)?;
        return Ok(users::build_user_from_sim_record(&actor_record, false));
    }

    bot::ensure_bot_is_chat_admin_or_owner(conn, bot.id, chat_key)?;
    Ok(bot::build_bot_user(bot))
}

pub fn ensure_request_actor_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
) -> Result<(), ApiError> {
    let _ = resolve_chat_admin_actor(conn, bot, chat_key)?;
    Ok(())
}

pub fn to_chat_member<T: Serialize>(member: T) -> Result<ChatMember, ApiError> {
    let value = serde_json::to_value(member).map_err(ApiError::internal)?;
    serde_json::from_value(value).map_err(ApiError::internal)
}

pub fn chat_member_from_status(status: &str, user: &User) -> Result<ChatMember, ApiError> {
    chat_member_from_status_with_details(status, user, None, None, None, None, None, None)
}

pub fn chat_member_from_status_with_details(
    status: &str,
    user: &User,
    tag: Option<String>,
    custom_title: Option<String>,
    permissions: Option<ChatPermissions>,
    until_date: Option<i64>,
    chat_type: Option<&str>,
    channel_admin_rights: Option<ChannelAdminRights>,
) -> Result<ChatMember, ApiError> {
    match status {
        "owner" => to_chat_member(ChatMemberOwner {
            status: "creator".to_string(),
            user: user.clone(),
            is_anonymous: false,
            custom_title,
        }),
        "admin" => {
            let is_channel = chat_type == Some("channel");
            let rights = channel_admin_rights.unwrap_or_else(channels::channel_admin_rights_full_access);
            to_chat_member(ChatMemberAdministrator {
                status: "administrator".to_string(),
                user: user.clone(),
                can_be_edited: false,
                is_anonymous: false,
                can_manage_chat: if is_channel { rights.can_manage_chat } else { true },
                can_delete_messages: if is_channel { rights.can_delete_messages } else { true },
                can_manage_video_chats: true,
                can_restrict_members: if is_channel { false } else { true },
                can_promote_members: false,
                can_change_info: if is_channel { rights.can_change_info } else { true },
                can_invite_users: if is_channel { rights.can_invite_users } else { true },
                can_post_stories: true,
                can_edit_stories: true,
                can_delete_stories: true,
                can_post_messages: if is_channel { Some(rights.can_post_messages) } else { None },
                can_edit_messages: if is_channel { Some(rights.can_edit_messages) } else { None },
                can_pin_messages: if is_channel { None } else { Some(true) },
                can_manage_topics: if is_channel { None } else { Some(false) },
                can_manage_direct_messages: if is_channel {
                    Some(rights.can_manage_direct_messages)
                } else {
                    None
                },
                can_manage_tags: None,
                custom_title: if is_channel { None } else { custom_title },
            })
        }
        "member" => to_chat_member(ChatMemberMember {
            status: "member".to_string(),
            tag,
            user: user.clone(),
            until_date,
        }),
        "restricted" => {
            let effective_permissions = permissions.unwrap_or_else(groups::default_group_permissions);
            let restricted_until = until_date.unwrap_or_else(|| Utc::now().timestamp() + 3600);
            to_chat_member(ChatMemberRestricted {
                status: "restricted".to_string(),
                tag,
                user: user.clone(),
                is_member: true,
                can_send_messages: permission_enabled(effective_permissions.can_send_messages, false),
                can_send_audios: permission_enabled(effective_permissions.can_send_audios, false),
                can_send_documents: permission_enabled(effective_permissions.can_send_documents, false),
                can_send_photos: permission_enabled(effective_permissions.can_send_photos, false),
                can_send_videos: permission_enabled(effective_permissions.can_send_videos, false),
                can_send_video_notes: permission_enabled(effective_permissions.can_send_video_notes, false),
                can_send_voice_notes: permission_enabled(effective_permissions.can_send_voice_notes, false),
                can_send_polls: permission_enabled(effective_permissions.can_send_polls, false),
                can_send_other_messages: permission_enabled(effective_permissions.can_send_other_messages, false),
                can_add_web_page_previews: permission_enabled(effective_permissions.can_add_web_page_previews, false),
                can_edit_tag: permission_enabled(effective_permissions.can_edit_tag, false),
                can_change_info: permission_enabled(effective_permissions.can_change_info, false),
                can_invite_users: permission_enabled(effective_permissions.can_invite_users, false),
                can_pin_messages: permission_enabled(effective_permissions.can_pin_messages, false),
                can_manage_topics: permission_enabled(effective_permissions.can_manage_topics, false),
                until_date: restricted_until,
            })
        }
        "banned" => to_chat_member(ChatMemberBanned {
            status: "kicked".to_string(),
            user: user.clone(),
            until_date: until_date.unwrap_or(0),
        }),
        _ => to_chat_member(ChatMemberLeft {
            status: "left".to_string(),
            user: user.clone(),
        }),
    }
}

pub fn chat_member_from_record(
    record: &SimChatMemberRecord,
    user: &User,
    chat_type: &str,
) -> Result<ChatMember, ApiError> {
    let permissions = record
        .permissions_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok());
    let is_channel = chat_type == "channel";
    let channel_admin_rights = if is_channel && record.status == "admin" {
        Some(channels::parse_channel_admin_rights_json(record.admin_rights_json.as_deref()))
    } else {
        None
    };

    chat_member_from_status_with_details(
        record.status.as_str(),
        user,
        if is_channel { None } else { record.tag.clone() },
        if is_channel { None } else { record.custom_title.clone() },
        permissions,
        record.until_date,
        Some(chat_type),
        channel_admin_rights,
    )
}

pub fn ensure_request_actor_can_manage_sender_chat_in_linked_context(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
    sim_chat: &SimChatRecord,
) -> Result<(), ApiError> {
    if ensure_request_actor_is_chat_admin_or_owner(conn, bot, chat_key).is_ok() {
        return Ok(());
    }

    if sim_chat.chat_type != "group" && sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request("not enough rights to manage chat"));
    }

    let linked_channel_chat_key: Option<String> = conn
        .query_row(
            "SELECT chat_key
             FROM sim_chats
             WHERE bot_id = ?1
               AND chat_type = 'channel'
               AND linked_discussion_chat_id = ?2
             ORDER BY updated_at DESC
             LIMIT 1",
            params![bot.id, sim_chat.chat_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some(channel_chat_key) = linked_channel_chat_key {
        if ensure_request_actor_is_chat_admin_or_owner(conn, bot, &channel_chat_key).is_ok() {
            return Ok(());
        }
    }

    Err(ApiError::bad_request("not enough rights to manage chat"))
}

pub fn emit_chat_member_transition_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    target: &User,
    old_status: &str,
    new_status: &str,
    date: i64,
) -> Result<(), ApiError> {
    emit_chat_member_transition_update_with_records(
        state,
        conn,
        token,
        bot_id,
        chat,
        actor,
        target,
        old_status,
        new_status,
        None,
        None,
        date,
    )
}

pub fn emit_chat_member_transition_update_with_records(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    target: &User,
    old_status: &str,
    new_status: &str,
    old_record: Option<&SimChatMemberRecord>,
    new_record: Option<&SimChatMemberRecord>,
    date: i64,
) -> Result<(), ApiError> {
    let old_chat_member = if let Some(record) = old_record {
        chat_member_from_record(record, target, chat.r#type.as_str())?
    } else {
        chat_member_from_status(old_status, target)?
    };
    let new_chat_member = if let Some(record) = new_record {
        chat_member_from_record(record, target, chat.r#type.as_str())?
    } else {
        chat_member_from_status(new_status, target)?
    };

    let update = Update {
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
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    webhook::persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )
}

pub fn emit_my_chat_member_transition_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    old_status: &str,
    new_status: &str,
    date: i64,
) -> Result<(), ApiError> {
    emit_my_chat_member_transition_update_with_records(
        state,
        conn,
        token,
        bot_id,
        chat,
        actor,
        old_status,
        new_status,
        None,
        None,
        date,
    )
}

pub fn emit_my_chat_member_transition_update_with_records(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat: &Chat,
    actor: &User,
    old_status: &str,
    new_status: &str,
    old_record: Option<&SimChatMemberRecord>,
    new_record: Option<&SimChatMemberRecord>,
    date: i64,
) -> Result<(), ApiError> {
    let old_chat_member = if let Some(record) = old_record {
        chat_member_from_record(record, actor, chat.r#type.as_str())?
    } else {
        chat_member_from_status(old_status, actor)?
    };
    let new_chat_member = if let Some(record) = new_record {
        chat_member_from_record(record, actor, chat.r#type.as_str())?
    } else {
        chat_member_from_status(new_status, actor)?
    };

    let update = Update {
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
        callback_query: None,
        shipping_query: None,
        pre_checkout_query: None,
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: Some(ChatMemberUpdated {
            chat: chat.clone(),
            from: actor.clone(),
            date,
            old_chat_member,
            new_chat_member,
            invite_link: None,
            via_join_request: None,
            via_chat_folder_invite_link: None,
        }),
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: None,
    };

    webhook::persist_and_dispatch_update(
        state,
        conn,
        token,
        bot_id,
        serde_json::to_value(update).map_err(ApiError::internal)?,
    )
}

pub fn resolve_linked_chat_id_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
) -> Result<Option<i64>, ApiError> {
    if sim_chat.chat_type == "channel" {
        return Ok(sim_chat.linked_discussion_chat_id);
    }

    if sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup" {
        let linked_channel_id: Option<i64> = conn
            .query_row(
                "SELECT chat_id
                 FROM sim_chats
                 WHERE bot_id = ?1
                   AND chat_type = 'channel'
                   AND linked_discussion_chat_id = ?2
                 ORDER BY updated_at DESC
                 LIMIT 1",
                params![bot_id, sim_chat.chat_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        return Ok(linked_channel_id);
    }

    Ok(None)
}

pub fn ensure_private_sim_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
    user: &SimUserRecord,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    ensure_chat(conn, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_chats
         (bot_id, chat_key, chat_id, chat_type, title, username, description, photo_file_id, is_forum, is_direct_messages, parent_channel_chat_id, message_history_visible, slow_mode_delay, permissions_json, owner_user_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, 'private', NULL, ?4, NULL, NULL, 0, 0, NULL, 1, 0, NULL, NULL, ?5, ?5)
         ON CONFLICT(bot_id, chat_key) DO UPDATE SET updated_at = excluded.updated_at",
        params![bot_id, chat_key, chat_id, user.username, now],
    )
    .map_err(ApiError::internal)?;

    Ok(SimChatRecord {
        chat_key,
        chat_id,
        chat_type: "private".to_string(),
        title: None,
        username: user.username.clone(),
        is_forum: false,
        is_direct_messages: false,
        parent_channel_chat_id: None,
        direct_messages_enabled: false,
        direct_messages_star_count: 0,
        channel_show_author_signature: false,
        channel_paid_reactions_enabled: false,
        linked_discussion_chat_id: None,
    })
}

pub fn resolve_sim_chat_for_user_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: i64,
    user: &SimUserRecord,
) -> Result<SimChatRecord, ApiError> {
    let chat_key = chat_id.to_string();
    if let Some(record) = load_sim_chat_record(conn, bot_id, &chat_key)? {
        return Ok(record);
    }
    if let Some(record) = load_sim_chat_record_by_chat_id(conn, bot_id, chat_id)? {
        return Ok(record);
    }
    ensure_private_sim_chat(conn, bot_id, chat_id, user)
}

pub fn ensure_sender_chat_not_banned(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    sender_chat_id: i64,
) -> Result<(), ApiError> {
    let banned: Option<i64> = conn
        .query_row(
            "SELECT 1
             FROM sim_banned_sender_chats
             WHERE bot_id = ?1 AND chat_key = ?2 AND sender_chat_id = ?3",
            params![bot_id, chat_key, sender_chat_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if banned.is_some() {
        return Err(ApiError::bad_request(
            "this sender chat is banned in the destination chat",
        ));
    }

    Ok(())
}

pub fn ensure_user_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
    error_message: &'static str,
) -> Result<(), ApiError> {
    let status = load_chat_member_status(conn, bot_id, chat_key, user_id)?;
    if status
        .as_deref()
        .map(groups::is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Ok(());
    }

    Err(ApiError::bad_request(error_message))
}

// --- Links ---

pub fn chat_invite_link_from_record(
    creator: User,
    record: &SimChatInviteLinkRecord,
    pending_join_request_count: Option<i64>,
) -> ChatInviteLink {
    ChatInviteLink {
        invite_link: record.invite_link.clone(),
        creator,
        creates_join_request: record.creates_join_request,
        is_primary: record.is_primary,
        is_revoked: record.is_revoked,
        name: record.name.clone(),
        expire_date: record.expire_date,
        member_limit: record.member_limit,
        pending_join_request_count,
        subscription_period: record.subscription_period,
        subscription_price: record.subscription_price,
    }
}

pub fn generate_unique_invite_link_for_bot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
) -> Result<String, ApiError> {
    let mut invite_link = generate_sim_invite_link();
    loop {
        let exists: Option<String> = conn
            .query_row(
                "SELECT invite_link FROM sim_chat_invite_links WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot_id, &invite_link],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            return Ok(invite_link);
        }
        invite_link = generate_sim_invite_link();
    }
}

pub fn pending_join_request_count_for_link(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    invite_link: &str,
) -> Result<i64, ApiError> {
    conn.query_row(
        "SELECT COUNT(*) FROM sim_chat_join_requests
         WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3 AND status = 'pending'",
        params![bot_id, chat_key, invite_link],
        |row| row.get(0),
    )
    .map_err(ApiError::internal)
}

pub fn load_invite_link_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    invite_link: &str,
) -> Result<Option<SimChatInviteLinkRecord>, ApiError> {
    conn.query_row(
        "SELECT creator_user_id, creates_join_request, is_primary, is_revoked, name, expire_date, member_limit, subscription_period, subscription_price
         FROM sim_chat_invite_links
         WHERE bot_id = ?1 AND chat_key = ?2 AND invite_link = ?3",
        params![bot_id, chat_key, invite_link],
        |row| {
            Ok(SimChatInviteLinkRecord {
                invite_link: invite_link.to_string(),
                creator_user_id: row.get(0)?,
                creates_join_request: row.get::<_, i64>(1)? == 1,
                is_primary: row.get::<_, i64>(2)? == 1,
                is_revoked: row.get::<_, i64>(3)? == 1,
                name: row.get(4)?,
                expire_date: row.get(5)?,
                member_limit: row.get(6)?,
                subscription_period: row.get(7)?,
                subscription_price: row.get(8)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn resolve_invite_creator_user(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    creator_user_id: i64,
) -> Result<User, ApiError> {
    if creator_user_id == bot.id {
        return Ok(bot::build_bot_user(bot));
    }

    if let Some(record) = users::load_sim_user_record(conn, creator_user_id)? {
        return Ok(users::build_user_from_sim_record(&record, false));
    }

    let fallback = users::ensure_user(
        conn,
        Some(creator_user_id),
        Some(format!("User {}", creator_user_id)),
        None,
    )?;
    Ok(users::build_user_from_sim_record(&fallback, false))
}

pub fn clear_chat_member_restrictions(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    user_id: i64,
) -> Result<(), ApiError> {
    conn.execute(
        "UPDATE sim_chat_members
         SET permissions_json = NULL, until_date = NULL
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot_id, chat_key, user_id],
    )
    .map_err(ApiError::internal)?;
    Ok(())
}

pub fn resolve_sender_chat_for_sim_user_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    sender_user: &SimUserRecord,
    sender_chat_id: Option<i64>,
    send_kind: ChatSendKind,
) -> Result<Option<Chat>, ApiError> {
    let Some(requested_sender_chat_id) = sender_chat_id else {
        return Ok(None);
    };

    if requested_sender_chat_id == 0 {
        return Err(ApiError::bad_request("sender_chat_id is invalid"));
    }

    if sim_chat.chat_type != "group" && sim_chat.chat_type != "supergroup" {
        return Err(ApiError::bad_request(
            "sender_chat_id can only be used in groups or supergroups",
        ));
    }

    if requested_sender_chat_id == sim_chat.chat_id {
        ensure_user_is_chat_admin_or_owner(
            conn,
            bot_id,
            &sim_chat.chat_key,
            sender_user.id,
            "only group owner/admin can send on behalf of this chat",
        )?;
        ensure_sender_chat_not_banned(conn, bot_id, &sim_chat.chat_key, requested_sender_chat_id)?;
        return Ok(Some(groups::build_chat_from_group_record(sim_chat)));
    }

    let sender_chat_key = requested_sender_chat_id.to_string();
    let Some(sender_chat_record) = load_sim_chat_record(conn, bot_id, &sender_chat_key)? else {
        return Err(ApiError::bad_request("sender_chat_id chat not found"));
    };

    if sender_chat_record.chat_type != "channel" {
        return Err(ApiError::bad_request(
            "sender_chat_id must be the current group or its linked channel",
        ));
    }

    let linked_channel_chat_id = resolve_linked_chat_id_for_chat(conn, bot_id, sim_chat)?;
    if linked_channel_chat_id != Some(sender_chat_record.chat_id) {
        return Err(ApiError::bad_request(
            "sender_chat_id must match the linked channel for this discussion",
        ));
    }

    ensure_sender_can_send_in_chat(
        conn,
        bot_id,
        &sender_chat_key,
        sender_user.id,
        send_kind,
    )?;
    ensure_sender_chat_not_banned(conn, bot_id, &sim_chat.chat_key, requested_sender_chat_id)?;

    Ok(Some(groups::build_chat_from_group_record(&sender_chat_record)))
}

pub fn handle_sim_approve_join_request(
    state: &Data<AppState>,
    token: &str,
    body: SimResolveJoinRequestRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        bot::build_bot_user(&bot)
    } else {
        let actor_record = users::get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        users::build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(groups::is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only owner or admin can approve join requests"));
    }
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let request_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT invite_link, status
             FROM sim_chat_join_requests
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, body.user_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((invite_link, status)) = request_row else {
        return Err(ApiError::not_found("join request not found"));
    };
    if status != "pending" {
        return Ok(json!({
            "approved": false,
            "reason": "already_resolved",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let target_user = if let Some(record) = users::load_sim_user_record(&mut conn, body.user_id)? {
        record
    } else {
        users::ensure_user(
            &mut conn,
            Some(body.user_id),
            Some(format!("User {}", body.user_id)),
            None,
        )?
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'approved', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, body.user_id, now],
    )
    .map_err(ApiError::internal)?;

    let current_status = load_chat_member_status(&mut conn, bot.id, &chat_key, target_user.id)?;
    if current_status
        .as_deref()
        .map(groups::is_active_chat_member_status)
        .unwrap_or(false)
    {
        return Ok(json!({
            "approved": true,
            "joined": false,
            "reason": "already_member",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }
    if current_status.as_deref() == Some("banned") {
        return Err(ApiError::bad_request("user is banned in this chat"));
    }

    let invite = if let Some(raw_link) = invite_link {
        let record_row: Option<(i64, i64, i64, Option<String>, Option<i64>, Option<i64>, Option<i64>, Option<i64>)> = conn
            .query_row(
                "SELECT creator_user_id, creates_join_request, is_primary, name, expire_date, member_limit, subscription_period, subscription_price
                 FROM sim_chat_invite_links
                 WHERE bot_id = ?1 AND invite_link = ?2",
                params![bot.id, &raw_link],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?, row.get(6)?, row.get(7)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some((creator_user_id, creates_join_request_raw, is_primary_raw, name, expire_date, member_limit, subscription_period, subscription_price)) = record_row {
            let creator = if creator_user_id == bot.id {
                bot::build_bot_user(&bot)
            } else if let Some(record) = users::load_sim_user_record(&mut conn, creator_user_id)? {
                users::build_user_from_sim_record(&record, false)
            } else {
                users::build_user_from_sim_record(
                    &users::ensure_user(
                        &mut conn,
                        Some(creator_user_id),
                        Some(format!("User {}", creator_user_id)),
                        None,
                    )?,
                    false,
                )
            };
            Some(chat_invite_link_from_record(
                creator,
                &SimChatInviteLinkRecord {
                    invite_link: raw_link,
                    creator_user_id,
                    creates_join_request: creates_join_request_raw == 1,
                    is_primary: is_primary_raw == 1,
                    is_revoked: false,
                    name,
                    expire_date,
                    member_limit,
                    subscription_period,
                    subscription_price,
                },
                None,
            ))
        } else {
            None
        }
    } else {
        None
    };

    groups::join_user_to_group(
        state,
        &mut conn,
        token,
        bot.id,
        &sim_chat,
        &target_user,
        current_status.as_deref(),
        invite,
        Some(true),
    )?;

    Ok(json!({
        "approved": true,
        "joined": true,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_decline_join_request(
    state: &Data<AppState>,
    token: &str,
    body: SimResolveJoinRequestRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = body.chat_id.to_string();
    let Some(sim_chat) = load_sim_chat_record(&mut conn, bot.id, &chat_key)? else {
        return Err(ApiError::not_found("chat not found"));
    };

    let actor_id = body.actor_user_id.unwrap_or(bot.id);
    let actor = if actor_id == bot.id {
        bot::build_bot_user(&bot)
    } else {
        let actor_record = users::get_or_create_user(
            &mut conn,
            Some(actor_id),
            body.actor_first_name,
            body.actor_username,
        )?;
        users::build_user_from_sim_record(&actor_record, false)
    };

    let actor_status = load_chat_member_status(&mut conn, bot.id, &chat_key, actor.id)?;
    if !actor_status
        .as_deref()
        .map(groups::is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request("only owner or admin can decline join requests"));
    }
    if sim_chat.chat_type == "channel" {
        channels::ensure_channel_actor_can_manage_invite_links(&mut conn, bot.id, &chat_key, actor.id)?;
    }

    let status: Option<String> = conn
        .query_row(
            "SELECT status FROM sim_chat_join_requests WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
            params![bot.id, &chat_key, body.user_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(current_status) = status else {
        return Err(ApiError::not_found("join request not found"));
    };
    if current_status != "pending" {
        return Ok(json!({
            "declined": false,
            "reason": "already_resolved",
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_chat_join_requests
         SET status = 'declined', updated_at = ?4
         WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3",
        params![bot.id, &chat_key, body.user_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "declined": true,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_add_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    body: SimAddUserChatBoostsRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let count = body.count.unwrap_or(1);
    if count <= 0 || count > 100 {
        return Err(ApiError::bad_request("count must be between 1 and 100"));
    }

    let duration_days = body.duration_days.unwrap_or(30);
    if duration_days <= 0 || duration_days > 3650 {
        return Err(ApiError::bad_request("duration_days must be between 1 and 3650"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    users::ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let chat_id_value = Value::from(body.chat_id);
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &chat_id_value)?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, body.user_id)?;

    let user = users::ensure_sim_user_record(&mut conn, body.user_id)?;
    if !user.is_premium {
        return Err(ApiError::bad_request("only premium users can boost chats"));
    }

    let source_json = serde_json::to_string(&json!({
        "source": "premium",
        "user": users::build_user_from_sim_record(&user, false),
    }))
    .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    let mut added_boost_ids = Vec::<String>::with_capacity(count as usize);
    for index in 0..count {
        let boost_id = generate_telegram_numeric_id();
        let add_date = now - (index * 60);
        let expiration_date = add_date + (duration_days * 24 * 60 * 60);

        conn.execute(
            "INSERT INTO sim_user_chat_boosts
             (bot_id, chat_key, user_id, boost_id, add_date, expiration_date, source_json, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)",
            params![
                bot.id,
                &chat_key,
                body.user_id,
                &boost_id,
                add_date,
                expiration_date,
                &source_json,
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        added_boost_ids.push(boost_id);
    }

    Ok(json!({
        "added_count": added_boost_ids.len(),
        "boost_ids": added_boost_ids,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_remove_user_chat_boosts(
    state: &Data<AppState>,
    token: &str,
    body: SimRemoveUserChatBoostsRequest,
) -> ApiResult {
    if body.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    users::ensure_sim_user_chat_boosts_storage(&mut conn)?;

    let chat_id_value = Value::from(body.chat_id);
    let (chat_key, _sim_chat) = resolve_non_private_sim_chat(&mut conn, bot.id, &chat_id_value)?;
    ensure_sender_is_chat_member(&mut conn, bot.id, &chat_key, body.user_id)?;

    users::ensure_sim_user_record(&mut conn, body.user_id)?;

    let mut stmt = conn
        .prepare(
            "SELECT boost_id
             FROM sim_user_chat_boosts
             WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3
             ORDER BY expiration_date DESC, add_date DESC, boost_id ASC",
        )
        .map_err(ApiError::internal)?;

    let existing_rows = stmt
        .query_map(params![bot.id, &chat_key, body.user_id], |row| row.get::<_, String>(0))
        .map_err(ApiError::internal)?;
    let existing_boost_ids = existing_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;

    if existing_boost_ids.is_empty() {
        return Ok(json!({
            "removed_count": 0,
            "boost_ids": Vec::<String>::new(),
            "chat_id": body.chat_id,
            "user_id": body.user_id,
        }));
    }

    let target_ids = if body.remove_all.unwrap_or(false) {
        existing_boost_ids.clone()
    } else if let Some(boost_ids) = body.boost_ids {
        let wanted = boost_ids
            .into_iter()
            .map(|item| item.trim().to_string())
            .filter(|item| !item.is_empty())
            .collect::<HashSet<_>>();

        existing_boost_ids
            .iter()
            .filter(|boost_id| wanted.contains(*boost_id))
            .cloned()
            .collect::<Vec<_>>()
    } else {
        vec![existing_boost_ids[0].clone()]
    };

    for boost_id in &target_ids {
        conn.execute(
            "DELETE FROM sim_user_chat_boosts WHERE bot_id = ?1 AND chat_key = ?2 AND user_id = ?3 AND boost_id = ?4",
            params![bot.id, &chat_key, body.user_id, boost_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!({
        "removed_count": target_ids.len(),
        "boost_ids": target_ids,
        "chat_id": body.chat_id,
        "user_id": body.user_id,
    }))
}

pub fn handle_sim_bootstrap(state: &Data<AppState>, token: &str) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_default_user(&mut conn)?;
    ensure_sim_verifications_storage(&mut conn)?;

    let mut users_stmt = conn
        .prepare(
            "SELECT u.id, u.username, u.first_name, u.last_name, u.phone_number, u.photo_url, u.bio, u.is_premium,
                    u.business_name, u.business_intro, u.business_location, u.gift_count,
                    v.custom_description
             FROM users u
             LEFT JOIN sim_user_verifications v
                ON v.bot_id = ?1 AND v.user_id = u.id
             ORDER BY u.id ASC",
        )
        .map_err(ApiError::internal)?;
    let users_rows = users_stmt
        .query_map(params![bot.id], |row| {
            let verification_description: Option<String> = row.get(12)?;
            Ok(json!({
                "id": row.get::<_, i64>(0)?,
                "username": row.get::<_, Option<String>>(1)?,
                "first_name": row.get::<_, String>(2)?,
                "last_name": row.get::<_, Option<String>>(3)?,
                "phone_number": row.get::<_, Option<String>>(4)?,
                "photo_url": row.get::<_, Option<String>>(5)?,
                "bio": row.get::<_, Option<String>>(6)?,
                "is_premium": row.get::<_, i64>(7)? == 1,
                "business_name": row.get::<_, Option<String>>(8)?,
                "business_intro": row.get::<_, Option<String>>(9)?,
                "business_location": row.get::<_, Option<String>>(10)?,
                "gift_count": row.get::<_, i64>(11)?,
                "is_verified": verification_description.is_some(),
                "verification_description": verification_description,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut users = Vec::<Value>::new();
    for row in users_rows {
        users.push(row.map_err(ApiError::internal)?);
    }

    let mut chats_stmt = conn
        .prepare(
                        "SELECT c.chat_id, c.chat_type, c.title, c.username, c.is_forum, c.is_direct_messages,
                                cv.custom_description
                         FROM sim_chats c
                         LEFT JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
                         LEFT JOIN sim_chat_verifications cv
                            ON cv.bot_id = c.bot_id AND cv.chat_key = c.chat_key
                         WHERE c.bot_id = ?1
                             AND (
                                        COALESCE(c.is_direct_messages, 0) = 0
                                        OR COALESCE(parent.direct_messages_enabled, 0) = 1
                             )
                         ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let chats_rows = chats_stmt
        .query_map(params![bot.id], |row| {
            let chat = Chat {
                id: row.get(0)?,
                r#type: row.get(1)?,
                title: row.get(2)?,
                username: row.get(3)?,
                first_name: None,
                last_name: None,
                is_forum: Some(row.get::<_, i64>(4)? == 1),
                is_direct_messages: if row.get::<_, i64>(5)? == 1 {
                    Some(true)
                } else {
                    None
                },
            };
            let verification_description: Option<String> = row.get(6)?;
            Ok((chat, verification_description))
        })
        .map_err(ApiError::internal)?;
    let mut chats = Vec::<Value>::new();
    for row in chats_rows {
        let (chat, verification_description) = row.map_err(ApiError::internal)?;
        let mut chat_value = serde_json::to_value(chat).map_err(ApiError::internal)?;
        if let Some(object) = chat_value.as_object_mut() {
            object.insert(
                "is_verified".to_string(),
                Value::Bool(verification_description.is_some()),
            );
            if let Some(description) = verification_description {
                object.insert(
                    "verification_description".to_string(),
                    Value::String(description),
                );
            }
        }
        chats.push(chat_value);
    }

    let mut channel_direct_messages_stmt = conn
        .prepare(
                        "SELECT c.parent_channel_chat_id, c.chat_id
                         FROM sim_chats c
                         INNER JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
                         WHERE c.bot_id = ?1
                             AND c.is_direct_messages = 1
                             AND c.parent_channel_chat_id IS NOT NULL
                             AND parent.direct_messages_enabled = 1
                         ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let channel_direct_messages_rows = channel_direct_messages_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "channel_chat_id": row.get::<_, i64>(0)?,
                "direct_messages_chat_id": row.get::<_, i64>(1)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut channel_direct_messages = Vec::<Value>::new();
    for row in channel_direct_messages_rows {
        channel_direct_messages.push(row.map_err(ApiError::internal)?);
    }

    let mut chat_settings_stmt = conn
        .prepare(
            "SELECT chat_id, description, channel_show_author_signature, channel_paid_reactions_enabled, linked_discussion_chat_id, message_history_visible, slow_mode_delay, permissions_json, direct_messages_enabled, direct_messages_star_count
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_type IN ('group', 'supergroup', 'channel')
             ORDER BY chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let chat_settings_rows = chat_settings_stmt
        .query_map(params![bot.id], |row| {
            let permissions_raw: Option<String> = row.get(7)?;
            let permissions = permissions_raw
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
                .unwrap_or_else(groups::default_group_permissions);
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "description": row.get::<_, Option<String>>(1)?,
                "show_author_signature": row.get::<_, i64>(2)? == 1,
                "paid_star_reactions_enabled": row.get::<_, i64>(3)? == 1,
                "linked_chat_id": row.get::<_, Option<i64>>(4)?,
                "message_history_visible": row.get::<_, i64>(5)? == 1,
                "slow_mode_delay": row.get::<_, i64>(6)?,
                "direct_messages_enabled": row.get::<_, i64>(8)? == 1,
                "direct_messages_star_count": row.get::<_, i64>(9)?,
                "permissions": permissions,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut chat_settings = Vec::<Value>::new();
    for row in chat_settings_rows {
        chat_settings.push(row.map_err(ApiError::internal)?);
    }

    let mut memberships_stmt = conn
        .prepare(
                        "SELECT c.chat_id, m.user_id, m.status, m.role, m.custom_title, m.tag
             FROM sim_chat_members m
             INNER JOIN sim_chats c
               ON c.bot_id = m.bot_id AND c.chat_key = m.chat_key
             WHERE m.bot_id = ?1
             ORDER BY c.chat_id ASC, m.user_id ASC",
        )
        .map_err(ApiError::internal)?;
    let memberships_rows = memberships_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "user_id": row.get::<_, i64>(1)?,
                "status": row.get::<_, String>(2)?,
                "role": row.get::<_, String>(3)?,
                "custom_title": row.get::<_, Option<String>>(4)?,
                "tag": row.get::<_, Option<String>>(5)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut memberships = Vec::<Value>::new();
    for row in memberships_rows {
        memberships.push(row.map_err(ApiError::internal)?);
    }

    let mut join_requests_stmt = conn
        .prepare(
            "SELECT c.chat_id, r.user_id, r.invite_link, r.status, r.created_at, u.first_name, u.username
             FROM sim_chat_join_requests r
             INNER JOIN sim_chats c
               ON c.bot_id = r.bot_id AND c.chat_key = r.chat_key
             LEFT JOIN users u
               ON u.id = r.user_id
             WHERE r.bot_id = ?1
             ORDER BY r.created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let join_requests_rows = join_requests_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "user_id": row.get::<_, i64>(1)?,
                "invite_link": row.get::<_, Option<String>>(2)?,
                "status": row.get::<_, String>(3)?,
                "date": row.get::<_, i64>(4)?,
                "first_name": row.get::<_, Option<String>>(5)?,
                "username": row.get::<_, Option<String>>(6)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut join_requests = Vec::<Value>::new();
    for row in join_requests_rows {
        join_requests.push(row.map_err(ApiError::internal)?);
    }

    let mut forum_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id, t.message_thread_id, t.name, t.icon_color, t.icon_custom_emoji_id,
                    t.is_closed, t.updated_at
             FROM forum_topics t
             INNER JOIN sim_chats c
               ON c.bot_id = t.bot_id AND c.chat_key = t.chat_key
             WHERE t.bot_id = ?1
             ORDER BY c.chat_id ASC, t.message_thread_id ASC",
        )
        .map_err(ApiError::internal)?;
    let forum_topics_rows = forum_topics_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": row.get::<_, i64>(1)?,
                "name": row.get::<_, String>(2)?,
                "icon_color": row.get::<_, i64>(3)?,
                "icon_custom_emoji_id": row.get::<_, Option<String>>(4)?,
                "is_closed": row.get::<_, i64>(5)? == 1,
                "is_hidden": false,
                "is_general": false,
                "updated_at": row.get::<_, i64>(6)?,
            }))
        })
        .map_err(ApiError::internal)?;
    let mut forum_topics = Vec::<Value>::new();
    for row in forum_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    let mut general_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id,
                    COALESCE(g.name, 'General') AS name,
                    COALESCE(g.is_closed, 0) AS is_closed,
                    COALESCE(g.is_hidden, 0) AS is_hidden,
                    COALESCE(g.updated_at, CAST(strftime('%s','now') AS INTEGER)) AS updated_at
             FROM sim_chats c
             LEFT JOIN forum_topic_general_states g
               ON g.bot_id = c.bot_id AND g.chat_key = c.chat_key
                         WHERE c.bot_id = ?1
                             AND c.chat_type = 'supergroup'
                             AND c.is_forum = 1
                             AND COALESCE(c.is_direct_messages, 0) = 0
             ORDER BY c.chat_id ASC",
        )
        .map_err(ApiError::internal)?;
    let general_topics_rows = general_topics_stmt
        .query_map(params![bot.id], |row| {
            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": 1,
                "name": row.get::<_, String>(1)?,
                "icon_color": groups::forum_topic_default_icon_color(),
                "icon_custom_emoji_id": Value::Null,
                "is_closed": row.get::<_, i64>(2)? == 1,
                "is_hidden": row.get::<_, i64>(3)? == 1,
                "is_general": true,
                "updated_at": row.get::<_, i64>(4)?,
            }))
        })
        .map_err(ApiError::internal)?;
    for row in general_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    let mut direct_topics_stmt = conn
        .prepare(
            "SELECT c.chat_id, t.topic_id, u.first_name, u.username, t.updated_at
             FROM sim_direct_message_topics t
             INNER JOIN sim_chats c
               ON c.bot_id = t.bot_id AND c.chat_key = t.chat_key
                         INNER JOIN sim_chats parent
                             ON parent.bot_id = c.bot_id
                            AND parent.chat_type = 'channel'
                            AND c.parent_channel_chat_id = parent.chat_id
             LEFT JOIN users u
               ON u.id = t.user_id
                         WHERE t.bot_id = ?1
                             AND parent.direct_messages_enabled = 1
             ORDER BY c.chat_id ASC, t.updated_at DESC, t.topic_id ASC",
        )
        .map_err(ApiError::internal)?;
    let direct_topics_rows = direct_topics_stmt
        .query_map(params![bot.id], |row| {
            let topic_id: i64 = row.get(1)?;
            let first_name: Option<String> = row.get(2)?;
            let username: Option<String> = row.get(3)?;
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
                        .unwrap_or_else(|| format!("User {}", topic_id))
                });

            Ok(json!({
                "chat_id": row.get::<_, i64>(0)?,
                "message_thread_id": topic_id,
                "name": label,
                "icon_color": groups::forum_topic_default_icon_color(),
                "icon_custom_emoji_id": Value::Null,
                "is_closed": false,
                "is_hidden": false,
                "is_general": false,
                "updated_at": row.get::<_, i64>(4)?,
            }))
        })
        .map_err(ApiError::internal)?;
    for row in direct_topics_rows {
        forum_topics.push(row.map_err(ApiError::internal)?);
    }

    Ok(json!({
        "bot": {
            "id": bot.id,
            "token": token,
            "username": bot.username,
            "first_name": bot.first_name
        },
        "users": users,
        "chats": chats,
        "channel_direct_messages": channel_direct_messages,
        "chat_settings": chat_settings,
        "memberships": memberships,
        "join_requests": join_requests,
        "forum_topics": forum_topics
    }))
}

pub fn normalize_profile_pagination(
    offset: Option<i64>,
    limit: Option<i64>,
) -> Result<(usize, usize), ApiError> {
    let normalized_offset = offset.unwrap_or(0);
    if normalized_offset < 0 {
        return Err(ApiError::bad_request("offset must be non-negative"));
    }

    let normalized_limit = limit.unwrap_or(100);
    if !(1..=100).contains(&normalized_limit) {
        return Err(ApiError::bad_request("limit must be between 1 and 100"));
    }

    Ok((normalized_offset as usize, normalized_limit as usize))
}