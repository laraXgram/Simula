use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use crate::types::ApiError;
use crate::handlers::client::types::chats::SimChatRecord;
use crate::handlers::client::types::groups::GroupRuntimeSettings;

use crate::generated::types::ChatPermissions;

use super::{channels, chats};

pub fn is_active_chat_member_status(status: &str) -> bool {
    matches!(status, "owner" | "admin" | "member" | "restricted")
}

pub fn is_group_admin_or_owner_status(status: &str) -> bool {
    matches!(status, "owner" | "admin")
}

pub fn default_group_permissions() -> ChatPermissions {
    ChatPermissions {
        can_send_messages: Some(true),
        can_send_audios: Some(true),
        can_send_documents: Some(true),
        can_send_photos: Some(true),
        can_send_videos: Some(true),
        can_send_video_notes: Some(true),
        can_send_voice_notes: Some(true),
        can_send_polls: Some(true),
        can_send_other_messages: Some(true),
        can_add_web_page_previews: Some(true),
        can_change_info: Some(false),
        can_invite_users: Some(true),
        can_pin_messages: Some(false),
        can_manage_topics: Some(false),
        can_edit_tag: Some(false),
    }
}

pub fn load_group_runtime_settings(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<Option<GroupRuntimeSettings>, ApiError> {
    let row: Option<(String, i64, Option<String>)> = conn
        .query_row(
            "SELECT chat_type, slow_mode_delay, permissions_json
             FROM sim_chats
             WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_type, slow_mode_delay, permissions_raw)) = row else {
        return Ok(None);
    };
    if chat_type == "private" {
        return Ok(None);
    }

    let permissions = permissions_raw
        .as_deref()
        .and_then(|raw| serde_json::from_str::<ChatPermissions>(raw).ok())
        .unwrap_or_else(default_group_permissions);

    Ok(Some(GroupRuntimeSettings {
        chat_type,
        slow_mode_delay: slow_mode_delay.max(0),
        permissions,
    }))
}

// --- Forum Topics Begin ---

pub fn resolve_forum_message_thread_for_chat_key(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot_id, chat_key)? {
        return resolve_forum_message_thread_id(conn, bot_id, &sim_chat, requested_message_thread_id);
    }

    if requested_message_thread_id.is_some() {
        return Err(ApiError::bad_request(
            "message_thread_id is available only in forum supergroups",
        ));
    }

    Ok(None)
}

pub fn resolve_forum_message_thread_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    sim_chat: &SimChatRecord,
    requested_message_thread_id: Option<i64>,
) -> Result<Option<i64>, ApiError> {
    if channels::is_direct_messages_chat(sim_chat) {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not available in channel direct messages chats",
            ));
        }
        return Ok(None);
    }

    if sim_chat.chat_type != "supergroup" || !sim_chat.is_forum {
        if requested_message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is available only in forum supergroups",
            ));
        }
        return Ok(None);
    }

    let thread_id = requested_message_thread_id.unwrap_or(1);
    if thread_id <= 0 {
        return Err(ApiError::bad_request("message_thread_id is invalid"));
    }

    if thread_id == 1 {
        let (_, is_closed, is_hidden) = ensure_general_forum_topic_state(conn, bot_id, &sim_chat.chat_key)?;
        if is_closed {
            return Err(ApiError::bad_request("general forum topic is closed"));
        }
        if is_hidden {
            return Err(ApiError::bad_request("general forum topic is hidden"));
        }
        return Ok(Some(thread_id));
    }

    let topic_is_closed: Option<i64> = conn
        .query_row(
            "SELECT is_closed FROM forum_topics
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot_id, &sim_chat.chat_key, thread_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if topic_is_closed.is_none() {
        return Err(ApiError::not_found("forum topic not found"));
    }

    if topic_is_closed.unwrap_or_default() == 1 {
        return Err(ApiError::bad_request("forum topic is closed"));
    }

    Ok(Some(thread_id))
}

pub fn ensure_general_forum_topic_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(String, bool, bool), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO forum_topic_general_states (bot_id, chat_key, name, is_closed, is_hidden, updated_at)
         VALUES (?1, ?2, 'General', 0, 0, ?3)
         ON CONFLICT(bot_id, chat_key) DO NOTHING",
        params![bot_id, chat_key, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT name, is_closed, is_hidden FROM forum_topic_general_states
         WHERE bot_id = ?1 AND chat_key = ?2",
        params![bot_id, chat_key],
        |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)? == 1,
                row.get::<_, i64>(2)? == 1,
            ))
        },
    )
    .map_err(ApiError::internal)
}