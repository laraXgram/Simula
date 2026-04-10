use chrono::Utc;
use rusqlite::{params, OptionalExtension};

use crate::types::ApiError;
use crate::handlers::client::types::users::SimUserRecord;

use crate::generated::types::{Chat, User};

use crate::handlers::utils::updates::current_request_actor_user_id;

use super::chats::ChatSendKind;
use super::{bot, chats};

pub fn build_user_from_sim_record(record: &SimUserRecord, is_bot: bool) -> User {
    User {
        id: record.id,
        is_bot,
        first_name: record.first_name.clone(),
        last_name: record.last_name.clone(),
        username: record.username.clone(),
        language_code: None,
        is_premium: Some(record.is_premium),
        added_to_attachment_menu: None,
        can_join_groups: None,
        can_read_all_group_messages: None,
        supports_inline_queries: None,
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

pub fn ensure_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<SimUserRecord, ApiError> {
    if let Some(existing) = load_sim_user_record(conn, user_id)? {
        return Ok(existing);
    }

    ensure_user(
        conn,
        Some(user_id),
        Some(format!("User {}", user_id)),
        None,
    )
}

pub fn ensure_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    let id = user_id.unwrap_or(10001);
    let effective_first_name = first_name.unwrap_or_else(|| "Test User".to_string());
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO users (id, username, first_name, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            first_name = excluded.first_name",
        params![id, username, effective_first_name, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .map_err(ApiError::internal)
}

pub fn load_sim_user_record(
    conn: &mut rusqlite::Connection,
    user_id: i64,
) -> Result<Option<SimUserRecord>, ApiError> {
    conn.query_row(
        "SELECT id, first_name, username, last_name, is_premium FROM users WHERE id = ?1",
        params![user_id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
                last_name: row.get(3)?,
                is_premium: row.get::<_, i64>(4)? == 1,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn resolve_transport_sender_user(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    send_kind: ChatSendKind,
) -> Result<User, ApiError> {
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    if actor_user_id == bot.id {
        return Ok(bot::build_bot_user(bot));
    }

    let actor_record = ensure_sim_user_record(conn, actor_user_id)?;
    if destination_chat.r#type != "private" {
        chats::ensure_sender_can_send_in_chat(conn, bot.id, destination_chat_key, actor_user_id, send_kind)?;
    }

    Ok(build_user_from_sim_record(&actor_record, false))
}