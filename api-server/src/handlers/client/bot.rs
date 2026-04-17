use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashSet;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::types::{
    BotCommand, BotCommandScope, ChatAdministratorRights,
    Message, MenuButton, User
};
use crate::handlers::{
    generate_telegram_token, token_suffix, sanitize_username, generate_telegram_numeric_id,
    utils::updates::value_to_chat_key
};

use super::{bot, chats, groups, messages, users};
use super::types::bot::{
    SimCreateBotRequest, SimUpdateBotRequest, SimSetPrivacyModeRequest, SimManagedBotRecord,
};
use super::types::users::SimUserRecord;

pub fn ensure_bot_is_chat_admin_or_owner(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
) -> Result<(), ApiError> {
    let bot_status = chats::load_chat_member_status(conn, bot_id, chat_key, bot_id)?;
    if !bot_status
        .as_deref()
        .map(groups::is_group_admin_or_owner_status)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "bot is not an administrator in this chat",
        ));
    }

    Ok(())
}

pub fn build_bot_user(bot: &crate::database::BotInfoRecord) -> User {
    User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
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
    }
}

pub fn ensure_username_available_globally(
    conn: &mut rusqlite::Connection,
    username: &str,
    allowed_bot_id: Option<i64>,
    allowed_user_id: Option<i64>,
    allowed_chat: Option<(i64, &str)>,
) -> Result<(), ApiError> {
    let normalized = username.trim().to_ascii_lowercase();
    if normalized.is_empty() {
        return Err(ApiError::bad_request("username is invalid"));
    }

    let existing_bot_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM bots WHERE username IS NOT NULL AND LOWER(username) = LOWER(?1) LIMIT 1",
            params![normalized],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if let Some(existing_bot_id) = existing_bot_id {
        if Some(existing_bot_id) != allowed_bot_id {
            return Err(ApiError::bad_request("username is already taken"));
        }
    }

    let existing_user_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM users WHERE username IS NOT NULL AND LOWER(username) = LOWER(?1) LIMIT 1",
            params![normalized],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if let Some(existing_user_id) = existing_user_id {
        if Some(existing_user_id) != allowed_user_id {
            return Err(ApiError::bad_request("username is already taken"));
        }
    }

    let existing_chat: Option<(i64, String)> = conn
        .query_row(
            "SELECT bot_id, chat_key
             FROM sim_chats
             WHERE username IS NOT NULL AND LOWER(username) = LOWER(?1)
             LIMIT 1",
            params![normalized],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if let Some((existing_chat_bot_id, existing_chat_key)) = existing_chat {
        let is_allowed_chat = allowed_chat
            .map(|(allowed_chat_bot_id, allowed_chat_key)| {
                allowed_chat_bot_id == existing_chat_bot_id
                    && allowed_chat_key == existing_chat_key.as_str()
            })
            .unwrap_or(false);
        if !is_allowed_chat {
            return Err(ApiError::bad_request("username is already taken"));
        }
    }

    Ok(())
}

pub fn normalize_bot_username(raw: &str) -> Result<String, ApiError> {
    let normalized = sanitize_username(raw);
    if normalized.is_empty() {
        return Err(ApiError::bad_request("bot username is invalid"));
    }
    if !normalized.ends_with("bot") {
        return Err(ApiError::bad_request("bot username must end with 'bot'"));
    }

    Ok(normalized)
}

pub fn ensure_sim_bot_commands_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_commands (
            bot_id         INTEGER NOT NULL,
            scope_key      TEXT NOT NULL,
            language_code  TEXT NOT NULL,
            commands_json  TEXT NOT NULL,
            updated_at     INTEGER NOT NULL,
            PRIMARY KEY (bot_id, scope_key, language_code),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_bot_profile_texts_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_profile_texts (
            bot_id             INTEGER NOT NULL,
            language_code      TEXT NOT NULL,
            name               TEXT,
            description        TEXT,
            short_description  TEXT,
            updated_at         INTEGER NOT NULL,
            PRIMARY KEY (bot_id, language_code),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_bot_profile_photos_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_profile_photos (
            bot_id        INTEGER PRIMARY KEY,
            file_id       TEXT NOT NULL,
            media_kind    TEXT NOT NULL,
            updated_at    INTEGER NOT NULL,
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_bot_default_admin_rights_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_bot_default_admin_rights (
            bot_id        INTEGER NOT NULL,
            for_channels  INTEGER NOT NULL,
            rights_json   TEXT,
            updated_at    INTEGER NOT NULL,
            PRIMARY KEY (bot_id, for_channels),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}

pub fn ensure_sim_managed_bots_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_managed_bots (
            bot_id          INTEGER NOT NULL,
            owner_user_id   INTEGER NOT NULL,
            managed_bot_id  INTEGER NOT NULL,
            created_at      INTEGER NOT NULL,
            updated_at      INTEGER NOT NULL,
            PRIMARY KEY (bot_id, owner_user_id),
            UNIQUE (bot_id, managed_bot_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id),
            FOREIGN KEY(managed_bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}
pub fn message_targets_bot_in_privacy_mode(message: &Message, bot: &crate::database::BotInfoRecord) -> bool {
    if let Some(reply) = message.reply_to_message.as_ref() {
        let reply_from_id = reply.from.as_ref().map(|from| from.id);
        if reply_from_id == Some(bot.id) {
            return true;
        }
    }

    if messages::message_targets_bot_via_entities(
        message.text.as_deref(),
        message.entities.as_ref(),
        &bot.username,
    ) {
        return true;
    }

    if messages::message_targets_bot_via_entities(
        message.caption.as_deref(),
        message.caption_entities.as_ref(),
        &bot.username,
    ) {
        return true;
    }

    if let Some(text) = message.text.as_deref() {
        if messages::text_matches_privacy_command_or_mention(text, &bot.username) {
            return true;
        }
    }

    if let Some(caption) = message.caption.as_deref() {
        if messages::text_matches_privacy_command_or_mention(caption, &bot.username) {
            return true;
        }
    }

    false
}

pub fn should_emit_user_generated_update_to_bot(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_type: &str,
    from_user_id: i64,
    message: &Message,
) -> Result<bool, ApiError> {
    if chat_type == "private" || from_user_id == bot.id {
        return Ok(true);
    }

    let privacy_mode_enabled = bot::load_bot_privacy_mode_enabled(conn, bot.id)?;
    if !privacy_mode_enabled {
        return Ok(true);
    }

    Ok(bot::message_targets_bot_in_privacy_mode(message, bot))
}

pub fn load_bot_privacy_mode_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
) -> Result<bool, ApiError> {
    let value: Option<i64> = conn
        .query_row(
            "SELECT privacy_mode_enabled FROM sim_bot_runtime_settings WHERE bot_id = ?1",
            params![bot_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    Ok(value.unwrap_or(1) == 1)
}

pub fn set_bot_privacy_mode_enabled(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    enabled: bool,
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_runtime_settings (bot_id, privacy_mode_enabled, updated_at)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(bot_id)
         DO UPDATE SET
            privacy_mode_enabled = excluded.privacy_mode_enabled,
            updated_at = excluded.updated_at",
        params![bot_id, if enabled { 1 } else { 0 }, now],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn handle_sim_create_bot(state: &Data<AppState>, body: SimCreateBotRequest) -> ApiResult {
    let mut conn = lock_db(state)?;

    let token = generate_telegram_token();
    let now = Utc::now().timestamp();
    let suffix = token_suffix(&token);

    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("Simula Bot {}", &suffix[..4]));

    let username = if let Some(raw_username) = body.username.as_deref() {
        normalize_bot_username(raw_username)?
    } else {
        format!("simula_{}bot", suffix)
    };

    ensure_username_available_globally(&mut conn, &username, None, None, None)?;

    conn.execute(
        "INSERT INTO bots (token, username, first_name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![token, username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": conn.last_insert_rowid(),
        "token": token,
        "username": username,
        "first_name": first_name
    }))
}

pub fn handle_sim_update_bot(
    state: &Data<AppState>,
    token: &str,
    body: SimUpdateBotRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or(bot.first_name);

    let username = if let Some(raw_username) = body.username.as_deref() {
        normalize_bot_username(raw_username)?
    } else {
        bot.username
    };

    if body.username.is_some() {
        ensure_username_available_globally(&mut conn, &username, Some(bot.id), None, None)?;
    }

    conn.execute(
        "UPDATE bots SET first_name = ?1, username = ?2 WHERE id = ?3",
        params![first_name, username, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": bot.id,
        "token": token,
        "username": username,
        "first_name": first_name
    }))
}

pub fn handle_sim_get_privacy_mode(
    state: &Data<AppState>,
    token: &str,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let enabled = load_bot_privacy_mode_enabled(&mut conn, bot.id)?;

    Ok(json!({
        "enabled": enabled,
    }))
}

pub fn handle_sim_set_privacy_mode(
    state: &Data<AppState>,
    token: &str,
    body: SimSetPrivacyModeRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    set_bot_privacy_mode_enabled(&mut conn, bot.id, body.enabled)?;

    Ok(json!({
        "enabled": body.enabled,
    }))
}

pub fn normalize_bot_language_code(language_code: Option<&str>) -> Result<String, ApiError> {
    let normalized = language_code
        .map(str::trim)
        .unwrap_or("")
        .to_ascii_lowercase();

    if normalized.chars().count() > 32 {
        return Err(ApiError::bad_request(
            "language_code must be at most 32 characters",
        ));
    }

    if !normalized.is_empty()
        && !normalized
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || ch == '-' || ch == '_')
    {
        return Err(ApiError::bad_request("language_code is invalid"));
    }

    Ok(normalized)
}

pub fn normalize_bot_command_scope_key(scope: Option<&BotCommandScope>) -> Result<String, ApiError> {
    let Some(scope) = scope else {
        return Ok("default".to_string());
    };

    let object = scope
        .extra
        .as_object()
        .ok_or_else(|| ApiError::bad_request("scope must be a JSON object"))?;
    let scope_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("scope.type is required"))?;

    match scope_type.as_str() {
        "default" => Ok("default".to_string()),
        "all_private_chats" => Ok("all_private_chats".to_string()),
        "all_group_chats" => Ok("all_group_chats".to_string()),
        "all_chat_administrators" => Ok("all_chat_administrators".to_string()),
        "chat" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat:{}", chat_key))
        }
        "chat_administrators" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat_administrators:{}", chat_key))
        }
        "chat_member" => {
            let chat_id = object
                .get("chat_id")
                .ok_or_else(|| ApiError::bad_request("scope.chat_id is required"))?;
            let user_id = object
                .get("user_id")
                .and_then(|value| {
                    value
                        .as_i64()
                        .or_else(|| value.as_str().and_then(|raw| raw.trim().parse::<i64>().ok()))
                })
                .ok_or_else(|| ApiError::bad_request("scope.user_id is required"))?;
            if user_id <= 0 {
                return Err(ApiError::bad_request("scope.user_id must be greater than zero"));
            }
            let chat_key = value_to_chat_key(chat_id)?;
            Ok(format!("chat_member:{}:{}", chat_key, user_id))
        }
        _ => Err(ApiError::bad_request("unsupported scope.type for bot commands")),
    }
}

pub fn normalize_bot_commands_payload(commands: &[BotCommand]) -> Result<Vec<BotCommand>, ApiError> {
    if commands.is_empty() {
        return Err(ApiError::bad_request("commands must include at least one item"));
    }
    if commands.len() > 100 {
        return Err(ApiError::bad_request("commands must include at most 100 items"));
    }

    let mut seen_commands = HashSet::<String>::new();
    let mut normalized = Vec::<BotCommand>::with_capacity(commands.len());
    for item in commands {
        let command = item.command.trim().to_ascii_lowercase();
        if command.is_empty() {
            return Err(ApiError::bad_request("command is empty"));
        }
        if command.chars().count() > 32 {
            return Err(ApiError::bad_request(
                "command length must be at most 32 characters",
            ));
        }
        if !command
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
        {
            return Err(ApiError::bad_request(
                "command must contain only lowercase letters, digits, and underscores",
            ));
        }

        let description = item.description.trim();
        if description.is_empty() {
            return Err(ApiError::bad_request("command description is empty"));
        }
        if description.chars().count() > 256 {
            return Err(ApiError::bad_request(
                "command description length must be at most 256 characters",
            ));
        }

        if !seen_commands.insert(command.clone()) {
            return Err(ApiError::bad_request("duplicate command in commands list"));
        }

        normalized.push(BotCommand {
            command,
            description: description.to_string(),
        });
    }

    Ok(normalized)
}

pub fn load_bot_profile_text_value(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    language_code: &str,
    column_name: &str,
) -> Result<Option<String>, ApiError> {
    let sql = format!(
        "SELECT {} FROM sim_bot_profile_texts WHERE bot_id = ?1 AND language_code = ?2",
        column_name
    );

    let scoped_value: Option<Option<String>> = conn
        .query_row(&sql, params![bot_id, language_code], |row| row.get(0))
        .optional()
        .map_err(ApiError::internal)?;
    if let Some(value) = scoped_value.flatten() {
        return Ok(Some(value));
    }

    if language_code.is_empty() {
        return Ok(None);
    }

    let default_value: Option<Option<String>> = conn
        .query_row(&sql, params![bot_id, ""], |row| row.get(0))
        .optional()
        .map_err(ApiError::internal)?;

    Ok(default_value.flatten())
}

pub fn default_bot_administrator_rights(for_channels: bool) -> ChatAdministratorRights {
    ChatAdministratorRights {
        is_anonymous: false,
        can_manage_chat: false,
        can_delete_messages: false,
        can_manage_video_chats: false,
        can_restrict_members: false,
        can_promote_members: false,
        can_change_info: false,
        can_invite_users: false,
        can_post_stories: false,
        can_edit_stories: false,
        can_delete_stories: false,
        can_post_messages: if for_channels { Some(false) } else { None },
        can_edit_messages: if for_channels { Some(false) } else { None },
        can_pin_messages: if for_channels { None } else { Some(false) },
        can_manage_topics: if for_channels { None } else { Some(false) },
        can_manage_direct_messages: Some(false),
        can_manage_tags: Some(false),
    }
}

pub fn extract_bot_profile_photo_media_input(raw: &Value) -> Result<(&'static str, Value), ApiError> {
    let Some(obj) = raw.as_object() else {
        return Err(ApiError::bad_request(
            "photo must be a valid InputProfilePhoto object",
        ));
    };

    let photo_type = obj
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("photo.type is required"))?;

    match photo_type.as_str() {
        "static" => {
            let photo = obj
                .get("photo")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| ApiError::bad_request("photo.photo is required"))?;
            Ok(("photo", Value::String(photo.to_string())))
        }
        "animated" => {
            let animation = obj
                .get("animation")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| ApiError::bad_request("photo.animation is required"))?;
            Ok(("animation", Value::String(animation.to_string())))
        }
        _ => Err(ApiError::bad_request(
            "photo.type must be one of: static, animated",
        )),
    }
}

pub fn managed_bot_user_from_record(record: &SimManagedBotRecord) -> User {
    User {
        id: record.managed_bot_id,
        is_bot: true,
        first_name: record.managed_bot_first_name.clone(),
        last_name: None,
        username: Some(record.managed_bot_username.clone()),
        language_code: None,
        is_premium: None,
        added_to_attachment_menu: None,
        can_join_groups: Some(true),
        can_read_all_group_messages: Some(false),
        supports_inline_queries: Some(false),
        can_connect_to_business: None,
        has_main_web_app: None,
        has_topics_enabled: None,
        allows_users_to_create_topics: None,
        can_manage_bots: None,
    }
}

pub fn build_user_with_manage_bots(record: &SimUserRecord) -> User {
    let mut user = users::build_user_from_sim_record(record, false);
    user.can_manage_bots = Some(true);
    user
}

pub fn load_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
) -> Result<Option<SimManagedBotRecord>, ApiError> {
    conn.query_row(
        "SELECT m.owner_user_id, m.managed_bot_id, b.token, b.username, b.first_name, m.created_at, m.updated_at
         FROM sim_managed_bots m
         INNER JOIN bots b ON b.id = m.managed_bot_id
         WHERE m.bot_id = ?1 AND m.owner_user_id = ?2",
        params![manager_bot_id, owner_user_id],
        |row| {
            Ok(SimManagedBotRecord {
                owner_user_id: row.get(0)?,
                managed_bot_id: row.get(1)?,
                managed_token: row.get(2)?,
                managed_bot_username: row.get(3)?,
                managed_bot_first_name: row.get(4)?,
                created_at: row.get(5)?,
                updated_at: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn create_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
    suggested_name: Option<&str>,
    suggested_username: Option<&str>,
) -> Result<SimManagedBotRecord, ApiError> {
    let token = generate_telegram_token();
    let suffix = token_suffix(&token);
    let now = Utc::now().timestamp();

    let first_name = suggested_name
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .unwrap_or_else(|| format!("Managed Bot {}", &suffix[..4]));

    let username = if let Some(raw_username) = suggested_username
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        normalize_bot_username(raw_username)?
    } else {
        format!("managed_{}bot", suffix)
    };

    ensure_username_available_globally(conn, &username, None, None, None)?;

    conn.execute(
        "INSERT INTO bots (token, username, first_name, created_at) VALUES (?1, ?2, ?3, ?4)",
        params![token, username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    let managed_bot_id = conn.last_insert_rowid();
    conn.execute(
        "INSERT INTO sim_managed_bots (bot_id, owner_user_id, managed_bot_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?4)",
        params![manager_bot_id, owner_user_id, managed_bot_id, now],
    )
    .map_err(ApiError::internal)?;

    load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::internal("failed to create managed bot record"))
}

pub fn ensure_managed_bot_record(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
    suggested_name: Option<&str>,
    suggested_username: Option<&str>,
) -> Result<SimManagedBotRecord, ApiError> {
    if owner_user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    if let Some(existing) = load_managed_bot_record(conn, manager_bot_id, owner_user_id)? {
        return Ok(existing);
    }

    create_managed_bot_record(
        conn,
        manager_bot_id,
        owner_user_id,
        suggested_name,
        suggested_username,
    )
}

pub fn rotate_managed_bot_token(
    conn: &mut rusqlite::Connection,
    manager_bot_id: i64,
    owner_user_id: i64,
) -> Result<SimManagedBotRecord, ApiError> {
    let current = load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::not_found("managed bot not found"))?;

    let now = Utc::now().timestamp();
    let new_token = generate_telegram_token();
    conn.execute(
        "UPDATE bots SET token = ?1 WHERE id = ?2",
        params![new_token, current.managed_bot_id],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE sim_managed_bots
         SET updated_at = ?1
         WHERE bot_id = ?2 AND owner_user_id = ?3",
        params![now, manager_bot_id, owner_user_id],
    )
    .map_err(ApiError::internal)?;

    load_managed_bot_record(conn, manager_bot_id, owner_user_id)?
        .ok_or_else(|| ApiError::internal("managed bot update failed"))
}

pub fn menu_button_scope_key(chat_id: Option<i64>) -> String {
    match chat_id {
        Some(value) => format!("chat:{}", value),
        None => "default".to_string(),
    }
}

pub fn default_menu_button() -> MenuButton {
    MenuButton {
        extra: json!({ "type": "default" }),
    }
}

pub fn load_bot_star_balance(conn: &mut rusqlite::Connection, bot_id: i64) -> Result<i64, ApiError> {
    conn.query_row(
        "SELECT COALESCE(SUM(amount), 0) FROM star_transactions_ledger WHERE bot_id = ?1",
        params![bot_id],
        |row| row.get(0),
    )
    .map_err(ApiError::internal)
}

pub fn ensure_bot_star_balance_for_charge(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    required_star_count: i64,
    now: i64,
) -> Result<(), ApiError> {
    if required_star_count <= 0 {
        return Ok(());
    }

    let current_balance = load_bot_star_balance(conn, bot_id)?;
    if current_balance >= required_star_count {
        return Ok(());
    }

    let top_up_amount = required_star_count.saturating_sub(current_balance);
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'simulator_bot_topup')",
        params![
            format!("sim_topup_{}", generate_telegram_numeric_id()),
            bot_id,
            bot_id,
            format!("sim_topup_charge_{}", generate_telegram_numeric_id()),
            top_up_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn ensure_sim_passport_data_errors_storage(
    conn: &mut rusqlite::Connection,
) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_passport_data_errors (
            bot_id       INTEGER NOT NULL,
            user_id      INTEGER NOT NULL,
            errors_json  TEXT NOT NULL,
            updated_at   INTEGER NOT NULL,
            PRIMARY KEY (bot_id, user_id),
            FOREIGN KEY(bot_id) REFERENCES bots(id)
        );",
    )
    .map_err(ApiError::internal)
}