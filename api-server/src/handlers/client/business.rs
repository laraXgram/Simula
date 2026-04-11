use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};
use crate::generated::types::{
    AcceptedGiftTypes, BusinessBotRights, BusinessConnection, Chat, User, Update
};
use crate::handlers::utils::updates::current_request_actor_user_id;

use super::types::business::{
    SimBusinessConnectionRecord, SimBusinessProfileRecord, SimSetBusinessConnectionRequest,
    SimRemoveBusinessConnectionRequest
};
use super::{users, webhook};

pub fn normalize_business_connection_id(raw: Option<&str>) -> Option<String> {
    raw.map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

pub fn default_business_connection_id(bot_id: i64, user_id: i64) -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("business:{}:{}", bot_id, user_id);
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AQAD{}", &hexed[..28])
}

pub fn default_business_accepted_gift_types() -> AcceptedGiftTypes {
    AcceptedGiftTypes {
        unlimited_gifts: true,
        limited_gifts: true,
        unique_gifts: true,
        premium_subscription: true,
        gifts_from_channels: true,
    }
}

pub fn parse_business_accepted_gift_types_json(raw: Option<&str>) -> AcceptedGiftTypes {
    raw.and_then(|value| serde_json::from_str::<AcceptedGiftTypes>(value).ok())
        .unwrap_or_else(default_business_accepted_gift_types)
}

pub fn default_business_bot_rights() -> BusinessBotRights {
    BusinessBotRights {
        can_reply: Some(true),
        can_read_messages: Some(true),
        can_delete_sent_messages: Some(true),
        can_delete_all_messages: Some(true),
        can_edit_name: Some(true),
        can_edit_bio: Some(true),
        can_edit_profile_photo: Some(true),
        can_edit_username: Some(true),
        can_change_gift_settings: Some(true),
        can_view_gifts_and_stars: Some(true),
        can_convert_gifts_to_stars: Some(true),
        can_transfer_and_upgrade_gifts: Some(true),
        can_transfer_stars: Some(true),
        can_manage_stories: Some(true),
    }
}

pub fn parse_business_bot_rights_json(raw: &str) -> BusinessBotRights {
    serde_json::from_str::<BusinessBotRights>(raw).unwrap_or_else(|_| default_business_bot_rights())
}

pub fn load_sim_business_connection_by_id(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    connection_id: &str,
) -> Result<Option<SimBusinessConnectionRecord>, ApiError> {
    conn.query_row(
        "SELECT connection_id, user_id, user_chat_id, rights_json, is_enabled,
                gift_settings_show_button, gift_settings_types_json, star_balance,
                created_at, updated_at
         FROM sim_business_connections
         WHERE bot_id = ?1 AND LOWER(connection_id) = LOWER(?2)",
        params![bot_id, connection_id],
        |row| {
            Ok(SimBusinessConnectionRecord {
                connection_id: row.get(0)?,
                user_id: row.get(1)?,
                user_chat_id: row.get(2)?,
                rights_json: row.get(3)?,
                is_enabled: row.get::<_, i64>(4)? == 1,
                gift_settings_show_button: row.get::<_, i64>(5)? == 1,
                gift_settings_types_json: row.get(6)?,
                star_balance: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn load_sim_business_connection_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<Option<SimBusinessConnectionRecord>, ApiError> {
    conn.query_row(
        "SELECT connection_id, user_id, user_chat_id, rights_json, is_enabled,
                gift_settings_show_button, gift_settings_types_json, star_balance,
                created_at, updated_at
         FROM sim_business_connections
         WHERE bot_id = ?1 AND user_id = ?2
         ORDER BY updated_at DESC
         LIMIT 1",
        params![bot_id, user_id],
        |row| {
            Ok(SimBusinessConnectionRecord {
                connection_id: row.get(0)?,
                user_id: row.get(1)?,
                user_chat_id: row.get(2)?,
                rights_json: row.get(3)?,
                is_enabled: row.get::<_, i64>(4)? == 1,
                gift_settings_show_button: row.get::<_, i64>(5)? == 1,
                gift_settings_types_json: row.get(6)?,
                star_balance: row.get(7)?,
                created_at: row.get(8)?,
                updated_at: row.get(9)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn upsert_sim_business_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    connection_id: &str,
    user_id: i64,
    user_chat_id: i64,
    rights: &BusinessBotRights,
    is_enabled: bool,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    let now = Utc::now().timestamp();
    let rights_json = serde_json::to_string(rights).map_err(ApiError::internal)?;

    if let Some(existing) = load_sim_business_connection_for_user(conn, bot_id, user_id)? {
        conn.execute(
            "UPDATE sim_business_connections
             SET connection_id = ?1,
                 user_chat_id = ?2,
                 rights_json = ?3,
                 is_enabled = ?4,
                 updated_at = ?5
             WHERE bot_id = ?6 AND connection_id = ?7",
            params![
                connection_id,
                user_chat_id,
                rights_json,
                if is_enabled { 1 } else { 0 },
                now,
                bot_id,
                existing.connection_id,
            ],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT INTO sim_business_connections
             (bot_id, connection_id, user_id, user_chat_id, rights_json, is_enabled, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7)",
            params![
                bot_id,
                connection_id,
                user_id,
                user_chat_id,
                rights_json,
                if is_enabled { 1 } else { 0 },
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    load_sim_business_connection_by_id(conn, bot_id, connection_id)?
        .ok_or_else(|| ApiError::internal("failed to persist business connection"))
}

pub fn load_sim_business_profile(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<Option<SimBusinessProfileRecord>, ApiError> {
    conn.query_row(
        "SELECT last_name, bio, profile_photo_file_id, public_profile_photo_file_id
         FROM sim_business_account_profiles
         WHERE bot_id = ?1 AND user_id = ?2",
        params![bot_id, user_id],
        |row| {
            Ok(SimBusinessProfileRecord {
                last_name: row.get(0)?,
                bio: row.get(1)?,
                profile_photo_file_id: row.get(2)?,
                public_profile_photo_file_id: row.get(3)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn build_business_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    record: &SimBusinessConnectionRecord,
) -> Result<BusinessConnection, ApiError> {
    let user_record = users::ensure_sim_user_record(conn, record.user_id)?;
    let profile = load_sim_business_profile(conn, bot_id, record.user_id)?;

    let user = User {
        id: user_record.id,
        is_bot: false,
        first_name: user_record.first_name,
        last_name: profile
            .as_ref()
            .and_then(|item| item.last_name.clone())
            .or(user_record.last_name),
        username: user_record.username,
        language_code: None,
        is_premium: Some(user_record.is_premium),
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

    Ok(BusinessConnection {
        id: record.connection_id.clone(),
        user,
        user_chat_id: record.user_chat_id,
        date: record.created_at,
        rights: Some(parse_business_bot_rights_json(&record.rights_json)),
        is_enabled: record.is_enabled,
    })
}

pub fn business_right_enabled(
    rights: &Option<BusinessBotRights>,
    resolver: impl Fn(&BusinessBotRights) -> Option<bool>,
) -> bool {
    rights
        .as_ref()
        .and_then(resolver)
        .unwrap_or(false)
}

pub fn ensure_business_right(
    connection: &BusinessConnection,
    resolver: impl Fn(&BusinessBotRights) -> Option<bool>,
    message: &str,
) -> Result<(), ApiError> {
    if business_right_enabled(&connection.rights, resolver) {
        return Ok(());
    }

    Err(ApiError::bad_request(message))
}

pub fn handle_sim_set_business_connection(
    state: &Data<AppState>,
    token: &str,
    body: SimSetBusinessConnectionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let connection_id = normalize_business_connection_id(body.business_connection_id.as_deref())
        .unwrap_or_else(|| default_business_connection_id(bot.id, user.id));
    let rights = body.rights.unwrap_or_else(default_business_bot_rights);
    let enabled = body.enabled.unwrap_or(true);

    let record = upsert_sim_business_connection(
        &mut conn,
        bot.id,
        &connection_id,
        user.id,
        user.id,
        &rights,
        enabled,
    )?;

    let connection = build_business_connection(&mut conn, bot.id, &record)?;
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: Some(connection.clone()),
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

    webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    serde_json::to_value(connection).map_err(ApiError::internal)
}

pub fn handle_sim_remove_business_connection(
    state: &Data<AppState>,
    token: &str,
    body: SimRemoveBusinessConnectionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let requested_connection_id = normalize_business_connection_id(body.business_connection_id.as_deref());
    let record = if let Some(connection_id) = requested_connection_id.as_deref() {
        if let Some(found) = load_sim_business_connection_by_id(&mut conn, bot.id, connection_id)? {
            found
        } else if let Some(user_id) = body.user_id {
            load_sim_business_connection_for_user(&mut conn, bot.id, user_id)?
                .ok_or_else(|| ApiError::not_found("business connection not found"))?
        } else {
            return Err(ApiError::not_found("business connection not found"));
        }
    } else {
        let user_id = body
            .user_id
            .ok_or_else(|| ApiError::bad_request("user_id is required when business_connection_id is omitted"))?;
        load_sim_business_connection_for_user(&mut conn, bot.id, user_id)?
            .ok_or_else(|| ApiError::not_found("business connection not found"))?
    };

    if let Some(user_id) = body.user_id {
        if record.user_id != user_id {
            return Err(ApiError::bad_request(
                "business connection does not belong to the provided user_id",
            ));
        }
    }

    let connection = build_business_connection(&mut conn, bot.id, &record)?;

    conn.execute(
        "DELETE FROM sim_business_read_messages WHERE bot_id = ?1 AND connection_id = ?2",
        params![bot.id, &record.connection_id],
    )
    .map_err(ApiError::internal)?;
    conn.execute(
        "DELETE FROM sim_business_connections WHERE bot_id = ?1 AND connection_id = ?2",
        params![bot.id, &record.connection_id],
    )
    .map_err(ApiError::internal)?;

    let mut disabled_connection = connection;
    disabled_connection.is_enabled = false;

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: Some(disabled_connection),
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
    webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "deleted": true,
        "business_connection_id": record.connection_id,
        "user_id": record.user_id,
    }))
}

pub fn extract_business_profile_photo_media_input(raw: &Value) -> Result<Value, ApiError> {
    let Some(obj) = raw.as_object() else {
        return Err(ApiError::bad_request("photo must be a valid InputProfilePhoto object"));
    };

    if let Some(photo) = obj.get("photo").and_then(Value::as_str) {
        let trimmed = photo.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("photo is empty"));
        }
        return Ok(Value::String(trimmed.to_string()));
    }

    if let Some(animation) = obj.get("animation").and_then(Value::as_str) {
        let trimmed = animation.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("animation is empty"));
        }
        return Ok(Value::String(trimmed.to_string()));
    }

    Err(ApiError::bad_request("photo must contain photo or animation"))
}

pub fn load_business_connection_or_404(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    let normalized = normalize_business_connection_id(Some(business_connection_id))
        .ok_or_else(|| ApiError::bad_request("business_connection_id is empty"))?;
    load_sim_business_connection_by_id(conn, bot_id, &normalized)?
        .ok_or_else(|| ApiError::not_found("business connection not found"))
}

pub fn resolve_story_business_connection_for_request(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    raw_business_connection_id: Option<&str>,
) -> Result<(SimBusinessConnectionRecord, BusinessConnection), ApiError> {
    let normalized_connection_id = normalize_business_connection_id(raw_business_connection_id);
    let record = if let Some(connection_id) = normalized_connection_id.as_deref() {
        load_business_connection_or_404(conn, bot_id, connection_id)?
    } else {
        let actor_user_id = current_request_actor_user_id().ok_or_else(|| {
            ApiError::bad_request(
                "business_connection_id is required when actor user context is unavailable",
            )
        })?;
        let actor_user = users::ensure_user(conn, Some(actor_user_id), None, None)?;
        let loaded = match load_sim_business_connection_for_user(conn, bot_id, actor_user.id)? {
            Some(existing) => existing,
            None => {
                let default_connection_id = default_business_connection_id(bot_id, actor_user.id);
                upsert_sim_business_connection(
                    conn,
                    bot_id,
                    &default_connection_id,
                    actor_user.id,
                    actor_user.id,
                    &default_business_bot_rights(),
                    true,
                )?
            }
        };

        let mut rights = parse_business_bot_rights_json(&loaded.rights_json);
        let mut should_upsert = false;
        if rights.can_manage_stories != Some(true) {
            rights.can_manage_stories = Some(true);
            should_upsert = true;
        }
        if !loaded.is_enabled {
            should_upsert = true;
        }

        if should_upsert {
            upsert_sim_business_connection(
                conn,
                bot_id,
                &loaded.connection_id,
                loaded.user_id,
                loaded.user_chat_id,
                &rights,
                true,
            )?
        } else {
            loaded
        }
    };

    if !record.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }

    let connection = build_business_connection(conn, bot_id, &record)?;
    if normalized_connection_id.is_some() {
        ensure_business_right(
            &connection,
            |rights| rights.can_manage_stories,
            "not enough rights to manage stories",
        )?;
    }

    Ok((record, connection))
}

pub fn resolve_outbound_business_connection_for_bot_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat: &Chat,
    raw_business_connection_id: Option<&str>,
) -> Result<Option<String>, ApiError> {
    let Some(connection_id) = normalize_business_connection_id(raw_business_connection_id) else {
        return Ok(None);
    };

    let record = load_business_connection_or_404(conn, bot_id, &connection_id)?;
    if !record.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }

    if chat.r#type != "private" || chat.id != record.user_chat_id {
        return Err(ApiError::bad_request(
            "business connection does not match target private chat",
        ));
    }

    let connection = build_business_connection(conn, bot_id, &record)?;
    ensure_business_right(
        &connection,
        |rights| rights.can_reply,
        "not enough rights to send business messages",
    )?;

    Ok(Some(record.connection_id))
}

pub fn ensure_sim_business_connection_for_user(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    user_id: i64,
) -> Result<SimBusinessConnectionRecord, ApiError> {
    if let Some(existing) = load_sim_business_connection_for_user(conn, bot_id, user_id)? {
        return Ok(existing);
    }

    let user = users::ensure_user(conn, Some(user_id), None, None)?;
    let connection_id = default_business_connection_id(bot_id, user.id);
    upsert_sim_business_connection(
        conn,
        bot_id,
        &connection_id,
        user.id,
        user.id,
        &default_business_bot_rights(),
        true,
    )
}