use actix_web::web::Data;
use chrono::Utc;
use reqwest::Url;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::net::IpAddr;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    CloseRequest, DeleteMyCommandsRequest, DeleteWebhookRequest,
    GetWebhookInfoRequest, GetMeRequest, GetMyCommandsRequest, GetMyDefaultAdministratorRightsRequest,
    GetMyDescriptionRequest, GetMyNameRequest, GetMyShortDescriptionRequest, GetMyStarBalanceRequest,
    GetManagedBotTokenRequest, GetUpdatesRequest, RemoveMyProfilePhotoRequest,
    ReplaceManagedBotTokenRequest, SetMyCommandsRequest, SetMyDefaultAdministratorRightsRequest,
    SetMyDescriptionRequest, SetMyNameRequest, SetMyProfilePhotoRequest,
    SetMyShortDescriptionRequest, SetWebhookRequest, LogOutRequest,
};

use crate::generated::types::{
    WebhookInfo, User, BotCommand, ChatAdministratorRights, BotDescription,
    BotName, BotShortDescription, StarAmount, Update, ManagedBotUpdated
};

use crate::handlers::client::{bot, channels, messages, users, webhook};

use crate::handlers::{parse_request, sql_value_to_rusqlite};

const DEFAULT_EXCLUDED_UPDATE_TYPES: [&str; 3] = [
    "chat_member",
    "message_reaction",
    "message_reaction_count",
];

fn normalize_allowed_updates(raw_values: Option<Vec<String>>) -> Result<Option<Vec<String>>, ApiError> {
    let Some(values) = raw_values else {
        return Ok(None);
    };

    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::new();
    for raw in values {
        let value = raw.trim().to_ascii_lowercase();
        if value.is_empty() {
            return Err(ApiError::bad_request(
                "allowed_updates must not contain empty values",
            ));
        }

        if seen.insert(value.clone()) {
            normalized.push(value);
        }
    }

    Ok(Some(normalized))
}

fn is_local_webhook_host(host: &str) -> bool {
    let normalized = host.trim().trim_matches('.').to_ascii_lowercase();
    if normalized == "localhost" || normalized.ends_with(".localhost") {
        return true;
    }

    if let Ok(ip) = normalized.parse::<IpAddr>() {
        return match ip {
            IpAddr::V4(value) => value.is_loopback() || value.is_private() || value.is_link_local(),
            IpAddr::V6(value) => {
                value.is_loopback() || value.is_unique_local() || value.is_unicast_link_local()
            }
        };
    }

    false
}

fn parse_allowed_updates_json(raw_json: Option<String>) -> Result<Option<Vec<String>>, ApiError> {
    let Some(raw) = raw_json.map(|value| value.trim().to_string()).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let parsed: Vec<String> = serde_json::from_str(&raw).map_err(ApiError::internal)?;
    normalize_allowed_updates(Some(parsed))
}

fn load_polling_allowed_updates(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
) -> Result<Option<Vec<String>>, ApiError> {
    let raw_allowed_updates: Option<Option<String>> = conn
        .query_row(
            "SELECT polling_allowed_updates_json FROM sim_bot_runtime_settings WHERE bot_id = ?1",
            params![bot_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    parse_allowed_updates_json(raw_allowed_updates.flatten())
}

fn store_polling_allowed_updates(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    allowed_updates: &[String],
) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let privacy_mode_enabled = conn
        .query_row(
            "SELECT privacy_mode_enabled FROM sim_bot_runtime_settings WHERE bot_id = ?1",
            params![bot_id],
            |row| row.get::<_, i64>(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .unwrap_or(1);

    conn.execute(
        "INSERT INTO sim_bot_runtime_settings
         (bot_id, privacy_mode_enabled, polling_allowed_updates_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id)
         DO UPDATE SET
            polling_allowed_updates_json = excluded.polling_allowed_updates_json,
            updated_at = excluded.updated_at",
        params![
            bot_id,
            privacy_mode_enabled,
            serde_json::to_string(allowed_updates).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

fn detect_update_type(update: &Value) -> Option<&'static str> {
    if update.get("message").is_some() {
        return Some("message");
    }
    if update.get("edited_message").is_some() {
        return Some("edited_message");
    }
    if update.get("channel_post").is_some() {
        return Some("channel_post");
    }
    if update.get("edited_channel_post").is_some() {
        return Some("edited_channel_post");
    }
    if update.get("business_connection").is_some() {
        return Some("business_connection");
    }
    if update.get("business_message").is_some() {
        return Some("business_message");
    }
    if update.get("edited_business_message").is_some() {
        return Some("edited_business_message");
    }
    if update.get("deleted_business_messages").is_some() {
        return Some("deleted_business_messages");
    }
    if update.get("message_reaction").is_some() {
        return Some("message_reaction");
    }
    if update.get("message_reaction_count").is_some() {
        return Some("message_reaction_count");
    }
    if update.get("inline_query").is_some() {
        return Some("inline_query");
    }
    if update.get("chosen_inline_result").is_some() {
        return Some("chosen_inline_result");
    }
    if update.get("callback_query").is_some() {
        return Some("callback_query");
    }
    if update.get("shipping_query").is_some() {
        return Some("shipping_query");
    }
    if update.get("pre_checkout_query").is_some() {
        return Some("pre_checkout_query");
    }
    if update.get("purchased_paid_media").is_some() {
        return Some("purchased_paid_media");
    }
    if update.get("poll").is_some() {
        return Some("poll");
    }
    if update.get("poll_answer").is_some() {
        return Some("poll_answer");
    }
    if update.get("my_chat_member").is_some() {
        return Some("my_chat_member");
    }
    if update.get("chat_member").is_some() {
        return Some("chat_member");
    }
    if update.get("chat_join_request").is_some() {
        return Some("chat_join_request");
    }
    if update.get("chat_boost").is_some() {
        return Some("chat_boost");
    }
    if update.get("removed_chat_boost").is_some() {
        return Some("removed_chat_boost");
    }
    if update.get("managed_bot").is_some() {
        return Some("managed_bot");
    }

    None
}

fn is_update_type_allowed(update_type: Option<&str>, allowed_updates: Option<&[String]>) -> bool {
    let Some(update_type) = update_type else {
        return true;
    };

    if let Some(allowed) = allowed_updates {
        if !allowed.is_empty() {
            return allowed.iter().any(|item| item == update_type);
        }
    }

    !DEFAULT_EXCLUDED_UPDATE_TYPES.contains(&update_type)
}

fn delete_updates_by_ids(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update_ids: &[i64],
) -> Result<(), ApiError> {
    if update_ids.is_empty() {
        return Ok(());
    }

    let placeholders = std::iter::repeat("?")
        .take(update_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "DELETE FROM updates WHERE bot_id = ? AND update_id IN ({})",
        placeholders
    );

    let mut bind_values = Vec::with_capacity(1 + update_ids.len());
    bind_values.push(Value::from(bot_id));
    for id in update_ids {
        bind_values.push(Value::from(*id));
    }

    let mut stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
    stmt.execute(rusqlite::params_from_iter(
        bind_values.iter().map(sql_value_to_rusqlite),
    ))
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn handle_close(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: CloseRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let _bot = ensure_bot(&mut conn, token)?;

    Ok(json!(true))
}

pub fn handle_delete_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMyCommandsRequest = parse_request(params)?;
    let scope_key = bot::normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_commands_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_bot_commands WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ?3",
        params![bot.id, &scope_key, &language_code],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_delete_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteWebhookRequest = parse_request(params)?;
    let drop_pending_updates = request.drop_pending_updates.unwrap_or(false);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;

    if drop_pending_updates {
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND bot_visible = 1",
            params![bot.id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "UPDATE updates SET webhook_pending = 0 WHERE bot_id = ?1",
            params![bot.id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(Value::Bool(true))
}

pub fn handle_get_webhook_info(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetWebhookInfoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, Option<String>, Option<i64>, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT url, ip_address, max_connections, allowed_updates_json, has_custom_certificate
             FROM webhooks
             WHERE bot_id = ?1",
            params![bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let pending_update_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM updates WHERE bot_id = ?1 AND bot_visible = 1 AND webhook_pending = 1",
            params![bot.id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let (url, ip_address, max_connections, allowed_updates, has_custom_certificate) = if let Some((url, ip_address, max_connections, allowed_updates_json, has_custom_certificate)) = row {
        let normalized_ip = ip_address
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        let parsed_allowed_updates = parse_allowed_updates_json(allowed_updates_json)?;
        (
            url,
            normalized_ip,
            max_connections,
            parsed_allowed_updates,
            has_custom_certificate.unwrap_or(0) != 0,
        )
    } else {
        (String::new(), None, None, None, false)
    };

    serde_json::to_value(WebhookInfo {
        url,
        has_custom_certificate,
        pending_update_count,
        ip_address,
        last_error_date: None,
        last_error_message: None,
        last_synchronization_error_date: None,
        max_connections,
        allowed_updates,
    })
    .map_err(ApiError::internal)
}

pub fn handle_get_me(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let _request: GetMeRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let user = User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name,
        last_name: None,
        username: Some(bot.username),
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
    };

    Ok(serde_json::to_value(user).map_err(ApiError::internal)?)
}

pub fn handle_get_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyCommandsRequest = parse_request(params)?;
    let scope_key = bot::normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_commands_storage(&mut conn)?;

    let mut commands_json: Option<String> = conn
        .query_row(
            "SELECT commands_json
             FROM sim_bot_commands
             WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ?3",
            params![bot.id, &scope_key, &language_code],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if commands_json.is_none() && !language_code.is_empty() {
        commands_json = conn
            .query_row(
                "SELECT commands_json
                 FROM sim_bot_commands
                 WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ''",
                params![bot.id, &scope_key],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
    }

    let commands = commands_json
        .map(|raw| serde_json::from_str::<Vec<BotCommand>>(&raw).map_err(ApiError::internal))
        .transpose()?
        .unwrap_or_default();

    Ok(serde_json::to_value(commands).map_err(ApiError::internal)?)
}

pub fn handle_get_my_default_administrator_rights(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyDefaultAdministratorRightsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

    let for_channels = request.for_channels.unwrap_or(false);
    let raw_rights: Option<Option<String>> = conn
        .query_row(
            "SELECT rights_json
             FROM sim_bot_default_admin_rights
             WHERE bot_id = ?1 AND for_channels = ?2",
            params![bot.id, if for_channels { 1 } else { 0 }],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let rights = raw_rights
        .flatten()
        .and_then(|raw| serde_json::from_str::<ChatAdministratorRights>(&raw).ok())
        .unwrap_or_else(|| bot::default_bot_administrator_rights(for_channels));

    Ok(serde_json::to_value(rights).map_err(ApiError::internal)?)
}

pub fn handle_get_my_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyDescriptionRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let description = bot::load_bot_profile_text_value(&mut conn, bot.id, &language_code, "description")?
        .unwrap_or_default();

    Ok(serde_json::to_value(BotDescription { description }).map_err(ApiError::internal)?)
}

pub fn handle_get_my_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyNameRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let name = bot::load_bot_profile_text_value(&mut conn, bot.id, &language_code, "name")?
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or(bot.first_name);

    Ok(serde_json::to_value(BotName { name }).map_err(ApiError::internal)?)
}

pub fn handle_get_my_short_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyShortDescriptionRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let short_description =
        bot::load_bot_profile_text_value(&mut conn, bot.id, &language_code, "short_description")?
            .unwrap_or_default();

    Ok(serde_json::to_value(BotShortDescription { short_description }).map_err(ApiError::internal)?)
}

pub fn handle_get_my_star_balance(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetMyStarBalanceRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let balance: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount), 0) FROM star_transactions_ledger WHERE bot_id = ?1",
            params![bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    let result = StarAmount {
        amount: balance,
        nanostar_amount: None,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

pub fn handle_get_managed_bot_token(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetManagedBotTokenRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    let record = bot::ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    Ok(Value::String(record.managed_token))
}

pub fn handle_get_updates(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetUpdatesRequest = parse_request(params)?;
    let requested_allowed_updates = normalize_allowed_updates(request.allowed_updates)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let webhook_url: Option<String> = conn
        .query_row(
            "SELECT url FROM webhooks WHERE bot_id = ?1",
            params![bot.id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if webhook_url.is_some() {
        return Err(ApiError::conflict(
            "can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first",
        ));
    }

    if let Some(allowed_updates) = requested_allowed_updates.as_ref() {
        store_polling_allowed_updates(&mut conn, bot.id, allowed_updates)?;
    }

    let effective_allowed_updates = if let Some(allowed_updates) = requested_allowed_updates {
        Some(allowed_updates)
    } else {
        load_polling_allowed_updates(&mut conn, bot.id)?
    };

    let mut offset = request.offset.unwrap_or(0);
    let mut limit = request.limit.unwrap_or(100);
    if limit <= 0 {
        limit = 1;
    }
    if limit > 100 {
        limit = 100;
    }

    if offset < 0 {
        let mut ids_stmt = conn
            .prepare(
                "SELECT update_id
                 FROM updates
                 WHERE bot_id = ?1 AND bot_visible = 1
                 ORDER BY update_id ASC",
            )
            .map_err(ApiError::internal)?;
        let ids_rows = ids_stmt
            .query_map(params![bot.id], |row| row.get::<_, i64>(0))
            .map_err(ApiError::internal)?;
        let update_ids = ids_rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::internal)?;

        if !update_ids.is_empty() {
            let from_tail = offset.unsigned_abs() as usize;
            let start_index = update_ids.len().saturating_sub(from_tail);
            let start_update_id = update_ids[start_index];
            conn.execute(
                "DELETE FROM updates
                 WHERE bot_id = ?1 AND bot_visible = 1 AND update_id < ?2",
                params![bot.id, start_update_id],
            )
            .map_err(ApiError::internal)?;
            offset = start_update_id;
        } else {
            offset = 1;
        }
    }

    if offset > 0 {
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND update_id < ?2",
            params![bot.id, offset],
        )
        .map_err(ApiError::internal)?;
    }

    let mut stmt = conn
        .prepare(
            "SELECT update_id, update_json FROM updates
             WHERE bot_id = ?1 AND update_id >= ?2 AND bot_visible = 1
             ORDER BY update_id ASC
             LIMIT ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, offset.max(1), limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?;

    let fetched_rows: Vec<(i64, String)> = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(stmt);

    let mut updates = Vec::new();
    let mut discarded_update_ids = Vec::new();
    for (update_id, raw) in fetched_rows {
        let mut parsed: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;
        channels::enrich_channel_post_payloads(&mut conn, bot.id, &mut parsed)?;

        if messages::update_targets_deleted_message(&mut conn, bot.id, &parsed)? {
            discarded_update_ids.push(update_id);
            continue;
        }

        if !is_update_type_allowed(
            detect_update_type(&parsed),
            effective_allowed_updates.as_deref(),
        ) {
            discarded_update_ids.push(update_id);
            continue;
        }

        updates.push(parsed);
    }

    delete_updates_by_ids(&mut conn, bot.id, &discarded_update_ids)?;

    Ok(Value::Array(updates))
}

pub fn handle_remove_my_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: RemoveMyProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_photos_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_bot_profile_photos WHERE bot_id = ?1",
        params![bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_replace_managed_bot_token(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReplaceManagedBotTokenRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    let _ = bot::ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    let record = bot::rotate_managed_bot_token(&mut conn, bot.id, owner.id)?;
    let new_token = record.managed_token.clone();

    let update_value = serde_json::to_value(Update {
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
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
        managed_bot: Some(ManagedBotUpdated {
            user: bot::build_user_with_manage_bots(&owner),
            bot: bot::managed_bot_user_from_record(&record),
        }),
    })
    .map_err(ApiError::internal)?;

    webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(Value::String(new_token))
}

pub fn handle_set_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyCommandsRequest = parse_request(params)?;
    let normalized_commands = bot::normalize_bot_commands_payload(&request.commands)?;
    let scope_key = bot::normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_commands_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_commands (bot_id, scope_key, language_code, commands_json, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, scope_key, language_code)
         DO UPDATE SET
            commands_json = excluded.commands_json,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            &scope_key,
            &language_code,
            serde_json::to_string(&normalized_commands).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_my_default_administrator_rights(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyDefaultAdministratorRightsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let for_channels = request.for_channels.unwrap_or(false);
    let rights_json = request
        .rights
        .as_ref()
        .map(|rights| serde_json::to_string(rights).map_err(ApiError::internal))
        .transpose()?;

    conn.execute(
        "INSERT INTO sim_bot_default_admin_rights (bot_id, for_channels, rights_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, for_channels)
         DO UPDATE SET
            rights_json = excluded.rights_json,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            if for_channels { 1 } else { 0 },
            rights_json,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_my_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyDescriptionRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_description = request
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(description) = normalized_description.as_ref() {
        if description.chars().count() > 512 {
            return Err(ApiError::bad_request(
                "description must be at most 512 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, description, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            description = excluded.description,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_my_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyNameRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_name = request
        .name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(name) = normalized_name.as_ref() {
        if name.chars().count() > 64 {
            return Err(ApiError::bad_request(
                "name must be at most 64 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, name, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            name = excluded.name,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_name.clone(), now],
    )
    .map_err(ApiError::internal)?;

    if language_code.is_empty() {
        if let Some(name) = normalized_name.as_ref() {
            conn.execute(
                "UPDATE bots SET first_name = ?1 WHERE id = ?2",
                params![name, bot.id],
            )
            .map_err(ApiError::internal)?;
        }
    }

    Ok(json!(true))
}

pub fn handle_set_my_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyProfilePhotoRequest = parse_request(params)?;
    let (media_kind, media_input) = bot::extract_bot_profile_photo_media_input(&request.photo.extra)?;
    let file = messages::resolve_media_file(state, token, &media_input, media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_photos_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_photos (bot_id, file_id, media_kind, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id)
         DO UPDATE SET
            file_id = excluded.file_id,
            media_kind = excluded.media_kind,
            updated_at = excluded.updated_at",
        params![bot.id, file.file_id, media_kind, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_my_short_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyShortDescriptionRequest = parse_request(params)?;
    let language_code = bot::normalize_bot_language_code(request.language_code.as_deref())?;
    let normalized_short_description = request
        .short_description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(short_description) = normalized_short_description.as_ref() {
        if short_description.chars().count() > 120 {
            return Err(ApiError::bad_request(
                "short_description must be at most 120 characters",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    bot::ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_bot_profile_texts (bot_id, language_code, short_description, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, language_code)
         DO UPDATE SET
            short_description = excluded.short_description,
            updated_at = excluded.updated_at",
        params![bot.id, &language_code, normalized_short_description, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetWebhookRequest = parse_request(params)?;
    let drop_pending_updates = request.drop_pending_updates.unwrap_or(false);
    let normalized_allowed_updates = normalize_allowed_updates(request.allowed_updates)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let webhook_url = request.url.trim();
    if webhook_url.is_empty() {
        conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
            .map_err(ApiError::internal)?;

        if drop_pending_updates {
            conn.execute(
                "DELETE FROM updates WHERE bot_id = ?1 AND bot_visible = 1",
                params![bot.id],
            )
            .map_err(ApiError::internal)?;
        } else {
            conn.execute(
                "UPDATE updates SET webhook_pending = 0 WHERE bot_id = ?1",
                params![bot.id],
            )
            .map_err(ApiError::internal)?;
        }

        return Ok(Value::Bool(true));
    }

    let parsed_url = Url::parse(webhook_url)
        .map_err(|_| ApiError::bad_request("bad webhook: failed to parse URL"))?;
    let host = parsed_url
        .host_str()
        .ok_or_else(|| ApiError::bad_request("bad webhook: URL host is missing"))?;
    let is_local_host = is_local_webhook_host(host);

    match parsed_url.scheme() {
        "https" => {}
        "http" if is_local_host => {}
        "http" => {
            return Err(ApiError::bad_request(
                "bad webhook: HTTP is allowed only for localhost or local IPs",
            ));
        }
        _ => {
            return Err(ApiError::bad_request(
                "bad webhook: URL must use HTTPS or HTTP",
            ));
        }
    }

    if let Some(port) = parsed_url.port() {
        let supported_public_port = matches!(port, 443 | 80 | 88 | 8443);
        if !is_local_host && !supported_public_port {
            return Err(ApiError::bad_request(
                "bad webhook: port must be one of 443, 80, 88, or 8443",
            ));
        }
    }

    let secret_token = request.secret_token.unwrap_or_default().trim().to_string();
    if !secret_token.is_empty() {
        if secret_token.len() > 256 {
            return Err(ApiError::bad_request("bad webhook: secret token is too long"));
        }
        if !secret_token
            .chars()
            .all(|value| value.is_ascii_alphanumeric() || value == '_' || value == '-')
        {
            return Err(ApiError::bad_request("bad webhook: secret token contains invalid characters"));
        }
    }

    let max_connections = request.max_connections.unwrap_or(40);
    if !(1..=100).contains(&max_connections) {
        return Err(ApiError::bad_request("bad webhook: max_connections must be between 1 and 100"));
    }

    let ip_address = request.ip_address.unwrap_or_default().trim().to_string();
    if !ip_address.is_empty() && ip_address.parse::<std::net::IpAddr>().is_err() {
        return Err(ApiError::bad_request("bad webhook: invalid ip_address"));
    }

    let previous_row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT allowed_updates_json, has_custom_certificate FROM webhooks WHERE bot_id = ?1",
            params![bot.id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let allowed_updates_json = if let Some(allowed_updates) = normalized_allowed_updates.as_ref() {
        Some(serde_json::to_string(allowed_updates).map_err(ApiError::internal)?)
    } else {
        previous_row.as_ref().and_then(|(allowed_updates_json, _)| allowed_updates_json.clone())
    };

    let has_custom_certificate = if request.certificate.is_some() {
        1
    } else {
        previous_row
            .as_ref()
            .and_then(|(_, has_custom_certificate)| *has_custom_certificate)
            .unwrap_or(0)
    };

    conn.execute(
        "INSERT INTO webhooks
         (bot_id, url, secret_token, max_connections, ip_address, allowed_updates_json, has_custom_certificate)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
         ON CONFLICT(bot_id) DO UPDATE SET
            url = excluded.url,
            secret_token = excluded.secret_token,
            max_connections = excluded.max_connections,
            ip_address = excluded.ip_address,
            allowed_updates_json = excluded.allowed_updates_json,
            has_custom_certificate = excluded.has_custom_certificate",
        params![
            bot.id,
            webhook_url,
            secret_token,
            max_connections,
            ip_address,
            allowed_updates_json,
            has_custom_certificate,
        ],
    )
    .map_err(ApiError::internal)?;

    if drop_pending_updates {
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND bot_visible = 1",
            params![bot.id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "UPDATE updates
             SET webhook_pending = 1
             WHERE bot_id = ?1 AND bot_visible = 1 AND webhook_pending = 0",
            params![bot.id],
        )
        .map_err(ApiError::internal)?;

        webhook::schedule_pending_retry(state, bot.id);
    }

    Ok(Value::Bool(true))
}

pub fn handle_log_out(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: LogOutRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;
    conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;

    Ok(json!(true))
}
