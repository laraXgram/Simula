use super::*;
use crate::generated::methods::{
    CloseRequest, DeleteMyCommandsRequest, DeleteWebhookRequest,
    GetWebhookInfoRequest, GetMeRequest, GetMyCommandsRequest, GetMyDefaultAdministratorRightsRequest,
    GetMyDescriptionRequest, GetMyNameRequest, GetMyShortDescriptionRequest, GetMyStarBalanceRequest,
    GetManagedBotTokenRequest, GetUpdatesRequest, RemoveMyProfilePhotoRequest,
    ReplaceManagedBotTokenRequest, SetMyCommandsRequest, SetMyDefaultAdministratorRightsRequest,
    SetMyDescriptionRequest, SetMyNameRequest, SetMyProfilePhotoRequest,
    SetMyShortDescriptionRequest, SetWebhookRequest, LogOutRequest,
};

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
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

    conn.execute(
        "DELETE FROM sim_bot_commands WHERE bot_id = ?1 AND scope_key = ?2 AND language_code = ?3",
        params![bot.id, &scope_key, &language_code],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_delete_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteWebhookRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    conn.execute("DELETE FROM webhooks WHERE bot_id = ?1", params![bot.id])
        .map_err(ApiError::internal)?;

    if request.drop_pending_updates.unwrap_or(false) {
        conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
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

    let row: Option<(String, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT url, ip_address, max_connections FROM webhooks WHERE bot_id = ?1",
            params![bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let pending_update_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM updates WHERE bot_id = ?1 AND bot_visible = 1",
            params![bot.id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    let (url, ip_address, max_connections) = if let Some((url, ip_address, max_connections)) = row {
        let normalized_ip = ip_address
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string);
        (url, normalized_ip, max_connections)
    } else {
        (String::new(), None, None)
    };

    serde_json::to_value(WebhookInfo {
        url,
        has_custom_certificate: false,
        pending_update_count,
        ip_address,
        last_error_date: None,
        last_error_message: None,
        last_synchronization_error_date: None,
        max_connections,
        allowed_updates: None,
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
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

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
    ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

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
        .unwrap_or_else(|| default_bot_administrator_rights(for_channels));

    Ok(serde_json::to_value(rights).map_err(ApiError::internal)?)
}

pub fn handle_get_my_description(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyDescriptionRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let description = load_bot_profile_text_value(&mut conn, bot.id, &language_code, "description")?
        .unwrap_or_default();

    Ok(serde_json::to_value(BotDescription { description }).map_err(ApiError::internal)?)
}

pub fn handle_get_my_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetMyNameRequest = parse_request(params)?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let name = load_bot_profile_text_value(&mut conn, bot.id, &language_code, "name")?
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
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

    let short_description =
        load_bot_profile_text_value(&mut conn, bot.id, &language_code, "short_description")?
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

    Ok(json!({
        "amount": balance,
    }))
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
    ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = ensure_sim_user_record(&mut conn, request.user_id)?;
    let _ = ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    Ok(json!(true))
}

pub fn handle_get_updates(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetUpdatesRequest = parse_request(params)?;

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
            "can't use getUpdates method while webhook is active",
        ));
    }

    let offset = request.offset.unwrap_or(0);
    let mut limit = request.limit.unwrap_or(100);
    if limit <= 0 {
        limit = 1;
    }
    if limit > 100 {
        limit = 100;
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
    let mut stale_update_ids = Vec::new();
    for (update_id, raw) in fetched_rows {
        let mut parsed: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;
        enrich_channel_post_payloads(&mut conn, bot.id, &mut parsed)?;

        if update_targets_deleted_message(&mut conn, bot.id, &parsed)? {
            stale_update_ids.push(update_id);
            continue;
        }

        updates.push(parsed);
    }

    if !stale_update_ids.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(stale_update_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM updates WHERE bot_id = ? AND update_id IN ({})",
            placeholders
        );

        let mut bind_values = Vec::with_capacity(1 + stale_update_ids.len());
        bind_values.push(Value::from(bot.id));
        for id in stale_update_ids {
            bind_values.push(Value::from(id));
        }

        let mut delete_stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
        delete_stmt
            .execute(rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)))
            .map_err(ApiError::internal)?;
    }

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
    ensure_sim_bot_profile_photos_storage(&mut conn)?;

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
    ensure_sim_managed_bots_storage(&mut conn)?;

    let owner = ensure_sim_user_record(&mut conn, request.user_id)?;
    let _ = ensure_managed_bot_record(&mut conn, bot.id, owner.id, None, None)?;
    let record = rotate_managed_bot_token(&mut conn, bot.id, owner.id)?;

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
            user: build_user_with_manage_bots(&owner),
            bot: managed_bot_user_from_record(&record),
        }),
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(json!(true))
}

pub fn handle_set_my_commands(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMyCommandsRequest = parse_request(params)?;
    let normalized_commands = normalize_bot_commands_payload(&request.commands)?;
    let scope_key = normalize_bot_command_scope_key(request.scope.as_ref())?;
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_commands_storage(&mut conn)?;

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
    ensure_sim_bot_default_admin_rights_storage(&mut conn)?;

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
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
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
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

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
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
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
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

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
    let (media_kind, media_input) = extract_bot_profile_photo_media_input(&request.photo.extra)?;
    let file = resolve_media_file(state, token, &media_input, media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    ensure_sim_bot_profile_photos_storage(&mut conn)?;

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
    let language_code = normalize_bot_language_code(request.language_code.as_deref())?;
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
    ensure_sim_bot_profile_texts_storage(&mut conn)?;

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

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if request.url.trim().is_empty() {
        return Err(ApiError::bad_request("bad webhook: URL is empty"));
    }

    let secret_token = request.secret_token.unwrap_or_default();
    let max_connections = request.max_connections.unwrap_or(40);
    let ip_address = request.ip_address.unwrap_or_default();

    conn.execute(
        "INSERT INTO webhooks (bot_id, url, secret_token, max_connections, ip_address)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id) DO UPDATE SET
            url = excluded.url,
            secret_token = excluded.secret_token,
            max_connections = excluded.max_connections,
            ip_address = excluded.ip_address",
        params![bot.id, request.url, secret_token, max_connections, ip_address],
    )
    .map_err(ApiError::internal)?;

    if request.drop_pending_updates.unwrap_or(false) {
        conn.execute("DELETE FROM updates WHERE bot_id = ?1", params![bot.id])
            .map_err(ApiError::internal)?;
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
