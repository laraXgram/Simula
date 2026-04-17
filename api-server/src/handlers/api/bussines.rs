use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    GetBusinessConnectionRequest,
    SetBusinessAccountNameRequest, SetBusinessAccountUsernameRequest,
    SetBusinessAccountBioRequest, SetBusinessAccountProfilePhotoRequest,
    RemoveBusinessAccountProfilePhotoRequest, ReadBusinessMessageRequest,
    DeleteBusinessMessagesRequest, SetBusinessAccountGiftSettingsRequest,
    GetBusinessAccountStarBalanceRequest, TransferBusinessAccountStarsRequest,
    GetBusinessAccountGiftsRequest,
};

use crate::generated::types::{
    BusinessMessagesDeleted, Chat, OwnedGifts, StarAmount, Update,
};

use crate::handlers::client::{
    business,
    gifts,
    messages,
    types::gifts::SimOwnedGiftFilterOptions,
    users,
    webhook,
};

use crate::handlers::{generate_telegram_numeric_id, parse_request};

const BUSINESS_NAME_MAX_LEN: usize = 64;
const BUSINESS_USERNAME_MAX_LEN: usize = 32;
const BUSINESS_BIO_MAX_LEN: usize = 140;
const BUSINESS_DELETE_MESSAGES_MAX_COUNT: usize = 100;
const BUSINESS_TRANSFER_STARS_MAX_COUNT: i64 = 10_000;
const BUSINESS_READ_MESSAGE_WINDOW_SECONDS: i64 = 86_400;

fn ensure_business_connection_enabled(is_enabled: bool) -> Result<(), ApiError> {
    if is_enabled {
        return Ok(());
    }

    Err(ApiError::bad_request("business connection is disabled"))
}

fn normalize_required_business_text(
    value: &str,
    max_len: usize,
    field_name: &str,
) -> Result<String, ApiError> {
    let normalized = value.trim();
    if normalized.is_empty() {
        return Err(ApiError::bad_request(&format!("{field_name} is empty")));
    }
    if normalized.chars().count() > max_len {
        return Err(ApiError::bad_request(&format!(
            "{field_name} must be at most {max_len} characters"
        )));
    }

    Ok(normalized.to_string())
}

fn normalize_optional_business_text(
    value: Option<String>,
    max_len: usize,
    field_name: &str,
) -> Result<Option<String>, ApiError> {
    let normalized = value
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);

    if let Some(text) = normalized.as_deref() {
        if text.chars().count() > max_len {
            return Err(ApiError::bad_request(&format!(
                "{field_name} must be at most {max_len} characters"
            )));
        }
    }

    Ok(normalized)
}

fn normalize_business_username(value: Option<String>) -> Result<Option<String>, ApiError> {
    let normalized = value
        .as_deref()
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string);

    let Some(username) = normalized else {
        return Ok(None);
    };

    if username.chars().count() > BUSINESS_USERNAME_MAX_LEN {
        return Err(ApiError::bad_request(
            "username must be at most 32 characters",
        ));
    }
    if !username
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
    {
        return Err(ApiError::bad_request(
            "username can contain only letters, digits, and underscores",
        ));
    }

    Ok(Some(username.to_ascii_lowercase()))
}

pub fn handle_get_business_connection(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessConnectionRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    serde_json::to_value(connection).map_err(ApiError::internal)
}

pub fn handle_set_business_account_name(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountNameRequest = parse_request(params)?;
    let first_name = normalize_required_business_text(
        &request.first_name,
        BUSINESS_NAME_MAX_LEN,
        "first_name",
    )?;
    let last_name = normalize_optional_business_text(
        request.last_name,
        BUSINESS_NAME_MAX_LEN,
        "last_name",
    )?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_edit_name,
        "not enough rights to edit business account name",
    )?;

    conn.execute(
        "UPDATE users SET first_name = ?1 WHERE id = ?2",
        params![first_name, record.user_id],
    )
    .map_err(ApiError::internal)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, ?3, NULL, NULL, NULL, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET last_name = excluded.last_name, updated_at = excluded.updated_at",
        params![bot.id, record.user_id, last_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_business_account_username(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountUsernameRequest = parse_request(params)?;
    let username = normalize_business_username(request.username)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_edit_username,
        "not enough rights to edit business account username",
    )?;

    if let Some(candidate) = username.as_deref() {
        let existing_owner_id: Option<i64> = conn
            .query_row(
                "SELECT id
                 FROM users
                 WHERE LOWER(username) = LOWER(?1)
                 LIMIT 1",
                params![candidate],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(owner_id) = existing_owner_id {
            if owner_id != record.user_id {
                return Err(ApiError::bad_request("username is already taken"));
            }
        }
    }

    conn.execute(
        "UPDATE users SET username = ?1 WHERE id = ?2",
        params![username, record.user_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_business_account_bio(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountBioRequest = parse_request(params)?;
    let bio = normalize_optional_business_text(request.bio, BUSINESS_BIO_MAX_LEN, "bio")?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_edit_bio,
        "not enough rights to edit business account bio",
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, NULL, ?3, NULL, NULL, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET bio = excluded.bio, updated_at = excluded.updated_at",
        params![bot.id, record.user_id, bio, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_set_business_account_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_edit_profile_photo,
        "not enough rights to edit business account profile photo",
    )?;

    let photo_input = business::extract_business_profile_photo_media_input(&request.photo.extra)?;
    let file = messages::resolve_media_file_with_conn(&mut conn, bot.id, &photo_input, "photo")?;
    let is_public = request.is_public.unwrap_or(false);
    let private_photo_file_id = if is_public {
        None
    } else {
        Some(file.file_id.clone())
    };
    let public_photo_file_id = if is_public {
        Some(file.file_id.clone())
    } else {
        None
    };
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
         VALUES (?1, ?2, NULL, NULL, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET
            profile_photo_file_id = CASE WHEN ?6 = 1 THEN sim_business_account_profiles.profile_photo_file_id ELSE excluded.profile_photo_file_id END,
            public_profile_photo_file_id = CASE WHEN ?6 = 1 THEN excluded.public_profile_photo_file_id ELSE sim_business_account_profiles.public_profile_photo_file_id END,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            record.user_id,
            private_photo_file_id,
            public_photo_file_id,
            now,
            if is_public { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_remove_business_account_profile_photo(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RemoveBusinessAccountProfilePhotoRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_edit_profile_photo,
        "not enough rights to edit business account profile photo",
    )?;

    let is_public = request.is_public.unwrap_or(false);
    let now = Utc::now().timestamp();
    if is_public {
        conn.execute(
            "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
             VALUES (?1, ?2, NULL, NULL, NULL, NULL, ?3)
             ON CONFLICT(bot_id, user_id)
             DO UPDATE SET public_profile_photo_file_id = NULL, updated_at = excluded.updated_at",
            params![bot.id, record.user_id, now],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT INTO sim_business_account_profiles (bot_id, user_id, last_name, bio, profile_photo_file_id, public_profile_photo_file_id, updated_at)
             VALUES (?1, ?2, NULL, NULL, NULL, NULL, ?3)
             ON CONFLICT(bot_id, user_id)
             DO UPDATE SET profile_photo_file_id = NULL, updated_at = excluded.updated_at",
            params![bot.id, record.user_id, now],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

pub fn handle_read_business_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ReadBusinessMessageRequest = parse_request(params)?;
    if request.chat_id == 0 {
        return Err(ApiError::bad_request("chat_id is invalid"));
    }
    if request.message_id <= 0 {
        return Err(ApiError::bad_request("message_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_read_messages,
        "not enough rights to read business messages",
    )?;

    if request.chat_id != record.user_chat_id {
        return Err(ApiError::bad_request("chat_id does not belong to the business connection"));
    }

    let chat_key = request.chat_id.to_string();
    let now = Utc::now().timestamp();
    let message_row: Option<(i64, Option<i64>)> = conn
        .query_row(
            "SELECT date, from_user_id
             FROM messages
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((message_date, from_user_id)) = message_row else {
        return Err(ApiError::not_found("message not found"));
    };

    if now.saturating_sub(message_date) > BUSINESS_READ_MESSAGE_WINDOW_SECONDS {
        return Err(ApiError::bad_request(
            "message is not active in the last 24 hours",
        ));
    }

    if from_user_id != Some(record.user_id) {
        return Err(ApiError::bad_request(
            "message is not an incoming business message",
        ));
    }

    conn.execute(
        "INSERT INTO sim_business_read_messages (bot_id, connection_id, chat_id, message_id, read_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, connection_id, chat_id, message_id)
         DO UPDATE SET read_at = excluded.read_at",
        params![bot.id, record.connection_id, request.chat_id, request.message_id, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_delete_business_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteBusinessMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }
    if request.message_ids.len() > BUSINESS_DELETE_MESSAGES_MAX_COUNT {
        return Err(ApiError::bad_request(
            "message_ids must contain between 1 and 100 items",
        ));
    }

    let mut unique_message_ids: Vec<i64> = Vec::new();
    for message_id in request.message_ids {
        if message_id <= 0 {
            return Err(ApiError::bad_request("message_ids contain invalid message_id"));
        }
        if !unique_message_ids.contains(&message_id) {
            unique_message_ids.push(message_id);
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;

    let can_delete_sent = business::business_right_enabled(&connection.rights, |rights| rights.can_delete_sent_messages);
    let can_delete_all = business::business_right_enabled(&connection.rights, |rights| rights.can_delete_all_messages);
    if !can_delete_sent && !can_delete_all {
        return Err(ApiError::bad_request("not enough rights to delete business messages"));
    }

    let chat_key = record.user_chat_id.to_string();
    let mut deleted_ids: Vec<i64> = Vec::new();
    for message_id in unique_message_ids {
        let message_row: Option<(String, Option<i64>)> = conn
            .query_row(
                "SELECT chat_key, from_user_id
                 FROM messages
                 WHERE bot_id = ?1 AND message_id = ?2",
                params![bot.id, message_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        let Some((message_chat_key, from_user_id)) = message_row else {
            continue;
        };

        if message_chat_key != chat_key {
            return Err(ApiError::bad_request(
                "all message_ids must belong to the business connection chat",
            ));
        }

        if !can_delete_all {
            let sent_by_bot = matches!(from_user_id, Some(sender_id) if sender_id == bot.id);
            if !sent_by_bot {
                continue;
            }
        }

        conn.execute(
            "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, message_id],
        )
        .map_err(ApiError::internal)?;

        let deleted = conn
            .execute(
                "DELETE FROM messages WHERE bot_id = ?1 AND message_id = ?2",
                params![bot.id, message_id],
            )
            .map_err(ApiError::internal)?;
        if deleted > 0 {
            deleted_ids.push(message_id);
        }
    }

    if !deleted_ids.is_empty() {
        let user_record = users::ensure_sim_user_record(&mut conn, record.user_id)?;
        let chat = Chat {
            id: record.user_chat_id,
            r#type: "private".to_string(),
            title: None,
            username: user_record.username.clone(),
            first_name: Some(user_record.first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        };
        let deleted_payload = BusinessMessagesDeleted {
            business_connection_id: record.connection_id.clone(),
            chat,
            message_ids: deleted_ids,
        };

        let update_value = serde_json::to_value(Update {
            update_id: 0,
            message: None,
            edited_message: None,
            channel_post: None,
            edited_channel_post: None,
            business_connection: None,
            business_message: None,
            edited_business_message: None,
            deleted_business_messages: Some(deleted_payload),
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
    }

    Ok(json!(true))
}

pub fn handle_set_business_account_gift_settings(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetBusinessAccountGiftSettingsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_change_gift_settings,
        "not enough rights to edit business gift settings",
    )?;

    let accepted_types_json = serde_json::to_string(&request.accepted_gift_types).map_err(ApiError::internal)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_connections
         SET gift_settings_show_button = ?1,
             gift_settings_types_json = ?2,
             updated_at = ?3
         WHERE bot_id = ?4 AND connection_id = ?5",
        params![
            if request.show_gift_button { 1 } else { 0 },
            accepted_types_json,
            now,
            bot.id,
            record.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_get_business_account_star_balance(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessAccountStarBalanceRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_view_gifts_and_stars,
        "not enough rights to view business stars",
    )?;

    let result = StarAmount {
        amount: record.star_balance,
        nanostar_amount: None,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

pub fn handle_transfer_business_account_stars(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: TransferBusinessAccountStarsRequest = parse_request(params)?;
    if request.star_count <= 0 || request.star_count > BUSINESS_TRANSFER_STARS_MAX_COUNT {
        return Err(ApiError::bad_request("star_count must be between 1 and 10000"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_transfer_stars,
        "not enough rights to transfer business stars",
    )?;

    if request.star_count > record.star_balance {
        return Err(ApiError::bad_request("not enough stars in business account balance"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance - ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![request.star_count, now, bot.id, record.connection_id],
    )
    .map_err(ApiError::internal)?;

    let transfer_charge_id = format!(
        "business_transfer_{}_{}",
        request.business_connection_id,
        generate_telegram_numeric_id(),
    );
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'business_transfer')",
        params![
            format!("business_transfer_{}", generate_telegram_numeric_id()),
            bot.id,
            record.user_id,
            transfer_charge_id,
            request.star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_get_business_account_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetBusinessAccountGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(&mut conn, bot.id, &request.business_connection_id)?;
    ensure_business_connection_enabled(record.is_enabled)?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_view_gifts_and_stars,
        "not enough rights to view business gifts",
    )?;

    let _accepted_gift_types =
        business::parse_business_accepted_gift_types_json(record.gift_settings_types_json.as_deref());

    let filter_options = SimOwnedGiftFilterOptions {
        exclude_unsaved: request.exclude_unsaved.unwrap_or(false),
        exclude_saved: request.exclude_saved.unwrap_or(false),
        exclude_unlimited: request.exclude_unlimited.unwrap_or(false),
        exclude_limited_upgradable: request.exclude_limited_upgradable.unwrap_or(false),
        exclude_limited_non_upgradable: request.exclude_limited_non_upgradable.unwrap_or(false),
        exclude_from_blockchain: request.exclude_from_blockchain.unwrap_or(false),
        exclude_unique: request.exclude_unique.unwrap_or(false),
        sort_by_price: request.sort_by_price.unwrap_or(false),
    };

    let records = gifts::load_owned_gift_records(&mut conn, bot.id, Some(record.user_id), None)?;
    let filtered = gifts::apply_owned_gift_filters(records, &filter_options);
    let total_count = filtered.len() as i64;
    let offset = gifts::parse_owned_gifts_offset(request.offset.as_deref());
    let limit = gifts::parse_owned_gifts_limit(request.limit.or(Some(100)));

    let page_records = filtered
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();
    let next_offset = if offset + page_records.len() < total_count as usize {
        Some((offset + page_records.len()).to_string())
    } else {
        None
    };

    let gift_items = page_records
        .iter()
        .map(|record| gifts::map_owned_gift_record(&mut conn, record))
        .collect::<Result<Vec<_>, _>>()?;

    let result = OwnedGifts {
        total_count,
        gifts: gift_items,
        next_offset,
    };
    serde_json::to_value(result).map_err(ApiError::internal)
}
