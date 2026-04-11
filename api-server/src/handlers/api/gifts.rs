use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    GetAvailableGiftsRequest, GetChatGiftsRequest,
    GetUserGiftsRequest, GiftPremiumSubscriptionRequest,
    ConvertGiftToStarsRequest, UpgradeGiftRequest, TransferGiftRequest,
};

use crate::generated::types::{
    Chat, Gift, GiftBackground, Gifts, OwnedGifts
};

use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id};

use crate::handlers::client::{bot, business, chats, messages, users, gifts, types::gifts::SimOwnedGiftFilterOptions};

use crate::handlers::{parse_request, generate_telegram_numeric_id};

pub fn handle_get_available_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetAvailableGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let _bot = ensure_bot(&mut conn, token)?;

    let result = Gifts {
        gifts: gifts::sim_available_gift_catalog()
            .into_iter()
            .map(|entry| entry.gift)
            .collect(),
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

pub fn handle_get_chat_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetChatGiftsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chats::chat_id_as_i64(&request.chat_id, &chat_key);
    let sim_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .or(chats::load_sim_chat_record_by_chat_id(&mut conn, bot.id, chat_id)?)
        .ok_or_else(|| ApiError::not_found("chat not found"))?;

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

    let records = gifts::load_owned_gift_records(&mut conn, bot.id, None, Some(sim_chat.chat_id))?;
    let filtered = gifts::apply_owned_gift_filters(records, &filter_options);
    let total_count = filtered.len() as i64;
    let offset = gifts::parse_owned_gifts_offset(request.offset.as_deref());
    let limit = gifts::parse_owned_gifts_limit(request.limit);

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

    let gifts = page_records
        .iter()
        .map(|record| gifts::map_owned_gift_record(&mut conn, record))
        .collect::<Result<Vec<_>, _>>()?;

    let result = OwnedGifts {
        total_count,
        gifts,
        next_offset,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

pub fn handle_get_user_gifts(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetUserGiftsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if users::load_sim_user_record(&mut conn, request.user_id)?.is_none() {
        return Err(ApiError::not_found("user not found"));
    }

    let filter_options = SimOwnedGiftFilterOptions {
        exclude_unsaved: false,
        exclude_saved: false,
        exclude_unlimited: request.exclude_unlimited.unwrap_or(false),
        exclude_limited_upgradable: request.exclude_limited_upgradable.unwrap_or(false),
        exclude_limited_non_upgradable: request.exclude_limited_non_upgradable.unwrap_or(false),
        exclude_from_blockchain: request.exclude_from_blockchain.unwrap_or(false),
        exclude_unique: request.exclude_unique.unwrap_or(false),
        sort_by_price: request.sort_by_price.unwrap_or(false),
    };

    let records = gifts::load_owned_gift_records(&mut conn, bot.id, Some(request.user_id), None)?;
    let filtered = gifts::apply_owned_gift_filters(records, &filter_options);
    let total_count = filtered.len() as i64;
    let offset = gifts::parse_owned_gifts_offset(request.offset.as_deref());
    let limit = gifts::parse_owned_gifts_limit(request.limit);

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

    let gifts = page_records
        .iter()
        .map(|record| gifts::map_owned_gift_record(&mut conn, record))
        .collect::<Result<Vec<_>, _>>()?;

    let result = OwnedGifts {
        total_count,
        gifts,
        next_offset,
    };

    serde_json::to_value(result).map_err(ApiError::internal)
}

pub fn handle_gift_premium_subscription(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GiftPremiumSubscriptionRequest = parse_request(params)?;

    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.month_count <= 0 {
        return Err(ApiError::bad_request("month_count must be greater than zero"));
    }
    if request.star_count <= 0 {
        return Err(ApiError::bad_request("star_count must be greater than zero"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let recipient = users::ensure_user(&mut conn, Some(request.user_id), None, None)?;
    let sender = users::ensure_user(&mut conn, current_request_actor_user_id(), None, None)?;

    let now = Utc::now().timestamp();
    bot::ensure_bot_star_balance_for_charge(&mut conn, bot.id, request.star_count, now)?;

    let premium_charge_id = format!(
        "gift_premium_subscription_{}",
        generate_telegram_numeric_id(),
    );

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_premium_subscription')",
        params![
            format!("gift_premium_{}", generate_telegram_numeric_id()),
            bot.id,
            recipient.id,
            premium_charge_id,
            -request.star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO star_subscriptions
         (bot_id, user_id, telegram_payment_charge_id, is_canceled, updated_at)
         VALUES (?1, ?2, ?3, 0, ?4)
         ON CONFLICT(bot_id, user_id, telegram_payment_charge_id)
         DO UPDATE SET is_canceled = 0, updated_at = excluded.updated_at",
        params![bot.id, recipient.id, premium_charge_id, now],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE users
         SET is_premium = 1,
             gift_count = COALESCE(gift_count, 0) + 1
         WHERE id = ?1",
        params![recipient.id],
    )
    .map_err(ApiError::internal)?;

    let premium_gift_id = format!("premium_subscription_{}m", request.month_count);
    let premium_gift = Gift {
        id: premium_gift_id.clone(),
        sticker: gifts::build_sim_gift_sticker("premium_subscription", "💎", "simula_premium_gifts"),
        star_count: request.star_count,
        upgrade_star_count: None,
        is_premium: Some(true),
        has_colors: Some(true),
        total_count: None,
        remaining_count: None,
        personal_total_count: None,
        personal_remaining_count: None,
        background: Some(GiftBackground {
            center_color: 0x7A7BFF,
            edge_color: 0x2A2E8F,
            text_color: 0xFFFFFF,
        }),
        unique_gift_variant_count: None,
        publisher_chat: None,
    };

    let premium_owned_gift_id = format!("owned_gift_{}", generate_telegram_numeric_id());
    let premium_text = request
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let premium_entities_json = request
        .text_entities
        .as_ref()
        .map(|entities| serde_json::to_string(entities).map_err(ApiError::internal))
        .transpose()?;

    conn.execute(
        "INSERT INTO sim_owned_gifts
         (bot_id, owned_gift_id, owner_user_id, owner_chat_id, sender_user_id,
          gift_id, gift_json, gift_star_count, is_unique, is_unlimited, is_from_blockchain,
          send_date, text, entities_json, is_private, is_saved, can_be_upgraded, was_refunded,
          convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
          unique_gift_number, transfer_star_count, next_transfer_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, NULL, ?4,
                 ?5, ?6, ?7, 0, 1, 0,
                 ?8, ?9, ?10, 0, 0, 0, 0,
                 NULL, NULL, 0,
                 NULL, NULL, NULL, ?8, ?8)",
        params![
            bot.id,
            premium_owned_gift_id.clone(),
            recipient.id,
            sender.id,
            premium_gift_id,
            serde_json::to_string(&premium_gift).map_err(ApiError::internal)?,
            request.star_count,
            now,
            premium_text.clone(),
            premium_entities_json,
        ],
    )
    .map_err(ApiError::internal)?;

    let sender_user = users::build_user_from_sim_record(&sender, false);
    let recipient_chat_key = recipient.id.to_string();
    ensure_chat(&mut conn, &recipient_chat_key)?;
    let recipient_chat = Chat {
        id: recipient.id,
        r#type: "private".to_string(),
        title: None,
        username: recipient.username.clone(),
        first_name: Some(recipient.first_name.clone()),
        last_name: recipient.last_name.clone(),
        is_forum: None,
        is_direct_messages: None,
    };

    let mut gift_payload = Map::<String, Value>::new();
    gift_payload.insert(
        "gift".to_string(),
        serde_json::to_value(&premium_gift).map_err(ApiError::internal)?,
    );
    gift_payload.insert(
        "owned_gift_id".to_string(),
        Value::String(premium_owned_gift_id),
    );
    gift_payload.insert("can_be_upgraded".to_string(), Value::Bool(false));
    gift_payload.insert("is_upgrade_separate".to_string(), Value::Bool(false));
    if let Some(text) = premium_text {
        gift_payload.insert("text".to_string(), Value::String(text));
    }
    if let Some(entities) = request.text_entities.as_ref() {
        gift_payload.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert("gift".to_string(), Value::Object(gift_payload));

    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &recipient_chat_key,
        &recipient_chat,
        &sender_user,
        now,
        format!(
            "{} sent a gift",
            messages::display_name_for_service_user(&sender_user)
        ),
        service_fields,
    )?;

    Ok(json!(true))
}

pub fn handle_convert_gift_to_stars(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ConvertGiftToStarsRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_convert_gifts_to_stars,
        "not enough rights to convert gifts to stars",
    )?;

    let owned_row: Option<(Option<i64>, Option<i64>, i64, i64, Option<i64>, i64)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id, is_unique, was_refunded, convert_star_count, gift_star_count
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, _owner_chat_id, is_unique, was_refunded, convert_star_count, gift_star_count)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if is_unique == 1 {
        return Err(ApiError::bad_request(
            "only regular gifts can be converted to stars",
        ));
    }
    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }

    let resolved_convert_amount = convert_star_count
        .unwrap_or_else(|| (gift_star_count / 2).max(1))
        .max(1);
    let now = Utc::now().timestamp();

    conn.execute(
        "UPDATE sim_business_connections
         SET star_balance = star_balance + ?1,
             updated_at = ?2
         WHERE bot_id = ?3 AND connection_id = ?4",
        params![
            resolved_convert_amount,
            now,
            bot.id,
            record.connection_id,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_convert')",
        params![
            format!("gift_convert_{}", generate_telegram_numeric_id()),
            bot.id,
            record.user_id,
            format!("gift_convert_{}", generate_telegram_numeric_id()),
            resolved_convert_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM sim_owned_gifts WHERE bot_id = ?1 AND owned_gift_id = ?2",
        params![bot.id, request.owned_gift_id],
    )
    .map_err(ApiError::internal)?;

    if let Some(user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = CASE
                 WHEN COALESCE(gift_count, 0) > 0 THEN gift_count - 1
                 ELSE 0
             END
             WHERE id = ?1",
            params![user_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

pub fn handle_upgrade_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: UpgradeGiftRequest = parse_request(params)?;
    if request.star_count.unwrap_or(0) < 0 {
        return Err(ApiError::bad_request("star_count must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_transfer_and_upgrade_gifts,
        "not enough rights to upgrade gifts",
    )?;

    let owned_row: Option<(String, i64, i64, i64, Option<i64>, i64, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT gift_json, is_unique, was_refunded, can_be_upgraded,
                    prepaid_upgrade_star_count, gift_star_count,
                    unique_gift_number, transfer_star_count
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                    row.get(6)?,
                    row.get(7)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((gift_json, is_unique, was_refunded, can_be_upgraded, prepaid_upgrade_star_count, gift_star_count, unique_gift_number, transfer_star_count)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }
    if is_unique == 1 {
        return Ok(json!(true));
    }
    if can_be_upgraded != 1 {
        return Err(ApiError::bad_request("gift cannot be upgraded"));
    }

    let resolved_upgrade_cost = request
        .star_count
        .unwrap_or_else(|| prepaid_upgrade_star_count.unwrap_or((gift_star_count / 2).max(1)))
        .max(0);

    if resolved_upgrade_cost > record.star_balance {
        return Err(ApiError::bad_request(
            "not enough stars in business account balance",
        ));
    }

    let now = Utc::now().timestamp();

    if resolved_upgrade_cost > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance - ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![resolved_upgrade_cost, now, bot.id, record.connection_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_upgrade')",
            params![
                format!("gift_upgrade_{}", generate_telegram_numeric_id()),
                bot.id,
                record.user_id,
                format!("gift_upgrade_{}", generate_telegram_numeric_id()),
                -resolved_upgrade_cost,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let mut gift = serde_json::from_str::<Gift>(&gift_json)
        .unwrap_or_else(|_| gifts::fallback_sim_gift("gift_upgraded"));
    let generated_unique_number = generate_telegram_numeric_id()
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>()
        .parse::<i64>()
        .ok()
        .unwrap_or_else(|| Utc::now().timestamp_micros().unsigned_abs() as i64);
    let unique_number = unique_gift_number.unwrap_or(generated_unique_number);
    if !request.keep_original_details.unwrap_or(false) {
        gift.id = format!("{}_u{}", gift.id, unique_number);
    }
    gift.total_count = Some(1);
    gift.remaining_count = Some(1);
    gift.personal_total_count = Some(1);
    gift.personal_remaining_count = Some(1);
    gift.unique_gift_variant_count = Some(1);
    gift.has_colors = Some(true);

    let next_transfer_star_count = transfer_star_count
        .unwrap_or_else(|| (resolved_upgrade_cost / 2).max(1))
        .max(1);

    conn.execute(
        "UPDATE sim_owned_gifts
         SET gift_id = ?1,
             gift_json = ?2,
             is_unique = 1,
             can_be_upgraded = 0,
             is_upgrade_separate = 0,
             prepaid_upgrade_star_count = ?3,
             unique_gift_number = ?4,
             transfer_star_count = ?5,
             next_transfer_date = ?6,
             updated_at = ?7
         WHERE bot_id = ?8 AND owned_gift_id = ?9",
        params![
            gift.id,
            serde_json::to_string(&gift).map_err(ApiError::internal)?,
            resolved_upgrade_cost,
            unique_number,
            next_transfer_star_count,
            now,
            now,
            bot.id,
            request.owned_gift_id,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_transfer_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: TransferGiftRequest = parse_request(params)?;
    if request.new_owner_chat_id == 0 {
        return Err(ApiError::bad_request("new_owner_chat_id is invalid"));
    }
    if request.star_count.unwrap_or(0) < 0 {
        return Err(ApiError::bad_request("star_count must be non-negative"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let record = business::load_business_connection_or_404(
        &mut conn,
        bot.id,
        &request.business_connection_id,
    )?;
    let connection = business::build_business_connection(&mut conn, bot.id, &record)?;
    business::ensure_business_right(
        &connection,
        |rights| rights.can_transfer_and_upgrade_gifts,
        "not enough rights to transfer gifts",
    )?;

    let owned_row: Option<(Option<i64>, Option<i64>, i64, i64, Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id, is_unique, was_refunded,
                    transfer_star_count, next_transfer_date
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, request.owned_gift_id],
            |row| {
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    row.get(2)?,
                    row.get(3)?,
                    row.get(4)?,
                    row.get(5)?,
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, owner_chat_id, is_unique, was_refunded, transfer_star_count, next_transfer_date)) = owned_row else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if is_unique != 1 {
        return Err(ApiError::bad_request("only unique gifts can be transferred"));
    }
    if was_refunded == 1 {
        return Err(ApiError::bad_request("gift has already been converted"));
    }

    let now = Utc::now().timestamp();
    if let Some(next_allowed_transfer) = next_transfer_date {
        if next_allowed_transfer > now {
            return Err(ApiError::bad_request(
                "gift cannot be transferred yet",
            ));
        }
    }

    let mut next_owner_user_id: Option<i64> = None;
    let mut next_owner_chat_id: Option<i64> = None;

    if let Some(sim_chat) = chats::load_sim_chat_record_by_chat_id(
        &mut conn,
        bot.id,
        request.new_owner_chat_id,
    )? {
        if sim_chat.chat_type == "private" {
            let recipient = users::ensure_user(&mut conn, Some(sim_chat.chat_id), None, None)?;
            next_owner_user_id = Some(recipient.id);
        } else {
            next_owner_chat_id = Some(sim_chat.chat_id);
        }
    } else {
        let recipient = users::ensure_user(&mut conn, Some(request.new_owner_chat_id), None, None)?;
        next_owner_user_id = Some(recipient.id);
    }

    if owner_user_id == next_owner_user_id && owner_chat_id == next_owner_chat_id {
        return Ok(json!(true));
    }

    let resolved_transfer_cost = request
        .star_count
        .unwrap_or(transfer_star_count.unwrap_or(0))
        .max(0);
    if resolved_transfer_cost > record.star_balance {
        return Err(ApiError::bad_request(
            "not enough stars in business account balance",
        ));
    }

    if resolved_transfer_cost > 0 {
        conn.execute(
            "UPDATE sim_business_connections
             SET star_balance = star_balance - ?1,
                 updated_at = ?2
             WHERE bot_id = ?3 AND connection_id = ?4",
            params![resolved_transfer_cost, now, bot.id, record.connection_id],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_transfer')",
            params![
                format!("gift_transfer_{}", generate_telegram_numeric_id()),
                bot.id,
                record.user_id,
                format!("gift_transfer_{}", generate_telegram_numeric_id()),
                -resolved_transfer_cost,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let next_transfer_cost = transfer_star_count
        .unwrap_or_else(|| resolved_transfer_cost.max(1))
        .max(1);

    conn.execute(
        "UPDATE sim_owned_gifts
         SET owner_user_id = ?1,
             owner_chat_id = ?2,
             sender_user_id = ?3,
             is_saved = 0,
             transfer_star_count = ?4,
             next_transfer_date = ?5,
             updated_at = ?6
         WHERE bot_id = ?7 AND owned_gift_id = ?8",
        params![
            next_owner_user_id,
            next_owner_chat_id,
            connection.user.id,
            next_transfer_cost,
            now.saturating_add(86_400),
            now,
            bot.id,
            request.owned_gift_id,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(previous_owner_user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = CASE
                 WHEN COALESCE(gift_count, 0) > 0 THEN gift_count - 1
                 ELSE 0
             END
             WHERE id = ?1",
            params![previous_owner_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    if let Some(current_owner_user_id) = next_owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = COALESCE(gift_count, 0) + 1
             WHERE id = ?1",
            params![current_owner_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}
