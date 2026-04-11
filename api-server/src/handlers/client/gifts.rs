use actix_web::web::Data;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::types::{Gift, OwnedGift, MessageEntity, GiftBackground, Sticker};
use crate::handlers::{generate_telegram_file_unique_id, utils::updates::value_to_chat_key};

use super::types::gifts::{
    SimOwnedGiftRecord, SimOwnedGiftFilterOptions, SimDeleteOwnedGiftRequest, SimGiftCatalogEntry
};

use super::{chats, users};

pub fn load_owned_gift_records(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    owner_user_id: Option<i64>,
    owner_chat_id: Option<i64>,
) -> Result<Vec<SimOwnedGiftRecord>, ApiError> {
    let mut records = Vec::new();

    if let Some(user_id) = owner_user_id {
        let mut stmt = conn
            .prepare(
                "SELECT owned_gift_id, sender_user_id, gift_id, gift_json, gift_star_count,
                        is_unique, is_unlimited, is_from_blockchain,
                        send_date, text, entities_json,
                        is_private, is_saved, can_be_upgraded, was_refunded,
                        convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
                        unique_gift_number, transfer_star_count, next_transfer_date
                 FROM sim_owned_gifts
                 WHERE bot_id = ?1 AND owner_user_id = ?2
                 ORDER BY send_date DESC, owned_gift_id DESC",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![bot_id, user_id], |row| {
                Ok(SimOwnedGiftRecord {
                    owned_gift_id: row.get(0)?,
                    sender_user_id: row.get(1)?,
                    gift_id: row.get(2)?,
                    gift_json: row.get(3)?,
                    gift_star_count: row.get(4)?,
                    is_unique: row.get::<_, i64>(5)? == 1,
                    is_unlimited: row.get::<_, i64>(6)? == 1,
                    is_from_blockchain: row.get::<_, i64>(7)? == 1,
                    send_date: row.get(8)?,
                    text: row.get(9)?,
                    entities_json: row.get(10)?,
                    is_private: row.get::<_, i64>(11)? == 1,
                    is_saved: row.get::<_, i64>(12)? == 1,
                    can_be_upgraded: row.get::<_, i64>(13)? == 1,
                    was_refunded: row.get::<_, i64>(14)? == 1,
                    convert_star_count: row.get(15)?,
                    prepaid_upgrade_star_count: row.get(16)?,
                    is_upgrade_separate: row.get::<_, i64>(17)? == 1,
                    unique_gift_number: row.get(18)?,
                    transfer_star_count: row.get(19)?,
                    next_transfer_date: row.get(20)?,
                })
            })
            .map_err(ApiError::internal)?;

        for row in rows {
            records.push(row.map_err(ApiError::internal)?);
        }

        return Ok(records);
    }

    if let Some(chat_id) = owner_chat_id {
        let mut stmt = conn
            .prepare(
                "SELECT owned_gift_id, sender_user_id, gift_id, gift_json, gift_star_count,
                        is_unique, is_unlimited, is_from_blockchain,
                        send_date, text, entities_json,
                        is_private, is_saved, can_be_upgraded, was_refunded,
                        convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
                        unique_gift_number, transfer_star_count, next_transfer_date
                 FROM sim_owned_gifts
                 WHERE bot_id = ?1 AND owner_chat_id = ?2
                 ORDER BY send_date DESC, owned_gift_id DESC",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![bot_id, chat_id], |row| {
                Ok(SimOwnedGiftRecord {
                    owned_gift_id: row.get(0)?,
                    sender_user_id: row.get(1)?,
                    gift_id: row.get(2)?,
                    gift_json: row.get(3)?,
                    gift_star_count: row.get(4)?,
                    is_unique: row.get::<_, i64>(5)? == 1,
                    is_unlimited: row.get::<_, i64>(6)? == 1,
                    is_from_blockchain: row.get::<_, i64>(7)? == 1,
                    send_date: row.get(8)?,
                    text: row.get(9)?,
                    entities_json: row.get(10)?,
                    is_private: row.get::<_, i64>(11)? == 1,
                    is_saved: row.get::<_, i64>(12)? == 1,
                    can_be_upgraded: row.get::<_, i64>(13)? == 1,
                    was_refunded: row.get::<_, i64>(14)? == 1,
                    convert_star_count: row.get(15)?,
                    prepaid_upgrade_star_count: row.get(16)?,
                    is_upgrade_separate: row.get::<_, i64>(17)? == 1,
                    unique_gift_number: row.get(18)?,
                    transfer_star_count: row.get(19)?,
                    next_transfer_date: row.get(20)?,
                })
            })
            .map_err(ApiError::internal)?;

        for row in rows {
            records.push(row.map_err(ApiError::internal)?);
        }
    }

    Ok(records)
}

pub fn apply_owned_gift_filters(
    mut records: Vec<SimOwnedGiftRecord>,
    options: &SimOwnedGiftFilterOptions,
) -> Vec<SimOwnedGiftRecord> {
    records.retain(|record| {
        if options.exclude_unique && record.is_unique {
            return false;
        }
        if options.exclude_unsaved && !record.is_saved {
            return false;
        }
        if options.exclude_saved && record.is_saved {
            return false;
        }
        if options.exclude_unlimited && record.is_unlimited {
            return false;
        }

        let is_limited = !record.is_unlimited;
        if options.exclude_limited_upgradable && is_limited && record.can_be_upgraded {
            return false;
        }
        if options.exclude_limited_non_upgradable && is_limited && !record.can_be_upgraded {
            return false;
        }
        if options.exclude_from_blockchain && record.is_from_blockchain {
            return false;
        }

        true
    });

    if options.sort_by_price {
        records.sort_by(|a, b| {
            b.gift_star_count
                .cmp(&a.gift_star_count)
                .then_with(|| b.send_date.cmp(&a.send_date))
                .then_with(|| b.owned_gift_id.cmp(&a.owned_gift_id))
        });
    } else {
        records.sort_by(|a, b| {
            b.send_date
                .cmp(&a.send_date)
                .then_with(|| b.owned_gift_id.cmp(&a.owned_gift_id))
        });
    }

    records
}

pub fn map_owned_gift_record(
    conn: &mut rusqlite::Connection,
    record: &SimOwnedGiftRecord,
) -> Result<OwnedGift, ApiError> {
    let gift = serde_json::from_str::<Gift>(&record.gift_json)
        .unwrap_or_else(|_| fallback_sim_gift(&record.gift_id));
    let entities = record
        .entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<MessageEntity>>(raw).ok());

    let sender_user = if let Some(sender_user_id) = record.sender_user_id {
        users::load_sim_user_record(conn, sender_user_id)?
            .map(|user| users::build_user_from_sim_record(&user, false))
    } else {
        None
    };

    let mut payload = Map::<String, Value>::new();
    payload.insert("type".to_string(), Value::String("regular".to_string()));
    payload.insert(
        "gift".to_string(),
        serde_json::to_value(gift).map_err(ApiError::internal)?,
    );
    payload.insert(
        "owned_gift_id".to_string(),
        Value::String(record.owned_gift_id.clone()),
    );
    payload.insert("send_date".to_string(), Value::from(record.send_date));
    payload.insert("is_private".to_string(), Value::Bool(record.is_private));
    payload.insert("is_saved".to_string(), Value::Bool(record.is_saved));
    payload.insert(
        "can_be_upgraded".to_string(),
        Value::Bool(record.can_be_upgraded),
    );
    payload.insert("was_refunded".to_string(), Value::Bool(record.was_refunded));
    payload.insert(
        "is_upgrade_separate".to_string(),
        Value::Bool(record.is_upgrade_separate),
    );

    if let Some(sender) = sender_user {
        payload.insert(
            "sender_user".to_string(),
            serde_json::to_value(sender).map_err(ApiError::internal)?,
        );
    }
    if let Some(text) = record.text.as_ref() {
        payload.insert("text".to_string(), Value::String(text.clone()));
    }
    if let Some(entities) = entities {
        payload.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }
    if let Some(value) = record.convert_star_count {
        payload.insert("convert_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.prepaid_upgrade_star_count {
        payload.insert("prepaid_upgrade_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.unique_gift_number {
        payload.insert("unique_gift_number".to_string(), Value::from(value));
    }
    if let Some(value) = record.transfer_star_count {
        payload.insert("transfer_star_count".to_string(), Value::from(value));
    }
    if let Some(value) = record.next_transfer_date {
        payload.insert("next_transfer_date".to_string(), Value::from(value));
    }

    Ok(OwnedGift {
        extra: Value::Object(payload),
    })
}

pub fn handle_sim_delete_owned_gift(
    state: &Data<AppState>,
    token: &str,
    body: SimDeleteOwnedGiftRequest,
) -> ApiResult {
    let owned_gift_id = body.owned_gift_id.trim();
    if owned_gift_id.is_empty() {
        return Err(ApiError::bad_request("owned_gift_id is required"));
    }
    let owned_gift_id = owned_gift_id.to_string();

    let requested_user_id = body.user_id.filter(|value| *value > 0);

    let requested_chat_id = if let Some(chat_value) = body.chat_id.as_ref() {
        let chat_key = value_to_chat_key(chat_value)?;
        Some(chats::chat_id_as_i64(chat_value, &chat_key))
    } else {
        None
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let owned_record: Option<(Option<i64>, Option<i64>)> = conn
        .query_row(
            "SELECT owner_user_id, owner_chat_id
             FROM sim_owned_gifts
             WHERE bot_id = ?1 AND owned_gift_id = ?2",
            params![bot.id, &owned_gift_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((owner_user_id, owner_chat_id)) = owned_record else {
        return Err(ApiError::not_found("owned gift not found"));
    };

    if let Some(expected_user_id) = requested_user_id {
        if owner_user_id != Some(expected_user_id) {
            return Err(ApiError::bad_request(
                "owned gift does not belong to requested user",
            ));
        }
    }

    if let Some(expected_chat_id) = requested_chat_id {
        if owner_chat_id != Some(expected_chat_id) {
            return Err(ApiError::bad_request(
                "owned gift does not belong to requested chat",
            ));
        }
    }

    conn.execute(
        "DELETE FROM sim_owned_gifts WHERE bot_id = ?1 AND owned_gift_id = ?2",
        params![bot.id, &owned_gift_id],
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

pub fn build_sim_gift_sticker(gift_id: &str, emoji: &str, set_name: &str) -> Sticker {
    Sticker {
        file_id: format!("gift_sticker_{}", gift_id),
        file_unique_id: generate_telegram_file_unique_id(),
        r#type: "regular".to_string(),
        width: 512,
        height: 512,
        is_animated: false,
        is_video: false,
        thumbnail: None,
        emoji: Some(emoji.to_string()),
        set_name: Some(set_name.to_string()),
        premium_animation: None,
        mask_position: None,
        custom_emoji_id: None,
        needs_repainting: None,
        file_size: Some(1024),
    }
}

pub fn build_sim_catalog_gift(
    gift_id: &str,
    star_count: i64,
    upgrade_star_count: Option<i64>,
    is_premium: bool,
    is_unlimited: bool,
    is_from_blockchain: bool,
    emoji: &str,
    set_name: &str,
) -> SimGiftCatalogEntry {
    let total_count = if is_unlimited { None } else { Some(20_000) };
    let remaining_count = if is_unlimited { None } else { Some(13_000) };
    SimGiftCatalogEntry {
        gift: Gift {
            id: gift_id.to_string(),
            sticker: build_sim_gift_sticker(gift_id, emoji, set_name),
            star_count,
            upgrade_star_count,
            is_premium: Some(is_premium),
            has_colors: Some(true),
            total_count,
            remaining_count,
            personal_total_count: if is_unlimited { None } else { Some(3) },
            personal_remaining_count: if is_unlimited { None } else { Some(3) },
            background: Some(GiftBackground {
                center_color: 0x7EC8FF,
                edge_color: 0x285B8C,
                text_color: 0xFFFFFF,
            }),
            unique_gift_variant_count: if is_unlimited { None } else { Some(120) },
            publisher_chat: None,
        },
        is_unlimited,
        is_from_blockchain,
    }
}

pub fn sim_available_gift_catalog() -> Vec<SimGiftCatalogEntry> {
    vec![
        build_sim_catalog_gift(
            "gift_rose",
            45,
            Some(120),
            false,
            false,
            false,
            "🌹",
            "simula_gifts",
        ),
        build_sim_catalog_gift(
            "gift_star_box",
            120,
            Some(240),
            false,
            true,
            false,
            "🎁",
            "simula_gifts",
        ),
        build_sim_catalog_gift(
            "gift_premium_badge",
            950,
            None,
            true,
            false,
            false,
            "💎",
            "simula_gifts",
        ),
    ]
}

pub fn find_sim_catalog_gift(gift_id: &str) -> Option<SimGiftCatalogEntry> {
    sim_available_gift_catalog()
        .into_iter()
        .find(|entry| entry.gift.id == gift_id)
}

pub fn fallback_sim_gift(gift_id: &str) -> Gift {
    find_sim_catalog_gift(gift_id)
        .map(|entry| entry.gift)
        .unwrap_or_else(|| {
            build_sim_catalog_gift(
                gift_id,
                100,
                Some(200),
                false,
                true,
                false,
                "🎁",
                "simula_gifts",
            )
            .gift
        })
}

pub fn parse_owned_gifts_offset(offset: Option<&str>) -> usize {
    offset
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(0)
}

pub fn parse_owned_gifts_limit(limit: Option<i64>) -> usize {
    limit.unwrap_or(20).clamp(1, 100) as usize
}