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
    AddStickerToSetRequest, CreateNewStickerSetRequest,
    DeleteStickerFromSetRequest, DeleteStickerSetRequest,
    GetCustomEmojiStickersRequest, GetStickerSetRequest,
    ReplaceStickerInSetRequest, SetCustomEmojiStickerSetThumbnailRequest,
    SetStickerEmojiListRequest, SetStickerKeywordsRequest,
    SetStickerMaskPositionRequest, SetStickerPositionInSetRequest,
    SetStickerSetThumbnailRequest, SetStickerSetTitleRequest,
    UploadStickerFileRequest,
};

use crate::generated::types::{File, InputSticker, PhotoSize, StickerSet};

use crate::handlers::client::messages;

use crate::handlers::{parse_request, sql_value_to_rusqlite};

#[derive(Debug)]
struct StickerSetRecord {
    sticker_type: String,
    needs_repainting: bool,
    owner_user_id: i64,
}

fn ensure_positive_user_id(user_id: i64, field_name: &str) -> Result<(), ApiError> {
    if user_id <= 0 {
        return Err(ApiError::bad_request(format!("{} is invalid", field_name)));
    }
    Ok(())
}

fn validate_sticker_set_title(title: &str) -> Result<(), ApiError> {
    let trimmed = title.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if trimmed.chars().count() > 64 {
        return Err(ApiError::bad_request("title must be 1-64 characters"));
    }
    Ok(())
}

fn validate_sticker_set_name(name: &str, bot_username: &str) -> Result<(), ApiError> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }
    if trimmed.chars().count() > 64 {
        return Err(ApiError::bad_request("name must be 1-64 characters"));
    }

    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err(ApiError::bad_request("name is empty"));
    };

    if !first.is_ascii_alphabetic() {
        return Err(ApiError::bad_request("name must begin with an English letter"));
    }

    if !trimmed
        .chars()
        .all(|ch| ch.is_ascii_alphabetic() || ch.is_ascii_digit() || ch == '_')
    {
        return Err(ApiError::bad_request(
            "name can contain only English letters, digits, and underscores",
        ));
    }

    if trimmed.contains("__") {
        return Err(ApiError::bad_request("name must not contain consecutive underscores"));
    }

    let required_suffix = format!("_by_{}", bot_username.to_ascii_lowercase());
    if !trimmed.to_ascii_lowercase().ends_with(&required_suffix) {
        return Err(ApiError::bad_request(
            "name must end with _by_<bot_username>",
        ));
    }

    Ok(())
}

fn validate_emoji_list(emoji_list: &[String]) -> Result<(), ApiError> {
    if emoji_list.is_empty() {
        return Err(ApiError::bad_request("emoji_list must include at least one item"));
    }
    if emoji_list.len() > 20 {
        return Err(ApiError::bad_request("emoji_list can include at most 20 items"));
    }

    for emoji in emoji_list {
        if emoji.trim().is_empty() {
            return Err(ApiError::bad_request("emoji_list contains an invalid emoji"));
        }
    }

    Ok(())
}

fn validate_keywords(keywords: Option<&Vec<String>>) -> Result<(), ApiError> {
    let Some(items) = keywords else {
        return Ok(());
    };

    if items.len() > 20 {
        return Err(ApiError::bad_request("keywords can include at most 20 items"));
    }

    let mut total_chars = 0usize;
    for keyword in items {
        let trimmed = keyword.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("keywords contains an invalid value"));
        }
        total_chars += trimmed.chars().count();
    }

    if total_chars > 64 {
        return Err(ApiError::bad_request(
            "keywords total length must not exceed 64 characters",
        ));
    }

    Ok(())
}

fn validate_input_sticker(sticker: &InputSticker, sticker_type: &str) -> Result<(), ApiError> {
    if sticker.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("input sticker file reference is empty"));
    }
    messages::normalize_sticker_format(&sticker.format)?;
    validate_emoji_list(&sticker.emoji_list)?;

    match sticker_type {
        "mask" => {
            if sticker.keywords.is_some() {
                return Err(ApiError::bad_request(
                    "keywords are supported only for regular and custom emoji stickers",
                ));
            }
        }
        "regular" | "custom_emoji" => {
            if sticker.mask_position.is_some() {
                return Err(ApiError::bad_request(
                    "mask_position is supported only for mask stickers",
                ));
            }
            validate_keywords(sticker.keywords.as_ref())?;
        }
        _ => {
            return Err(ApiError::bad_request("sticker_type is invalid"));
        }
    }

    Ok(())
}

fn load_sticker_set_record(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    name: &str,
) -> Result<Option<StickerSetRecord>, ApiError> {
    conn
        .query_row(
            "SELECT sticker_type, COALESCE(needs_repainting, 0), owner_user_id
             FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot_id, name],
            |r| {
                Ok(StickerSetRecord {
                    sticker_type: r.get(0)?,
                    needs_repainting: r.get::<_, i64>(1)? == 1,
                    owner_user_id: r.get(2)?,
                })
            },
        )
        .optional()
        .map_err(ApiError::internal)
}

fn build_sticker_set_thumbnail(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    file_id: Option<&str>,
) -> Result<Option<PhotoSize>, ApiError> {
    let Some(file_id) = file_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };

    let row: Option<(String, Option<i64>)> = conn
        .query_row(
            "SELECT file_unique_id, file_size FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot_id, file_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    Ok(row.map(|(file_unique_id, file_size)| PhotoSize {
        file_id: file_id.to_string(),
        file_unique_id,
        width: 100,
        height: 100,
        file_size,
    }))
}

pub fn handle_add_sticker_to_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: AddStickerToSetRequest = parse_request(params)?;
    ensure_positive_user_id(request.user_id, "user_id")?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let Some(set_record) = load_sticker_set_record(&mut conn, bot.id, &request.name)? else {
        return Err(ApiError::not_found("sticker set not found"));
    };
    if set_record.owner_user_id != request.user_id {
        return Err(ApiError::bad_request("user_id does not match sticker set owner"));
    }

    validate_input_sticker(&request.sticker, &set_record.sticker_type)?;

    let sticker_count: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM stickers WHERE bot_id = ?1 AND set_name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    let max_allowed = if set_record.sticker_type == "custom_emoji" { 200 } else { 120 };
    if sticker_count >= max_allowed {
        return Err(ApiError::bad_request(format!(
            "sticker set reached the maximum of {} stickers",
            max_allowed,
        )));
    }

    let next_position: i64 = conn
        .query_row(
            "SELECT COALESCE(MAX(position), -1) + 1 FROM stickers WHERE bot_id = ?1 AND set_name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    messages::upsert_set_sticker(
        state,
        &mut conn,
        &bot,
        &request.name,
        &set_record.sticker_type,
        set_record.needs_repainting,
        &request.sticker,
        next_position,
    )?;

    Ok(json!(true))
}

pub fn handle_create_new_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: CreateNewStickerSetRequest = parse_request(params)?;
    ensure_positive_user_id(request.user_id, "user_id")?;

    let sticker_type = messages::normalize_sticker_type(request.sticker_type.as_deref().unwrap_or("regular"))?;
    validate_sticker_set_title(&request.title)?;

    if request.needs_repainting.is_some() && sticker_type != "custom_emoji" {
        return Err(ApiError::bad_request(
            "needs_repainting is supported only for custom emoji sticker sets",
        ));
    }

    if request.stickers.is_empty() {
        return Err(ApiError::bad_request("stickers must include at least one item"));
    }

    let max_initial_stickers = if sticker_type == "mask" { 120 } else { 50 };
    if request.stickers.len() > max_initial_stickers {
        return Err(ApiError::bad_request(format!(
            "stickers can include at most {} items for this sticker type",
            max_initial_stickers,
        )));
    }

    for sticker in &request.stickers {
        validate_input_sticker(sticker, sticker_type)?;
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    validate_sticker_set_name(&request.name, &bot.username)?;

    let exists: Option<String> = conn
        .query_row(
            "SELECT name FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_some() {
        return Err(ApiError::bad_request("sticker set already exists"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sticker_sets
         (bot_id, name, title, sticker_type, needs_repainting, owner_user_id, thumbnail_file_id, thumbnail_format, custom_emoji_id, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, NULL, NULL, ?7, ?7)",
        params![
            bot.id,
            request.name,
            request.title,
            sticker_type,
            if request.needs_repainting.unwrap_or(false) { 1 } else { 0 },
            request.user_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    for (index, sticker_input) in request.stickers.iter().enumerate() {
        messages::upsert_set_sticker(
            state,
            &mut conn,
            &bot,
            &request.name,
            sticker_type,
            request.needs_repainting.unwrap_or(false),
            sticker_input,
            index as i64,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_delete_sticker_from_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteStickerFromSetRequest = parse_request(params)?;
    if request.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("sticker is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name_row: Option<Option<String>> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name_row else {
        return Err(ApiError::not_found("sticker not found"));
    };
    let Some(set_name) = set_name else {
        return Err(ApiError::bad_request("sticker is not part of a sticker set"));
    };

    let deleted = conn
        .execute(
            "DELETE FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    messages::compact_sticker_positions(&mut conn, bot.id, &set_name)?;
    Ok(json!(true))
}

pub fn handle_delete_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: DeleteStickerSetRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let deleted = conn
        .execute(
            "DELETE FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if deleted == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    conn.execute(
        "DELETE FROM stickers WHERE bot_id = ?1 AND set_name = ?2",
        params![bot.id, request.name],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

pub fn handle_get_custom_emoji_stickers(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetCustomEmojiStickersRequest = parse_request(params)?;
    if request.custom_emoji_ids.len() > 200 {
        return Err(ApiError::bad_request(
            "custom_emoji_ids can include at most 200 items",
        ));
    }

    if request.custom_emoji_ids.is_empty() {
        return Ok(json!([]));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let placeholders = std::iter::repeat("?")
        .take(request.custom_emoji_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT file_id, file_unique_id, set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, needs_repainting
         FROM stickers WHERE bot_id = ? AND custom_emoji_id IN ({})",
        placeholders,
    );

    let mut bind_values = Vec::with_capacity(1 + request.custom_emoji_ids.len());
    bind_values.push(Value::from(bot.id));
    for item in &request.custom_emoji_ids {
        bind_values.push(Value::from(item.clone()));
    }

    let mut stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(
            rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)),
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<String>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, Option<String>>(5)?,
                    r.get::<_, Option<String>>(6)?,
                    r.get::<_, Option<String>>(7)?,
                    r.get::<_, i64>(8)?,
                ))
            },
        )
        .map_err(ApiError::internal)?;

    let mut stickers = Vec::new();
    for row in rows {
        let (file_id, file_unique_id, set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, needs_repainting) = row.map_err(ApiError::internal)?;
        stickers.push(messages::sticker_from_row(
            file_id,
            file_unique_id,
            set_name,
            sticker_type,
            format,
            emoji,
            mask_position_json,
            custom_emoji_id,
            needs_repainting == 1,
            None,
        ));
    }

    Ok(serde_json::to_value(stickers).map_err(ApiError::internal)?)
}

pub fn handle_get_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetStickerSetRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_row: Option<(String, String, Option<String>)> = conn
        .query_row(
            "SELECT title, sticker_type, thumbnail_file_id
             FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((title, sticker_type, thumbnail_file_id)) = set_row else {
        return Err(ApiError::not_found("sticker set not found"));
    };

    let stickers = messages::load_set_stickers(&mut conn, bot.id, &request.name)?;
    let thumbnail = build_sticker_set_thumbnail(&mut conn, bot.id, thumbnail_file_id.as_deref())?;
    let result = StickerSet {
        name: request.name,
        title,
        sticker_type,
        stickers,
        thumbnail,
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}

pub fn handle_replace_sticker_in_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: ReplaceStickerInSetRequest = parse_request(params)?;
    ensure_positive_user_id(request.user_id, "user_id")?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }
    if request.old_sticker.trim().is_empty() {
        return Err(ApiError::bad_request("old_sticker is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let Some(set_record) = load_sticker_set_record(&mut conn, bot.id, &request.name)? else {
        return Err(ApiError::not_found("sticker set not found"));
    };
    if set_record.owner_user_id != request.user_id {
        return Err(ApiError::bad_request("user_id does not match sticker set owner"));
    }
    validate_input_sticker(&request.sticker, &set_record.sticker_type)?;

    if request.old_sticker.trim() == request.sticker.sticker.trim() {
        return Ok(json!(true));
    }

    let old_row: Option<(i64, String, i64)> = conn
        .query_row(
            "SELECT position, set_name, COALESCE(needs_repainting, 0)
             FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.old_sticker],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((position, set_name, needs_repainting)) = old_row else {
        return Err(ApiError::not_found("old_sticker not found"));
    };
    if set_name != request.name {
        return Err(ApiError::bad_request(
            "old_sticker does not belong to the provided sticker set",
        ));
    }

    conn.execute(
        "DELETE FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
        params![bot.id, request.old_sticker],
    )
    .map_err(ApiError::internal)?;

    messages::upsert_set_sticker(
        state,
        &mut conn,
        &bot,
        &set_name,
        &set_record.sticker_type,
        needs_repainting == 1 || set_record.needs_repainting,
        &request.sticker,
        position,
    )?;

    messages::compact_sticker_positions(&mut conn, bot.id, &set_name)?;
    Ok(json!(true))
}

pub fn handle_set_custom_emoji_sticker_set_thumbnail(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetCustomEmojiStickerSetThumbnailRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let Some(set_record) = load_sticker_set_record(&mut conn, bot.id, &request.name)? else {
        return Err(ApiError::not_found("sticker set not found"));
    };
    if set_record.sticker_type != "custom_emoji" {
        return Err(ApiError::bad_request(
            "setCustomEmojiStickerSetThumbnail is supported only for custom emoji sticker sets",
        ));
    }

    let normalized_custom_emoji_id = request
        .custom_emoji_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(custom_emoji_id) = normalized_custom_emoji_id.as_deref() {
        let exists: Option<String> = conn
            .query_row(
                "SELECT custom_emoji_id FROM stickers
                 WHERE bot_id = ?1 AND set_name = ?2 AND custom_emoji_id = ?3",
                params![bot.id, request.name, custom_emoji_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;
        if exists.is_none() {
            return Err(ApiError::bad_request(
                "custom_emoji_id must refer to a sticker from the target sticker set",
            ));
        }
    }

    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets SET custom_emoji_id = ?1, updated_at = ?2 WHERE bot_id = ?3 AND name = ?4",
            params![normalized_custom_emoji_id, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_emoji_list(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerEmojiListRequest = parse_request(params)?;
    if request.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("sticker is empty"));
    }
    validate_emoji_list(&request.emoji_list)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let sticker_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT set_name, sticker_type FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((set_name, sticker_type)) = sticker_row else {
        return Err(ApiError::not_found("sticker not found"));
    };
    if set_name.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
        return Err(ApiError::bad_request("sticker is not part of a sticker set"));
    }
    if sticker_type != "regular" && sticker_type != "custom_emoji" {
        return Err(ApiError::bad_request(
            "setStickerEmojiList is supported only for regular and custom emoji stickers",
        ));
    }

    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers
             SET emoji = ?1, emoji_list_json = ?2, updated_at = ?3
             WHERE bot_id = ?4 AND file_id = ?5",
            params![
                request.emoji_list[0].clone(),
                serde_json::to_string(&request.emoji_list).map_err(ApiError::internal)?,
                now,
                bot.id,
                request.sticker,
            ],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_keywords(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerKeywordsRequest = parse_request(params)?;
    if request.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("sticker is empty"));
    }
    let normalized_keywords = request.keywords.as_ref().map(|items| {
        items
            .iter()
            .map(|item| item.trim().to_string())
            .collect::<Vec<_>>()
    });
    validate_keywords(normalized_keywords.as_ref())?;

    let keywords_json = normalized_keywords
        .as_ref()
        .map(|k| serde_json::to_string(k).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let sticker_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT set_name, sticker_type FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((set_name, sticker_type)) = sticker_row else {
        return Err(ApiError::not_found("sticker not found"));
    };
    if set_name.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
        return Err(ApiError::bad_request("sticker is not part of a sticker set"));
    }
    if sticker_type != "regular" && sticker_type != "custom_emoji" {
        return Err(ApiError::bad_request(
            "setStickerKeywords is supported only for regular and custom emoji stickers",
        ));
    }

    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers SET keywords_json = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![keywords_json, now, bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_mask_position(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerMaskPositionRequest = parse_request(params)?;
    if request.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("sticker is empty"));
    }
    let mask_json = request
        .mask_position
        .as_ref()
        .map(|m| serde_json::to_string(m).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let sticker_row: Option<(Option<String>, String)> = conn
        .query_row(
            "SELECT set_name, sticker_type FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((set_name, sticker_type)) = sticker_row else {
        return Err(ApiError::not_found("sticker not found"));
    };
    if set_name.as_deref().map(str::trim).filter(|value| !value.is_empty()).is_none() {
        return Err(ApiError::bad_request("sticker is not part of a sticker set"));
    }
    if sticker_type != "mask" {
        return Err(ApiError::bad_request(
            "setStickerMaskPosition is supported only for mask stickers",
        ));
    }

    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE stickers SET mask_position_json = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![mask_json, now, bot.id, request.sticker],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_position_in_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerPositionInSetRequest = parse_request(params)?;
    if request.sticker.trim().is_empty() {
        return Err(ApiError::bad_request("sticker is empty"));
    }
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name_row: Option<Option<String>> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name_row else {
        return Err(ApiError::not_found("sticker not found in set"));
    };
    let Some(set_name) = set_name else {
        return Err(ApiError::bad_request("sticker is not part of a sticker set"));
    };

    let mut stmt = conn
        .prepare(
            "SELECT file_id FROM stickers WHERE bot_id = ?1 AND set_name = ?2 ORDER BY position ASC, created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot.id, set_name], |r| r.get::<_, String>(0))
        .map_err(ApiError::internal)?;
    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(ApiError::internal)?);
    }

    if ids.is_empty() {
        return Err(ApiError::bad_request("sticker set has no stickers"));
    }
    if request.position < 0 || request.position >= ids.len() as i64 {
        return Err(ApiError::bad_request(
            "position must be within sticker set bounds",
        ));
    }

    let current_index = ids.iter().position(|id| id == &request.sticker)
        .ok_or_else(|| ApiError::not_found("sticker not found in set"))?;
    let target = request.position as usize;

    let moved = ids.remove(current_index);
    ids.insert(target, moved);

    let now = Utc::now().timestamp();
    for (idx, file_id) in ids.iter().enumerate() {
        conn.execute(
            "UPDATE stickers SET position = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![idx as i64, now, bot.id, file_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_set_thumbnail(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerSetThumbnailRequest = parse_request(params)?;
    ensure_positive_user_id(request.user_id, "user_id")?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }

    let format = messages::normalize_sticker_format(&request.format)?;

    if format != "static" {
        if let Some(Value::String(raw_thumbnail)) = request.thumbnail.as_ref() {
            let normalized_thumbnail = raw_thumbnail.trim();
            if normalized_thumbnail.starts_with("http://") || normalized_thumbnail.starts_with("https://") {
                return Err(ApiError::bad_request(
                    "animated and video sticker set thumbnails can't be uploaded via HTTP URL",
                ));
            }
        }
    }

    let thumbnail_file_id = if let Some(value) = request.thumbnail {
        let normalized = messages::parse_input_file_value(&value, "thumbnail")?;
        Some(messages::resolve_media_file(state, token, &normalized, "thumbnail")?.file_id)
    } else {
        None
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let Some(set_record) = load_sticker_set_record(&mut conn, bot.id, &request.name)? else {
        return Err(ApiError::not_found("sticker set not found"));
    };
    if set_record.owner_user_id != request.user_id {
        return Err(ApiError::bad_request("user_id does not match sticker set owner"));
    }
    if set_record.sticker_type == "custom_emoji" {
        return Err(ApiError::bad_request(
            "setStickerSetThumbnail is supported only for regular or mask sticker sets",
        ));
    }

    let set_format: Option<String> = conn
        .query_row(
            "SELECT format FROM stickers WHERE bot_id = ?1 AND set_name = ?2 ORDER BY position ASC, created_at ASC LIMIT 1",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some(existing_format) = set_format {
        if existing_format != format {
            return Err(ApiError::bad_request(
                "thumbnail format must match sticker format of the set",
            ));
        }
    }

    if let Some(file_id) = thumbnail_file_id.as_deref() {
        let thumbnail_row: Option<(String, Option<String>, Option<String>)> = conn
            .query_row(
                "SELECT s.format, f.mime_type, f.file_path
                 FROM stickers s
                 LEFT JOIN files f ON f.bot_id = s.bot_id AND f.file_id = s.file_id
                 WHERE s.bot_id = ?1 AND s.file_id = ?2",
                params![bot.id, file_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        let inferred = if let Some((stored_format, _, _)) = thumbnail_row {
            Some(stored_format)
        } else {
            let file_row: Option<(Option<String>, Option<String>)> = conn
                .query_row(
                    "SELECT mime_type, file_path FROM files WHERE bot_id = ?1 AND file_id = ?2",
                    params![bot.id, file_id],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()
                .map_err(ApiError::internal)?;

            file_row.map(|(mime_type, file_path)| {
                let mime = mime_type.unwrap_or_default().to_ascii_lowercase();
                let path = file_path.unwrap_or_default().to_ascii_lowercase();

                if mime.contains("webm") || path.ends_with(".webm") {
                    "video".to_string()
                } else if mime.contains("x-tgsticker") || path.ends_with(".tgs") {
                    "animated".to_string()
                } else {
                    "static".to_string()
                }
            })
        };

        if let Some(inferred_format) = inferred {
            if inferred_format != format {
                return Err(ApiError::bad_request(
                    "thumbnail format must match the provided format parameter",
                ));
            }
        }
    }

    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets
             SET thumbnail_file_id = ?1, thumbnail_format = ?2, updated_at = ?3
             WHERE bot_id = ?4 AND name = ?5",
            params![thumbnail_file_id, format, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_set_title(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerSetTitleRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }
    validate_sticker_set_title(&request.title)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets SET title = ?1, updated_at = ?2 WHERE bot_id = ?3 AND name = ?4",
            params![request.title, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

pub fn handle_upload_sticker_file(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: UploadStickerFileRequest = parse_request(params)?;
    ensure_positive_user_id(request.user_id, "user_id")?;

    let requested_format = messages::normalize_sticker_format(&request.sticker_format)?;
    let sticker_input = messages::parse_input_file_value(&request.sticker.extra, "sticker")?;
    let file = messages::resolve_media_file(state, token, &sticker_input, "sticker")?;
    let inferred_format = messages::infer_sticker_format_from_file(&file);
    if let Some(actual_format) = inferred_format {
        if actual_format != requested_format {
            return Err(ApiError::bad_request(
                "sticker_format doesn't match the provided sticker file",
            ));
        }
    }
    let format = inferred_format.unwrap_or(requested_format);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let (is_animated, is_video) = messages::sticker_format_flags(format);

    conn.execute(
        "INSERT INTO stickers
         (bot_id, file_id, file_unique_id, set_name, sticker_type, format, width, height, is_animated, is_video,
          emoji, emoji_list_json, keywords_json, mask_position_json, custom_emoji_id, needs_repainting, position, created_at, updated_at)
         VALUES (?1, ?2, ?3, NULL, 'regular', ?4, 512, 512, ?5, ?6, NULL, NULL, NULL, NULL, NULL, 0, 0, ?7, ?7)
         ON CONFLICT(bot_id, file_id) DO UPDATE SET
            format = excluded.format,
            is_animated = excluded.is_animated,
            is_video = excluded.is_video,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            file.file_id,
            file.file_unique_id,
            format,
            if is_animated { 1 } else { 0 },
            if is_video { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let result = File {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        file_size: file.file_size,
        file_path: Some(file.file_path),
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}
