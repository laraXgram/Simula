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
};

use crate::generated::types::{File, StickerSet};

use crate::handlers::client::messages;

use crate::handlers::{parse_request, sql_value_to_rusqlite};

pub fn handle_add_sticker_to_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: AddStickerToSetRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT sticker_type, COALESCE(needs_repainting, 0) FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some((sticker_type, needs_repainting)) = row else {
        return Err(ApiError::not_found("sticker set not found"));
    };

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
        &sticker_type,
        needs_repainting == 1,
        &request.sticker,
        next_position,
    )?;

    Ok(json!(true))
}

pub fn handle_create_new_sticker_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: CreateNewStickerSetRequest = parse_request(params)?;
    if request.name.trim().is_empty() {
        return Err(ApiError::bad_request("name is empty"));
    }
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.stickers.is_empty() {
        return Err(ApiError::bad_request("stickers must include at least one item"));
    }

    let sticker_type = messages::normalize_sticker_type(request.sticker_type.as_deref().unwrap_or("regular"))?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

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
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name: Option<String> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name else {
        return Err(ApiError::not_found("sticker not found"));
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
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_row: Option<(String, String, i64)> = conn
        .query_row(
            "SELECT title, sticker_type, COALESCE(needs_repainting, 0)
             FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((title, sticker_type, _needs_repainting)) = set_row else {
        return Err(ApiError::not_found("sticker set not found"));
    };

    let stickers = messages::load_set_stickers(&mut conn, bot.id, &request.name)?;
    let result = StickerSet {
        name: request.name,
        title,
        sticker_type,
        stickers,
        thumbnail: None,
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}

pub fn handle_replace_sticker_in_set(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: ReplaceStickerInSetRequest = parse_request(params)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

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

    let set_type: String = conn
        .query_row(
            "SELECT sticker_type FROM sticker_sets WHERE bot_id = ?1 AND name = ?2",
            params![bot.id, request.name],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

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
        &set_type,
        needs_repainting == 1,
        &request.sticker,
        position,
    )?;

    messages::compact_sticker_positions(&mut conn, bot.id, &set_name)?;
    Ok(json!(true))
}

pub fn handle_set_custom_emoji_sticker_set_thumbnail(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetCustomEmojiStickerSetThumbnailRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    let updated = conn
        .execute(
            "UPDATE sticker_sets SET custom_emoji_id = ?1, updated_at = ?2 WHERE bot_id = ?3 AND name = ?4",
            params![request.custom_emoji_id, now, bot.id, request.name],
        )
        .map_err(ApiError::internal)?;
    if updated == 0 {
        return Err(ApiError::not_found("sticker set not found"));
    }

    Ok(json!(true))
}

pub fn handle_set_sticker_emoji_list(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SetStickerEmojiListRequest = parse_request(params)?;
    if request.emoji_list.is_empty() {
        return Err(ApiError::bad_request("emoji_list must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
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
    let keywords_json = request
        .keywords
        .as_ref()
        .map(|k| serde_json::to_string(k).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
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
    let mask_json = request
        .mask_position
        .as_ref()
        .map(|m| serde_json::to_string(m).map_err(ApiError::internal))
        .transpose()?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
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
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let set_name: Option<String> = conn
        .query_row(
            "SELECT set_name FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.sticker],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let Some(set_name) = set_name else {
        return Err(ApiError::not_found("sticker not found in set"));
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

    let current_index = ids.iter().position(|id| id == &request.sticker)
        .ok_or_else(|| ApiError::not_found("sticker not found in set"))?;
    let target = request.position.clamp(0, (ids.len().saturating_sub(1)) as i64) as usize;

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
    let format = messages::normalize_sticker_format(&request.format)?;
    let thumbnail_file_id = if let Some(value) = request.thumbnail {
        let normalized = messages::parse_input_file_value(&value, "thumbnail")?;
        Some(messages::resolve_media_file(state, token, &normalized, "thumbnail")?.file_id)
    } else {
        None
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
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
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }

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
    let user_id = params
        .get("user_id")
        .and_then(|value| value.as_i64().or_else(|| value.as_str().and_then(|raw| raw.parse::<i64>().ok())))
        .ok_or_else(|| ApiError::bad_request("user_id is required"))?;
    let sticker_format = params
        .get("sticker_format")
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| ApiError::bad_request("sticker_format is required"))?;
    let requested_format = messages::normalize_sticker_format(&sticker_format)?;
    let sticker_raw = params
        .get("sticker")
        .ok_or_else(|| ApiError::bad_request("sticker is required"))?;
    let sticker_input = messages::parse_input_file_value(sticker_raw, "sticker")?;
    let file = messages::resolve_media_file(state, token, &sticker_input, "sticker")?;
    let format = messages::infer_sticker_format_from_file(&file).unwrap_or(requested_format);

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

    let _ = user_id;

    let result = File {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        file_size: file.file_size,
        file_path: Some(file.file_path),
    };

    Ok(serde_json::to_value(result).map_err(ApiError::internal)?)
}
