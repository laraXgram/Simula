use actix_web::web::Data;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::database::AppState;
use crate::types::ApiError;

use crate::generated::types::{BusinessConnection, Chat, StoryArea};
use crate::handlers::client::types::business::SimBusinessStoryRecord;
use super::messages;

pub fn ensure_story_active_period(active_period: i64) -> Result<(), ApiError> {
    match active_period {
        21_600 | 43_200 | 86_400 | 172_800 => Ok(()),
        _ => Err(ApiError::bad_request(
            "active_period must be one of 21600, 43200, 86400, 172800",
        )),
    }
}

pub fn validate_story_content_payload(content: &Value) -> Result<(), ApiError> {
    let object = content
        .as_object()
        .ok_or_else(|| ApiError::bad_request("content must be a JSON object"))?;

    let content_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.to_ascii_lowercase())
        .ok_or_else(|| ApiError::bad_request("content.type is required"))?;

    match content_type.as_str() {
        "photo" => {
            let has_photo = object
                .get("photo")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
            if !has_photo {
                return Err(ApiError::bad_request("content.photo is required"));
            }
            Ok(())
        }
        "video" => {
            let has_video = object
                .get("video")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .is_some();
            if !has_video {
                return Err(ApiError::bad_request("content.video is required"));
            }

            if let Some(duration) = object.get("duration").and_then(Value::as_f64) {
                if !(0.0..=60.0).contains(&duration) {
                    return Err(ApiError::bad_request(
                        "content.duration must be between 0 and 60",
                    ));
                }
            }

            Ok(())
        }
        _ => Err(ApiError::bad_request(
            "content.type must be one of: photo, video",
        )),
    }
}

pub fn validate_story_areas_payload(areas: Option<&Vec<StoryArea>>) -> Result<(), ApiError> {
    let Some(areas) = areas else {
        return Ok(());
    };

    if areas.len() > 10 {
        return Err(ApiError::bad_request("a story can contain at most 10 areas"));
    }

    let mut location_count = 0;
    let mut suggested_reaction_count = 0;
    let mut link_count = 0;
    let mut weather_count = 0;
    let mut unique_gift_count = 0;

    for area in areas {
        let position = &area.position;
        if !position.x_percentage.is_finite()
            || position.x_percentage < 0.0
            || position.x_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.x_percentage must be between 0 and 100",
            ));
        }
        if !position.y_percentage.is_finite()
            || position.y_percentage < 0.0
            || position.y_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.y_percentage must be between 0 and 100",
            ));
        }
        if !position.width_percentage.is_finite()
            || position.width_percentage <= 0.0
            || position.width_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.width_percentage must be between 0 and 100",
            ));
        }
        if !position.height_percentage.is_finite()
            || position.height_percentage <= 0.0
            || position.height_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.height_percentage must be between 0 and 100",
            ));
        }
        if !position.rotation_angle.is_finite() || position.rotation_angle.abs() > 360.0 {
            return Err(ApiError::bad_request(
                "story area position.rotation_angle must be finite and between -360 and 360",
            ));
        }
        if !position.corner_radius_percentage.is_finite()
            || position.corner_radius_percentage < 0.0
            || position.corner_radius_percentage > 100.0
        {
            return Err(ApiError::bad_request(
                "story area position.corner_radius_percentage must be between 0 and 100",
            ));
        }

        let area_object = area
            .r#type
            .extra
            .as_object()
            .ok_or_else(|| ApiError::bad_request("story area payload is invalid"))?;
        let area_type = area
            .r#type
            .extra
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("story area type is invalid"))?;

        match area_type.as_str() {
            "location" => {
                location_count += 1;
                if location_count > 10 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 10 location areas",
                    ));
                }

                let latitude = area_object
                    .get("latitude")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("location area latitude is required"))?;
                let longitude = area_object
                    .get("longitude")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("location area longitude is required"))?;

                if !latitude.is_finite() || !(-90.0..=90.0).contains(&latitude) {
                    return Err(ApiError::bad_request(
                        "location area latitude must be between -90 and 90",
                    ));
                }
                if !longitude.is_finite() || !(-180.0..=180.0).contains(&longitude) {
                    return Err(ApiError::bad_request(
                        "location area longitude must be between -180 and 180",
                    ));
                }
            }
            "suggested_reaction" => {
                suggested_reaction_count += 1;
                if suggested_reaction_count > 5 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 5 suggested reaction areas",
                    ));
                }

                let reaction_type = area_object
                    .get("reaction_type")
                    .and_then(Value::as_object)
                    .ok_or_else(|| ApiError::bad_request("suggested_reaction.reaction_type is required"))?;
                let reaction_kind = reaction_type
                    .get("type")
                    .and_then(Value::as_str)
                    .map(|value| value.to_ascii_lowercase())
                    .ok_or_else(|| ApiError::bad_request("suggested_reaction.reaction_type.type is required"))?;

                match reaction_kind.as_str() {
                    "emoji" => {
                        let emoji = reaction_type
                            .get("emoji")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        if emoji.is_empty() {
                            return Err(ApiError::bad_request(
                                "suggested_reaction emoji value is required",
                            ));
                        }
                    }
                    "custom_emoji" => {
                        let custom_emoji_id = reaction_type
                            .get("custom_emoji_id")
                            .and_then(Value::as_str)
                            .map(str::trim)
                            .unwrap_or("");
                        if custom_emoji_id.is_empty() {
                            return Err(ApiError::bad_request(
                                "suggested_reaction custom_emoji_id is required",
                            ));
                        }
                    }
                    "paid" => {}
                    _ => {
                        return Err(ApiError::bad_request(
                            "suggested_reaction reaction_type.type is invalid",
                        ));
                    }
                }
            }
            "link" => {
                link_count += 1;
                if link_count > 3 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 3 link areas",
                    ));
                }

                let url = area_object
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if url.is_empty() {
                    return Err(ApiError::bad_request("link area url is required"));
                }
                if !(url.starts_with("https://")
                    || url.starts_with("http://")
                    || url.starts_with("tg://"))
                {
                    return Err(ApiError::bad_request(
                        "link area url must start with https://, http://, or tg://",
                    ));
                }
            }
            "weather" => {
                weather_count += 1;
                if weather_count > 3 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 3 weather areas",
                    ));
                }

                let temperature = area_object
                    .get("temperature")
                    .and_then(Value::as_f64)
                    .ok_or_else(|| ApiError::bad_request("weather area temperature is required"))?;
                if !temperature.is_finite() || !(-100.0..=100.0).contains(&temperature) {
                    return Err(ApiError::bad_request(
                        "weather area temperature must be between -100 and 100",
                    ));
                }

                let emoji = area_object
                    .get("emoji")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if emoji.is_empty() {
                    return Err(ApiError::bad_request("weather area emoji is required"));
                }

                let background_color = area_object
                    .get("background_color")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| ApiError::bad_request("weather area background_color is required"))?;
                if !(0..=0xFFFFFF).contains(&background_color) {
                    return Err(ApiError::bad_request(
                        "weather area background_color must be between 0 and 16777215",
                    ));
                }
            }
            "unique_gift" => {
                unique_gift_count += 1;
                if unique_gift_count > 1 {
                    return Err(ApiError::bad_request(
                        "a story can have at most 1 unique gift area",
                    ));
                }

                let name = area_object
                    .get("name")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .unwrap_or("");
                if name.is_empty() {
                    return Err(ApiError::bad_request("unique_gift area name is required"));
                }
            }
            _ => {
                return Err(ApiError::bad_request("story area type is not supported"));
            }
        }
    }

    Ok(())
}

pub fn ensure_sim_story_storage(conn: &mut rusqlite::Connection) -> Result<(), ApiError> {
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS sim_business_stories (
            bot_id INTEGER NOT NULL,
            business_connection_id TEXT NOT NULL,
            story_id INTEGER NOT NULL,
            owner_chat_id INTEGER NOT NULL,
            content_json TEXT NOT NULL,
            caption TEXT,
            caption_entities_json TEXT,
            areas_json TEXT,
            active_period INTEGER NOT NULL,
            post_to_chat_page INTEGER NOT NULL DEFAULT 0,
            protect_content INTEGER NOT NULL DEFAULT 0,
            source_chat_id INTEGER,
            source_story_id INTEGER,
            created_at INTEGER NOT NULL,
            updated_at INTEGER NOT NULL,
            PRIMARY KEY (bot_id, business_connection_id, story_id)
        );
        CREATE INDEX IF NOT EXISTS idx_sim_business_stories_chat_story
            ON sim_business_stories (bot_id, owner_chat_id, story_id);",
    )
    .map_err(ApiError::internal)
}

pub fn next_story_id_for_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
) -> Result<i64, ApiError> {
    let max_story_id: Option<i64> = conn
        .query_row(
            "SELECT MAX(story_id)
             FROM sim_business_stories
             WHERE bot_id = ?1 AND business_connection_id = ?2",
            params![bot_id, business_connection_id],
            |row| row.get(0),
        )
        .map_err(ApiError::internal)?;

    Ok(max_story_id.unwrap_or(0) + 1)
}

pub fn load_story_record_for_connection(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    business_connection_id: &str,
    story_id: i64,
) -> Result<Option<SimBusinessStoryRecord>, ApiError> {
    conn.query_row(
        "SELECT business_connection_id, story_id, owner_chat_id,
                content_json, caption, caption_entities_json, areas_json
         FROM sim_business_stories
         WHERE bot_id = ?1 AND business_connection_id = ?2 AND story_id = ?3",
        params![bot_id, business_connection_id, story_id],
        |row| {
            Ok(SimBusinessStoryRecord {
                business_connection_id: row.get(0)?,
                story_id: row.get(1)?,
                owner_chat_id: row.get(2)?,
                content_json: row.get(3)?,
                caption: row.get(4)?,
                caption_entities_json: row.get(5)?,
                areas_json: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn load_story_record_for_chat(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    owner_chat_id: i64,
    story_id: i64,
) -> Result<Option<SimBusinessStoryRecord>, ApiError> {
    conn.query_row(
        "SELECT business_connection_id, story_id, owner_chat_id,
                content_json, caption, caption_entities_json, areas_json
         FROM sim_business_stories
         WHERE bot_id = ?1 AND owner_chat_id = ?2 AND story_id = ?3",
        params![bot_id, owner_chat_id, story_id],
        |row| {
            Ok(SimBusinessStoryRecord {
                business_connection_id: row.get(0)?,
                story_id: row.get(1)?,
                owner_chat_id: row.get(2)?,
                content_json: row.get(3)?,
                caption: row.get(4)?,
                caption_entities_json: row.get(5)?,
                areas_json: row.get(6)?,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn story_chat_for_business_connection(connection: &BusinessConnection) -> Chat {
    Chat {
        id: connection.user.id,
        r#type: "private".to_string(),
        title: None,
        username: connection.user.username.clone(),
        first_name: Some(connection.user.first_name.clone()),
        last_name: connection.user.last_name.clone(),
        is_forum: None,
        is_direct_messages: None,
    }
}

pub fn normalize_story_content_payload(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
    content: &Value,
) -> Result<Value, ApiError> {
    let mut object = content
        .as_object()
        .cloned()
        .ok_or_else(|| ApiError::bad_request("content must be a JSON object"))?;

    let content_type = object
        .get("type")
        .and_then(Value::as_str)
        .map(|value| value.trim().to_ascii_lowercase())
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request("content.type is required"))?;

    let media_field = match content_type.as_str() {
        "photo" => "photo",
        "video" => "video",
        _ => {
            return Err(ApiError::bad_request(
                "content.type must be one of: photo, video",
            ));
        }
    };

    let media_ref = object
        .get(media_field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("content.{} is required", media_field)))?;

    let resolved_input = if let Some(attach_key) = media_ref.strip_prefix("attach://") {
        let attach_key = attach_key.trim();
        if attach_key.is_empty() {
            return Err(ApiError::bad_request(format!(
                "content.{} attachment reference is invalid",
                media_field
            )));
        }

        params.get(attach_key).cloned().ok_or_else(|| {
            ApiError::bad_request(format!(
                "content.{} attachment '{}' was not provided",
                media_field, attach_key
            ))
        })?
    } else {
        Value::String(media_ref.to_string())
    };

    let stored_file = messages::resolve_media_file(state, token, &resolved_input, media_field)?;
    object.insert(
        media_field.to_string(),
        Value::String(stored_file.file_id),
    );
    object.insert("type".to_string(), Value::String(content_type));

    Ok(Value::Object(object))
}

pub fn build_story_response_payload(
    chat: Chat,
    story_id: i64,
    content: Option<&Value>,
    caption: Option<&str>,
) -> Value {
    let mut payload = json!({
        "chat": chat,
        "id": story_id,
    });

    if let Some(content_value) = content {
        payload["content"] = content_value.clone();
    }
    if let Some(value) = caption {
        let trimmed = value.trim();
        if !trimmed.is_empty() {
            payload["caption"] = Value::String(trimmed.to_string());
        }
    }

    payload
}