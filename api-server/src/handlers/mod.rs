use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{json, Map, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use crate::database::{ensure_bot, ensure_chat, lock_db, AppState};
use crate::generated::methods::{
    DeleteMessageRequest, DeleteMessagesRequest, DeleteWebhookRequest, EditMessageCaptionRequest,
    EditMessageMediaRequest, EditMessageTextRequest, GetFileRequest, GetMeRequest,
    GetUpdatesRequest, SendAudioRequest, SendDocumentRequest, SendMediaGroupRequest,
    SendMessageRequest, SendPhotoRequest, SendVideoRequest, SendVoiceRequest, SetMessageReactionRequest, SetWebhookRequest,
};
use crate::generated::types::{Chat, Message, Update, User};
use crate::types::{strip_nulls, ApiError, ApiResult};

#[derive(Deserialize)]
pub struct SimSendUserMessageRequest {
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub text: String,
    pub parse_mode: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimSendUserMediaRequest {
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<String>,
    pub parse_mode: Option<String>,
    pub reply_to_message_id: Option<i64>,
}

#[derive(Deserialize)]
pub struct SimEditUserMediaRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub media_kind: String,
    pub media: Value,
    pub caption: Option<Value>,
    pub parse_mode: Option<String>,
}

#[derive(Deserialize)]
pub struct SimCreateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpdateBotRequest {
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimUpsertUserRequest {
    pub id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
}

#[derive(Deserialize)]
pub struct SimClearHistoryRequest {
    pub chat_id: i64,
}

#[derive(Deserialize)]
pub struct SimSetUserReactionRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub reaction: Option<Vec<Value>>,
}

pub fn dispatch_method(
    state: &Data<AppState>,
    token: &str,
    method: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    match method.to_ascii_lowercase().as_str() {
        "getme" => handle_get_me(state, token, &params),
        "sendmessage" => handle_send_message(state, token, &params),
        "sendphoto" => handle_send_photo(state, token, &params),
        "sendaudio" => handle_send_audio(state, token, &params),
        "senddocument" => handle_send_document(state, token, &params),
        "sendvideo" => handle_send_video(state, token, &params),
        "sendvoice" => handle_send_voice(state, token, &params),
        "sendmediagroup" => handle_send_media_group(state, token, &params),
        "editmessagetext" => handle_edit_message_text(state, token, &params),
        "editmessagecaption" => handle_edit_message_caption(state, token, &params),
        "editmessagemedia" => handle_edit_message_media(state, token, &params),
        "deletemessage" => handle_delete_message(state, token, &params),
        "deletemessages" => handle_delete_messages(state, token, &params),
        "getfile" => handle_get_file(state, token, &params),
        "getupdates" => handle_get_updates(state, token, &params),
        "setwebhook" => handle_set_webhook(state, token, &params),
        "deletewebhook" => handle_delete_webhook(state, token, &params),
        "setmessagereaction" => handle_set_message_reaction(state, token, &params),
        _ => Err(ApiError::not_found(format!("method {} not found", method))),
    }
}

fn handle_get_me(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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
    };

    Ok(serde_json::to_value(user).map_err(ApiError::internal)?)
}

fn handle_send_message(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendMessageRequest = parse_request(params)?;
    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = value_to_chat_key(&request.chat_id)?;
    ensure_chat(&mut conn, &chat_key)?;

    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, bot.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let chat_id = chat_id_as_i64(&request.chat_id, &chat_key);

    let from = User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
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
    };

    let chat = Chat {
        id: chat_id,
        r#type: "private".to_string(),
        title: None,
        username: None,
        first_name: None,
        last_name: None,
        is_forum: None,
        is_direct_messages: None,
    };

    let base_message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
        "text": parsed_text,
    });

    let mut base_message_json = base_message_json;
    if let Some(entities) = parsed_entities {
        base_message_json["entities"] = entities;
    }

    let message: Message = serde_json::from_value(base_message_json).map_err(ApiError::internal)?;

    let update_stub = Update {
        update_id: 0,
        message: Some(message.clone()),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();

    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    update_value["message"] = message_value.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update.clone());

    Ok(message_value)
}

fn handle_send_photo(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendPhotoRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.photo, "photo")?;
    let photo = json!([
        {
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "width": 1280,
            "height": 720,
            "file_size": file.file_size,
        }
    ]);

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        "photo",
        photo,
    )
}

fn handle_send_audio(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendAudioRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.audio, "audio")?;

    let audio = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "duration": request.duration.unwrap_or(0),
        "performer": request.performer,
        "title": request.title,
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        "audio",
        audio,
    )
}

fn handle_send_document(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendDocumentRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.document, "document")?;

    let document = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        "document",
        document,
    )
}

fn handle_send_video(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVideoRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.video, "video")?;

    let video = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "width": request.width.unwrap_or(1280),
        "height": request.height.unwrap_or(720),
        "duration": request.duration.unwrap_or(0),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        "video",
        video,
    )
}

fn handle_send_voice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVoiceRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let file = resolve_media_file(state, token, &request.voice, "voice")?;

    let voice = json!({
        "file_id": file.file_id,
        "file_unique_id": file.file_unique_id,
        "duration": request.duration.unwrap_or(0),
        "mime_type": file.mime_type,
        "file_size": file.file_size,
    });

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        "voice",
        voice,
    )
}

fn handle_send_media_group(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendMediaGroupRequest = parse_request(params)?;

    if request.media.len() < 2 || request.media.len() > 10 {
        return Err(ApiError::bad_request("media must include 2-10 items"));
    }

    let media_types: Vec<String> = request
        .media
        .iter()
        .map(|item| {
            item.get("type")
                .and_then(Value::as_str)
                .map(|t| t.to_ascii_lowercase())
                .unwrap_or_default()
        })
        .collect();

    if media_types.iter().any(|t| t.is_empty()) {
        return Err(ApiError::bad_request("every media item must include type"));
    }

    if media_types
        .iter()
        .any(|t| t != "photo" && t != "video" && t != "audio" && t != "document")
    {
        return Err(ApiError::bad_request(
            "sendMediaGroup supports only photo, video, audio, and document",
        ));
    }

    let has_audio = media_types.iter().any(|t| t == "audio");
    let has_document = media_types.iter().any(|t| t == "document");

    if has_audio && media_types.iter().any(|t| t != "audio") {
        return Err(ApiError::bad_request(
            "audio media groups can contain only audio items",
        ));
    }

    if has_document && media_types.iter().any(|t| t != "document") {
        return Err(ApiError::bad_request(
            "document media groups can contain only document items",
        ));
    }

    let media_group_id = format!("mg_{}", uuid::Uuid::new_v4().simple());
    let mut result = Vec::with_capacity(request.media.len());

    for raw_item in &request.media {
        let item = raw_item
            .as_object()
            .ok_or_else(|| ApiError::bad_request("media item must be an object"))?;

        let media_type = item
            .get("type")
            .and_then(Value::as_str)
            .map(|v| v.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("media item type is required"))?;

        let media_ref = item
            .get("media")
            .ok_or_else(|| ApiError::bad_request("media item media is required"))?;

        let explicit_caption_entities = item.get("caption_entities").cloned();
        let parse_mode = item.get("parse_mode").and_then(Value::as_str);
        let (caption, caption_entities) = parse_optional_formatted_text(
            item.get("caption").and_then(Value::as_str),
            parse_mode,
            explicit_caption_entities,
        );

        let value = match media_type.as_str() {
            "photo" => {
                let file = resolve_media_file(state, token, media_ref, "photo")?;
                let payload = json!([
                    {
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": 1280,
                        "height": 720,
                        "file_size": file.file_size,
                    }
                ]);
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    "photo",
                    payload,
                    Some(&media_group_id),
                )?
            }
            "video" => {
                let file = resolve_media_file(state, token, media_ref, "video")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "width": item.get("width").and_then(Value::as_i64).unwrap_or(1280),
                    "height": item.get("height").and_then(Value::as_i64).unwrap_or(720),
                    "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    "video",
                    payload,
                    Some(&media_group_id),
                )?
            }
            "audio" => {
                let file = resolve_media_file(state, token, media_ref, "audio")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                    "performer": item.get("performer").and_then(Value::as_str),
                    "title": item.get("title").and_then(Value::as_str),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    "audio",
                    payload,
                    Some(&media_group_id),
                )?
            }
            "document" => {
                let file = resolve_media_file(state, token, media_ref, "document")?;
                let payload = json!({
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
                    "mime_type": file.mime_type,
                    "file_size": file.file_size,
                });
                send_media_message_with_group(
                    state,
                    token,
                    &request.chat_id,
                    caption,
                    caption_entities,
                    "document",
                    payload,
                    Some(&media_group_id),
                )?
            }
            _ => {
                return Err(ApiError::bad_request(
                    "sendMediaGroup supports only photo, video, audio, and document",
                ));
            }
        };

        result.push(value);
    }

    Ok(Value::Array(result))
}

fn handle_get_file(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: GetFileRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, String, Option<i64>, String)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_size, file_path FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, request.file_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((file_id, file_unique_id, file_size, file_path)) = row else {
        return Err(ApiError::not_found("file not found"));
    };

    Ok(json!({
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "file_size": file_size,
        "file_path": file_path
    }))
}

fn send_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    media_field: &str,
    media_payload: Value,
) -> ApiResult {
    send_media_message_with_group(
        state,
        token,
        chat_id_value,
        caption,
        caption_entities,
        media_field,
        media_payload,
        None,
    )
}

fn send_media_message_with_group(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    media_field: &str,
    media_payload: Value,
    media_group_id: Option<&str>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = value_to_chat_key(chat_id_value)?;
    ensure_chat(&mut conn, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, bot.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let chat_id = chat_id_as_i64(chat_id_value, &chat_key);

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": {
            "id": chat_id,
            "type": "private"
        },
        "from": {
            "id": bot.id,
            "is_bot": true,
            "first_name": bot.first_name,
            "username": bot.username
        }
    });

    base[media_field] = media_payload;
    if let Some(c) = caption {
        base["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        base["caption_entities"] = entities;
    }
    if let Some(group_id) = media_group_id {
        base["media_group_id"] = Value::String(group_id.to_string());
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;

    let update_stub = Update {
        update_id: 0,
        message: Some(message),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    update_value["message"] = message_value.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(message_value)
}

#[derive(Debug)]
struct StoredFile {
    file_id: String,
    file_unique_id: String,
    file_path: String,
    mime_type: Option<String>,
    file_size: Option<i64>,
}

fn resolve_media_file(
    state: &Data<AppState>,
    token: &str,
    input: &Value,
    media_kind: &str,
) -> Result<StoredFile, ApiError> {
    let input_text = input
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("{} is invalid", media_kind)))?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    if input_text.starts_with("http://") || input_text.starts_with("https://") {
        let (bytes, mime) = download_remote_file(&input_text)?;
        return store_binary_file(&mut conn, bot.id, &bytes, mime.as_deref(), Some(input_text));
    }

    let local_candidate = if let Some(path) = input_text.strip_prefix("file://") {
        path.to_string()
    } else {
        input_text.clone()
    };

    if !local_candidate.is_empty() && Path::new(&local_candidate).exists() {
        let bytes = fs::read(&local_candidate).map_err(ApiError::internal)?;
        if bytes.is_empty() {
            return Err(ApiError::bad_request("uploaded file is empty"));
        }
        return store_binary_file(
            &mut conn,
            bot.id,
            &bytes,
            None,
            Some(local_candidate),
        );
    }

    let existing: Option<(String, String, String, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_path, mime_type, file_size
             FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, input_text],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some((file_id, file_unique_id, file_path, mime_type, file_size)) = existing {
        return Ok(StoredFile {
            file_id,
            file_unique_id,
            file_path,
            mime_type,
            file_size,
        });
    }

    let now = Utc::now().timestamp();
    let file_id = input_text.clone();
    let file_unique_id = uuid::Uuid::new_v4().simple().to_string();
    let file_path = format!("virtual/{}/{}", bot.id, file_id.replace('/', "_"));

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, ?6)",
        params![bot.id, file_id, file_unique_id, file_path, input_text, now],
    )
    .map_err(ApiError::internal)?;

    Ok(StoredFile {
        file_id,
        file_unique_id,
        file_path,
        mime_type: None,
        file_size: None,
    })
}

fn download_remote_file(url: &str) -> Result<(Vec<u8>, Option<String>), ApiError> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .map_err(ApiError::internal)?;

    let response = client
        .get(url)
        .send()
        .map_err(|_| ApiError::bad_request("failed to fetch remote file"))?;

    if !response.status().is_success() {
        return Err(ApiError::bad_request("remote file url returned non-200 status"));
    }

    let mime = response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let bytes = response.bytes().map_err(ApiError::internal)?;
    if bytes.is_empty() {
        return Err(ApiError::bad_request("remote file is empty"));
    }

    Ok((bytes.to_vec(), mime))
}

fn store_binary_file(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    bytes: &[u8],
    mime_type: Option<&str>,
    source: Option<String>,
) -> Result<StoredFile, ApiError> {
    let now = Utc::now().timestamp();
    let file_id = format!("file_{}", uuid::Uuid::new_v4().simple());
    let file_unique_id = uuid::Uuid::new_v4().simple().to_string();
    let file_path = format!("media/{}/{}", bot_id, file_id);

    let base_dir = media_storage_root();
    fs::create_dir_all(&base_dir).map_err(ApiError::internal)?;
    let local_path = base_dir.join(&file_id);
    fs::write(&local_path, bytes).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![
            bot_id,
            file_id,
            file_unique_id,
            file_path,
            local_path.to_string_lossy().to_string(),
            mime_type,
            bytes.len() as i64,
            source,
            now
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(StoredFile {
        file_id,
        file_unique_id,
        file_path,
        mime_type: mime_type.map(|m| m.to_string()),
        file_size: Some(bytes.len() as i64),
    })
}

fn media_storage_root() -> PathBuf {
    std::env::var("FILE_STORAGE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| Path::new("files").to_path_buf())
}

pub fn handle_download_file(
    state: &Data<AppState>,
    token: &str,
    file_path: &str,
) -> Result<(Vec<u8>, Option<String>), ApiError> {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT local_path, mime_type FROM files WHERE bot_id = ?1 AND file_path = ?2",
            params![bot.id, file_path],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((local_path, mime_type)) = row else {
        return Err(ApiError::not_found("file not found"));
    };

    let Some(path) = local_path else {
        return Err(ApiError::bad_request("file is not available for local download"));
    };

    let bytes = fs::read(path).map_err(ApiError::internal)?;
    Ok((bytes, mime_type))
}

pub fn handle_sim_bootstrap(state: &Data<AppState>, token: &str) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_default_user(&mut conn)?;

    Ok(json!({
        "bot": {
            "id": bot.id,
            "token": token,
            "username": bot.username,
            "first_name": bot.first_name
        },
        "users": [
            {
                "id": user.id,
                "username": user.username,
                "first_name": user.first_name
            }
        ]
    }))
}

pub fn handle_sim_send_user_message(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserMessageRequest,
) -> ApiResult {
    if body.text.trim().is_empty() {
        return Err(ApiError::bad_request("text is empty"));
    }

    let (parsed_text, parsed_entities) = parse_formatted_text(
        &body.text,
        body.parse_mode.as_deref(),
        None,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(
        &mut conn,
        body.user_id,
        body.first_name,
        body.username,
    )?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let chat_key = chat_id.to_string();
    ensure_chat(&mut conn, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
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
    };

    let chat = Chat {
        id: chat_id,
        r#type: "private".to_string(),
        title: None,
        username: user.username.clone(),
        first_name: Some(user.first_name.clone()),
        last_name: None,
        is_forum: None,
        is_direct_messages: None,
    };

    let message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
        "text": parsed_text,
    });

    let mut message_json = message_json;
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_message_value(&mut conn, &bot, reply_id)?;
        message_json["reply_to_message"] = reply_value;
    }
    if let Some(entities) = parsed_entities {
        message_json["entities"] = entities;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;

    let update_stub = Update {
        update_id: 0,
        message: Some(message.clone()),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    update_value["message"] = message_value.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(message_value)
}

pub fn handle_sim_send_user_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimSendUserMediaRequest = parse_request(&params)?;

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let (caption, caption_entities) = parse_optional_formatted_text(
        body.caption.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );

    let file = resolve_media_file(state, token, &body.media, &media_kind)?;

    let media_value = match media_kind.as_str() {
        "photo" => json!([
            {
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "width": 1280,
                "height": 720,
                "file_size": file.file_size,
            }
        ]),
        "video" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "width": 1280,
            "height": 720,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "audio" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "voice" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "document" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let chat_key = chat_id.to_string();
    ensure_chat(&mut conn, &chat_key)?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let from = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name.clone(),
        last_name: None,
        username: user.username.clone(),
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
    };

    let chat = Chat {
        id: chat_id,
        r#type: "private".to_string(),
        title: None,
        username: user.username.clone(),
        first_name: Some(user.first_name.clone()),
        last_name: None,
        is_forum: None,
        is_direct_messages: None,
    };

    let mut message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
    });

    message_json[media_kind.as_str()] = media_value;
    if let Some(c) = caption {
        message_json["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        message_json["caption_entities"] = entities;
    }
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_message_value(&mut conn, &bot, reply_id)?;
        message_json["reply_to_message"] = reply_value;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;

    let update_stub = Update {
        update_id: 0,
        message: Some(message.clone()),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    update_value["message"] = message_value.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(message_value)
}

pub fn handle_sim_edit_user_message_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimEditUserMediaRequest = parse_request(&params)?;

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let caption_text = body.caption.as_ref().and_then(value_to_optional_string);
    let (caption, caption_entities) = parse_optional_formatted_text(
        caption_text.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );

    let file = resolve_media_file(state, token, &body.media, &media_kind)?;

    let media_value = match media_kind.as_str() {
        "photo" => json!([
            {
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "width": 1280,
                "height": 720,
                "file_size": file.file_size,
            }
        ]),
        "video" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "width": 1280,
            "height": 720,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "audio" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "voice" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "duration": 0,
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        "document" => json!({
            "file_id": file.file_id,
            "file_unique_id": file.file_unique_id,
            "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
            "mime_type": file.mime_type,
            "file_size": file.file_size,
        }),
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = body.chat_id.to_string();

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, body.message_id)?;

    for key in ["photo", "video", "audio", "voice", "document", "animation", "video_note"] {
        edited_message.as_object_mut().map(|obj| obj.remove(key));
    }

    edited_message[media_kind.as_str()] = media_value;
    if let Some(c) = caption {
        edited_message["caption"] = Value::String(c.clone());
        conn.execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![c, bot.id, chat_key, body.message_id],
        )
        .map_err(ApiError::internal)?;
    }
    if let Some(entities) = caption_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("caption_entities"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(edited_message.clone()).ok(),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    update_value["edited_message"] = edited_message.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(edited_message)
}

pub fn handle_sim_create_bot(state: &Data<AppState>, body: SimCreateBotRequest) -> ApiResult {
    let conn = lock_db(state)?;

    let token = generate_telegram_token();
    let now = Utc::now().timestamp();
    let suffix = token_suffix(&token);

    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("LaraGram Bot {}", &suffix[..4]));

    let username = body
        .username
        .map(|v| sanitize_username(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("laragram_{}", suffix));

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

    let username = body
        .username
        .map(|v| sanitize_username(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or(bot.username);

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

pub fn handle_sim_upsert_user(state: &Data<AppState>, body: SimUpsertUserRequest) -> ApiResult {
    let conn = lock_db(state)?;

    let id = body
        .id
        .unwrap_or_else(|| (Utc::now().timestamp_millis() % 9_000_000) + 10_000);
    let first_name = body
        .first_name
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("User {}", id));
    let username = body
        .username
        .map(|v| sanitize_username(&v))
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| format!("user_{}", id));

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO users (id, username, first_name, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET username = excluded.username, first_name = excluded.first_name",
        params![id, username, first_name, now],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "id": id,
        "username": username,
        "first_name": first_name
    }))
}

pub fn handle_sim_clear_history(
    state: &Data<AppState>,
    token: &str,
    body: SimClearHistoryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let deleted = conn
        .execute(
            "DELETE FROM messages WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, body.chat_id.to_string()],
        )
        .map_err(ApiError::internal)?;

    Ok(json!({"deleted_count": deleted}))
}

fn handle_set_message_reaction(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMessageReactionRequest = parse_request(params)?;

    let reactions = normalize_reaction_values(request.reaction.as_ref().map(|r| {
        r.iter().map(|item| item.extra.clone()).collect::<Vec<Value>>()
    }))?;

    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chat_id_as_i64(&request.chat_id, &chat_key);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let actor = User {
        id: bot.id,
        is_bot: true,
        first_name: bot.first_name.clone(),
        last_name: None,
        username: Some(bot.username.clone()),
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
    };

    apply_message_reaction_change(
        state,
        &mut conn,
        &bot,
        token,
        &chat_key,
        chat_id,
        request.message_id,
        actor,
        reactions,
    )
}

pub fn handle_sim_set_user_reaction(
    state: &Data<AppState>,
    token: &str,
    body: SimSetUserReactionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let reactions = normalize_reaction_values(body.reaction)?;
    let chat_key = body.chat_id.to_string();

    let actor = User {
        id: user.id,
        is_bot: false,
        first_name: user.first_name,
        last_name: None,
        username: user.username,
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
    };

    apply_message_reaction_change(
        state,
        &mut conn,
        &bot,
        token,
        &chat_key,
        body.chat_id,
        body.message_id,
        actor,
        reactions,
    )
}

fn handle_get_updates(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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
            "SELECT update_json FROM updates
             WHERE bot_id = ?1 AND update_id >= ?2
             ORDER BY update_id ASC
             LIMIT ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, offset.max(1), limit], |row| row.get::<_, String>(0))
        .map_err(ApiError::internal)?;

    let mut updates = Vec::new();
    for row in rows {
        let raw = row.map_err(ApiError::internal)?;
        let parsed: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;
        updates.push(parsed);
    }

    Ok(Value::Array(updates))
}

fn handle_edit_message_text(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageTextRequest = parse_request(params)?;
    let Some(chat_id) = request.chat_id else {
        return Err(ApiError::bad_request("chat_id is required"));
    };
    let Some(message_id) = request.message_id else {
        return Err(ApiError::bad_request("message_id is required"));
    };

    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );

    if parsed_text.trim().is_empty() {
        return Err(ApiError::bad_request("text is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&chat_id)?;

    let updated = conn
        .execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![parsed_text, bot.id, chat_key, message_id],
        )
        .map_err(ApiError::internal)?;

    if updated == 0 {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    if let Some(entities) = parsed_entities {
        edited_message["entities"] = entities;
    }

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(edited_message.clone()).ok(),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    update_value["edited_message"] = edited_message.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(edited_message)
}

fn handle_edit_message_media(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageMediaRequest = parse_request(params)?;
    let Some(chat_id) = request.chat_id else {
        return Err(ApiError::bad_request("chat_id is required"));
    };
    let Some(message_id) = request.message_id else {
        return Err(ApiError::bad_request("message_id is required"));
    };

    let media_obj = request
        .media
        .extra
        .as_object()
        .ok_or_else(|| ApiError::bad_request("media must be an object"))?;

    let media_type = media_obj
        .get("type")
        .and_then(Value::as_str)
        .map(|v| v.to_ascii_lowercase())
        .ok_or_else(|| ApiError::bad_request("media.type is required"))?;

    let media_ref = media_obj
        .get("media")
        .ok_or_else(|| ApiError::bad_request("media.media is required"))?;

    let explicit_caption_entities = media_obj.get("caption_entities").cloned();
    let parse_mode = media_obj.get("parse_mode").and_then(Value::as_str);
    let (caption, caption_entities) = parse_optional_formatted_text(
        media_obj.get("caption").and_then(Value::as_str),
        parse_mode,
        explicit_caption_entities,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&chat_id)?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    let media_payload = match media_type.as_str() {
        "photo" => {
            let file = resolve_media_file(state, token, media_ref, "photo")?;
            json!([
                {
                    "file_id": file.file_id,
                    "file_unique_id": file.file_unique_id,
                    "width": 1280,
                    "height": 720,
                    "file_size": file.file_size,
                }
            ])
        }
        "video" => {
            let file = resolve_media_file(state, token, media_ref, "video")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "width": media_obj.get("width").and_then(Value::as_i64).unwrap_or(1280),
                "height": media_obj.get("height").and_then(Value::as_i64).unwrap_or(720),
                "duration": media_obj.get("duration").and_then(Value::as_i64).unwrap_or(0),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        "audio" => {
            let file = resolve_media_file(state, token, media_ref, "audio")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "duration": media_obj.get("duration").and_then(Value::as_i64).unwrap_or(0),
                "performer": media_obj.get("performer").and_then(Value::as_str),
                "title": media_obj.get("title").and_then(Value::as_str),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        "document" => {
            let file = resolve_media_file(state, token, media_ref, "document")?;
            json!({
                "file_id": file.file_id,
                "file_unique_id": file.file_unique_id,
                "file_name": file.file_path.split('/').last().unwrap_or("document.bin"),
                "mime_type": file.mime_type,
                "file_size": file.file_size,
            })
        }
        _ => {
            return Err(ApiError::bad_request(
                "editMessageMedia supports only photo, video, audio, and document",
            ));
        }
    };

    for key in ["photo", "video", "audio", "voice", "document", "animation", "video_note"] {
        edited_message.as_object_mut().map(|obj| obj.remove(key));
    }

    edited_message[media_type.as_str()] = media_payload;
    if let Some(c) = caption {
        edited_message["caption"] = Value::String(c.clone());
        conn.execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![c, bot.id, chat_key, message_id],
        )
        .map_err(ApiError::internal)?;
    }
    if let Some(entities) = caption_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("caption_entities"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(edited_message.clone()).ok(),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    update_value["edited_message"] = edited_message.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(edited_message)
}

fn handle_edit_message_caption(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageCaptionRequest = parse_request(params)?;
    let Some(chat_id) = request.chat_id else {
        return Err(ApiError::bad_request("chat_id is required"));
    };
    let Some(message_id) = request.message_id else {
        return Err(ApiError::bad_request("message_id is required"));
    };

    let explicit_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (parsed_caption, parsed_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_entities,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&chat_id)?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    if !message_has_media(&edited_message) {
        return Err(ApiError::bad_request(
            "message has no media caption to edit; use editMessageText",
        ));
    }

    let new_caption = parsed_caption.unwrap_or_default();
    conn.execute(
        "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![new_caption, bot.id, chat_key, message_id],
    )
    .map_err(ApiError::internal)?;

    edited_message["caption"] = Value::String(new_caption);
    if let Some(entities) = parsed_entities {
        edited_message["caption_entities"] = entities;
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("caption_entities"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: serde_json::from_value(edited_message.clone()).ok(),
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    update_value["edited_message"] = edited_message.clone();

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(&mut conn, bot.id, clean_update);

    Ok(edited_message)
}

fn handle_delete_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessageRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let deleted = conn
        .execute(
            "DELETE FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, request.message_id],
        )
        .map_err(ApiError::internal)?;

    Ok(Value::Bool(deleted > 0))
}

fn handle_delete_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessagesRequest = parse_request(params)?;

    if request.message_ids.is_empty() {
        return Ok(Value::Bool(true));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let placeholders = std::iter::repeat("?")
        .take(request.message_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "DELETE FROM messages WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders
    );

    let mut bind_values = Vec::with_capacity(2 + request.message_ids.len());
    bind_values.push(Value::from(bot.id));
    bind_values.push(Value::from(chat_key));
    for id in request.message_ids {
        bind_values.push(Value::from(id));
    }

    let mut stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
    let deleted = stmt
        .execute(rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)))
        .map_err(ApiError::internal)?;

    Ok(Value::Bool(deleted > 0))
}

fn handle_set_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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

fn handle_delete_webhook(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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

fn parse_request<T: DeserializeOwned>(params: &HashMap<String, Value>) -> Result<T, ApiError> {
    let object = Map::from_iter(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    serde_json::from_value(Value::Object(object)).map_err(|err| ApiError::bad_request(err.to_string()))
}

fn value_to_optional_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => Some(value.to_string()),
    }
}

fn parse_optional_formatted_text(
    text: Option<&str>,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (Option<String>, Option<Value>) {
    match text {
        Some(raw) if !raw.is_empty() => {
            let (plain, entities) = parse_formatted_text(raw, parse_mode, explicit_entities);
            (Some(plain), entities)
        }
        _ => (None, None),
    }
}

fn parse_formatted_text(
    text: &str,
    parse_mode: Option<&str>,
    explicit_entities: Option<Value>,
) -> (String, Option<Value>) {
    if let Some(entities) = explicit_entities {
        return (text.to_string(), Some(entities));
    }

    match parse_mode.map(|v| v.to_ascii_lowercase()) {
        Some(mode) if mode == "html" => {
            let (clean, entities) = parse_html_entities(text);
            (clean, entities_value(entities))
        }
        Some(mode) if mode == "markdown" || mode == "markdownv2" => {
            let (clean, entities) = parse_markdown_entities(text, mode == "markdownv2");
            (clean, entities_value(entities))
        }
        _ => (text.to_string(), None),
    }
}

fn entities_value(entities: Vec<Value>) -> Option<Value> {
    if entities.is_empty() {
        None
    } else {
        Some(Value::Array(entities))
    }
}

fn parse_html_entities(text: &str) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: Vec<(String, usize, Option<String>, bool)> = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;

    while i < bytes.len() {
        if bytes[i] == b'<' {
            if let Some(end) = text[i..].find('>') {
                let end_idx = i + end;
                let raw_tag = &text[i + 1..end_idx];
                let tag = raw_tag.trim();

                let is_close = tag.starts_with('/');
                let lower = tag.to_ascii_lowercase();

                if is_close {
                    let name = lower.trim_start_matches('/').trim();
                    let wanted = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(target) = wanted {
                        if let Some(pos) = stack.iter().rposition(|(kind, _, _, _)| kind == target) {
                            let (_, start, extra, is_expandable) = stack.remove(pos);
                            let len = utf16_len(&out).saturating_sub(start);
                            if len > 0 {
                                let mut entity = json!({
                                    "type": if target == "blockquote" && is_expandable {
                                        "expandable_blockquote"
                                    } else {
                                        target
                                    },
                                    "offset": start,
                                    "length": len,
                                });
                                if let Some(extra) = extra {
                                    if target == "text_link" {
                                        entity["url"] = Value::String(extra);
                                    } else if target == "custom_emoji" {
                                        entity["custom_emoji_id"] = Value::String(extra);
                                    } else if target == "date_time" {
                                        let unix = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("unix:"))
                                            .and_then(|v| v.parse::<i64>().ok())
                                            .unwrap_or(0);
                                        entity["unix_time"] = Value::from(unix);
                                        if let Some(fmt) = extra
                                            .split(';')
                                            .find_map(|seg| seg.strip_prefix("format:"))
                                        {
                                            entity["date_time_format"] = Value::String(fmt.to_string());
                                        }
                                    } else if target == "pre" {
                                        if let Some(lang) = extra.strip_prefix("lang:") {
                                            entity["language"] = Value::String(lang.to_string());
                                        }
                                    }
                                }
                                entities.push(entity);
                            }
                        }
                    }
                } else {
                    let mut parts = lower.split_whitespace();
                    let name = parts.next().unwrap_or("");
                    let kind = match name {
                        "b" | "strong" => Some("bold"),
                        "i" | "em" => Some("italic"),
                        "u" | "ins" => Some("underline"),
                        "s" | "strike" | "del" => Some("strikethrough"),
                        "span" if has_css_class(tag, "tg-spoiler") => Some("spoiler"),
                        "tg-spoiler" => Some("spoiler"),
                        "blockquote" => Some("blockquote"),
                        "tg-emoji" => Some("custom_emoji"),
                        "tg-time" => Some("date_time"),
                        "code" => Some("code"),
                        "pre" => Some("pre"),
                        "a" => Some("text_link"),
                        _ => None,
                    };

                    if let Some(entity_type) = kind {
                        if entity_type == "code" {
                            if let Some(language) = extract_code_language(tag) {
                                if let Some((_, _, pre_extra, _)) = stack
                                    .iter_mut()
                                    .rev()
                                    .find(|(kind, _, _, _)| kind == "pre")
                                {
                                    *pre_extra = Some(format!("lang:{}", language));
                                    i = end_idx + 1;
                                    continue;
                                }
                            }
                        }

                        let start = utf16_len(&out);
                        let expandable = entity_type == "blockquote" && lower.contains("expandable");
                        let url = if entity_type == "text_link" { extract_href(tag) } else { None };
                        let extra = if entity_type == "custom_emoji" {
                            extract_attr(tag, "emoji-id").map(|v| format!("custom_emoji_id:{}", v))
                        } else if entity_type == "date_time" {
                            extract_attr(tag, "unix").map(|unix| {
                                let mut payload = format!("unix:{}", unix);
                                if let Some(fmt) = extract_attr(tag, "format") {
                                    payload.push_str(&format!(";format:{}", fmt));
                                }
                                payload
                            })
                        } else {
                            None
                        };
                        if let Some(payload) = extra {
                            let stored = if let Some(v) = payload.strip_prefix("custom_emoji_id:") {
                                v.to_string()
                            } else {
                                payload
                            };
                            stack.push((entity_type.to_string(), start, Some(stored), expandable));
                        } else {
                            stack.push((entity_type.to_string(), start, url, expandable));
                        }
                    }
                }

                i = end_idx + 1;
                continue;
            }
        }

        if bytes[i] == b'&' {
            if let Some(end) = text[i..].find(';') {
                let end_idx = i + end;
                let entity = &text[i..=end_idx];
                if let Some(decoded) = decode_html_entity(entity) {
                    out.push_str(decoded);
                    i = end_idx + 1;
                    continue;
                }
            }
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

fn parse_markdown_entities(text: &str, markdown_v2: bool) -> (String, Vec<Value>) {
    let mut out = String::new();
    let mut entities = Vec::new();
    let mut stack: HashMap<&str, Vec<usize>> = HashMap::new();
    let mut i = 0;
    let mut line_start = true;

    while i < text.len() {
        if text[i..].starts_with("```") {
            if let Some((advance, code_text, language)) = parse_markdown_pre_block(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&code_text);
                let len = utf16_len(&code_text);
                if len > 0 {
                    let mut entity = json!({
                        "type": "pre",
                        "offset": start,
                        "length": len,
                    });
                    if let Some(lang) = language {
                        entity["language"] = Value::String(lang);
                    }
                    entities.push(entity);
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with("![") {
            if let Some((advance, label, url)) = parse_markdown_media_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&label);
                let len = utf16_len(&label);
                if len > 0 {
                    if let Some(id) = extract_query_param(&url, "id") {
                        if url.starts_with("tg://emoji") {
                            entities.push(json!({
                                "type": "custom_emoji",
                                "offset": start,
                                "length": len,
                                "custom_emoji_id": id,
                            }));
                        } else if url.starts_with("tg://time") {
                            let mut entity = json!({
                                "type": "date_time",
                                "offset": start,
                                "length": len,
                                "unix_time": extract_query_param(&url, "unix")
                                    .and_then(|v| v.parse::<i64>().ok())
                                    .unwrap_or(0),
                            });
                            if let Some(fmt) = extract_query_param(&url, "format") {
                                entity["date_time_format"] = Value::String(fmt);
                            }
                            entities.push(entity);
                        }
                    } else if url.starts_with("tg://time") {
                        let mut entity = json!({
                            "type": "date_time",
                            "offset": start,
                            "length": len,
                            "unix_time": extract_query_param(&url, "unix")
                                .and_then(|v| v.parse::<i64>().ok())
                                .unwrap_or(0),
                        });
                        if let Some(fmt) = extract_query_param(&url, "format") {
                            entity["date_time_format"] = Value::String(fmt);
                        }
                        entities.push(entity);
                    }
                }
                i += advance;
                continue;
            }
        }

        if markdown_v2 && text[i..].starts_with('\\') {
            let next_start = i + 1;
            if next_start < text.len() {
                if let Some(ch) = text[next_start..].chars().next() {
                    out.push(ch);
                    line_start = ch == '\n';
                    i = next_start + ch.len_utf8();
                    continue;
                }
            }
            i += 1;
            continue;
        }

        if markdown_v2 && line_start && (text[i..].starts_with('>') || text[i..].starts_with("**>")) {
            let mut start_shift = 1;
            let mut forced_expandable = false;
            if text[i..].starts_with("**>") {
                start_shift = 3;
                forced_expandable = true;
            }
            let line_end = text[i..].find('\n').map(|v| i + v).unwrap_or(text.len());
            let raw_line = &text[i + start_shift..line_end];
            let trimmed_line = raw_line.trim_start();
            let is_expandable = forced_expandable || trimmed_line.trim_end().ends_with("||");
            let content = if is_expandable {
                trimmed_line.trim_end().trim_end_matches("||").trim_end()
            } else {
                trimmed_line
            };

            let start = utf16_len(&out);
            out.push_str(content);
            let len = utf16_len(content);
            if len > 0 {
                entities.push(json!({
                    "type": if is_expandable { "expandable_blockquote" } else { "blockquote" },
                    "offset": start,
                    "length": len,
                }));
            }

            if line_end < text.len() {
                out.push('\n');
                i = line_end + 1;
                line_start = true;
            } else {
                i = line_end;
                line_start = false;
            }
            continue;
        }

        if text[i..].starts_with('[') {
            if let Some((advance, link_text, link_url)) = parse_markdown_link(&text[i..]) {
                let start = utf16_len(&out);
                out.push_str(&link_text);
                let len = utf16_len(&link_text);
                if len > 0 {
                    entities.push(json!({
                        "type": "text_link",
                        "offset": start,
                        "length": len,
                        "url": link_url,
                    }));
                }
                i += advance;
                continue;
            }
        }

        let mut matched = false;
        for (token, entity_type) in markdown_tokens(markdown_v2) {
            if !text[i..].starts_with(token) {
                continue;
            }

            matched = true;
            let start = utf16_len(&out);
            let entry = stack.entry(token).or_default();
            if let Some(open_start) = entry.pop() {
                let len = start.saturating_sub(open_start);
                if len > 0 {
                    entities.push(json!({
                        "type": entity_type,
                        "offset": open_start,
                        "length": len,
                    }));
                }
            } else {
                entry.push(start);
            }

            i += token.len();
            break;
        }

        if matched {
            continue;
        }

        if let Some(ch) = text[i..].chars().next() {
            out.push(ch);
            line_start = ch == '\n';
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    entities.sort_by_key(|entity| {
        entity
            .get("offset")
            .and_then(Value::as_u64)
            .unwrap_or_default()
    });

    (out, entities)
}

fn parse_markdown_pre_block(input: &str) -> Option<(usize, String, Option<String>)> {
    if !input.starts_with("```") {
        return None;
    }

    let rest = &input[3..];
    let mut language = None;
    let mut content_start = 3;

    if let Some(line_end) = rest.find('\n') {
        let header = rest[..line_end].trim();
        if !header.is_empty() {
            language = Some(header.to_string());
        }
        content_start = 3 + line_end + 1;
    }

    let body = &input[content_start..];
    let close_rel = body.find("```")?;
    let close_abs = content_start + close_rel;
    let content = &input[content_start..close_abs];
    let advance = close_abs + 3;

    Some((advance, content.to_string(), language))
}

fn markdown_tokens(markdown_v2: bool) -> Vec<(&'static str, &'static str)> {
    if markdown_v2 {
        vec![
            ("||", "spoiler"),
            ("__", "underline"),
            ("*", "bold"),
            ("_", "italic"),
            ("~", "strikethrough"),
            ("`", "code"),
        ]
    } else {
        vec![("*", "bold"), ("_", "italic"), ("`", "code")]
    }
}

fn parse_markdown_link(input: &str) -> Option<(usize, String, String)> {
    let close_text = input.find(']')?;
    let rest = &input[close_text + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let text = &input[1..close_text];
    let url = &rest[1..close_url];
    let advance = close_text + 1 + close_url + 1;
    Some((advance, text.to_string(), url.to_string()))
}

fn parse_markdown_media_link(input: &str) -> Option<(usize, String, String)> {
    if !input.starts_with("![") {
        return None;
    }
    let close_label = input.find(']')?;
    let rest = &input[close_label + 1..];
    if !rest.starts_with('(') {
        return None;
    }
    let close_url = rest.find(')')?;
    let label = &input[2..close_label];
    let url = &rest[1..close_url];
    let advance = close_label + 1 + close_url + 1;
    Some((advance, label.to_string(), url.to_string()))
}

fn utf16_len(text: &str) -> usize {
    text.encode_utf16().count()
}

fn extract_href(tag: &str) -> Option<String> {
    extract_attr(tag, "href")
}

fn extract_attr(tag: &str, attr: &str) -> Option<String> {
    let lower = tag.to_ascii_lowercase();
    let needle = format!("{}=", attr.to_ascii_lowercase());
    let attr_pos = lower.find(&needle)?;
    let raw = &tag[attr_pos + needle.len()..].trim_start();
    if let Some(rest) = raw.strip_prefix('"') {
        let end = rest.find('"')?;
        return Some(rest[..end].to_string());
    }
    if let Some(rest) = raw.strip_prefix('\'') {
        let end = rest.find('\'')?;
        return Some(rest[..end].to_string());
    }

    let end = raw.find(char::is_whitespace).unwrap_or(raw.len());
    Some(raw[..end].to_string())
}

fn has_css_class(tag: &str, class_name: &str) -> bool {
    extract_attr(tag, "class")
        .map(|v| {
            v.split_whitespace()
                .any(|part| part.eq_ignore_ascii_case(class_name))
        })
        .unwrap_or(false)
}

fn extract_code_language(tag: &str) -> Option<String> {
    let class_attr = extract_attr(tag, "class")?;
    class_attr
        .split_whitespace()
        .find_map(|part| part.strip_prefix("language-"))
        .map(|v| v.to_string())
}

fn extract_query_param(url: &str, key: &str) -> Option<String> {
    let query = url.split('?').nth(1)?;
    for part in query.split('&') {
        let mut seg = part.splitn(2, '=');
        let k = seg.next()?.trim();
        let v = seg.next().unwrap_or("").trim();
        if k.eq_ignore_ascii_case(key) {
            return Some(v.to_string());
        }
    }
    None
}

fn decode_html_entity(entity: &str) -> Option<&'static str> {
    match entity {
        "&lt;" => Some("<"),
        "&gt;" => Some(">"),
        "&amp;" => Some("&"),
        "&quot;" => Some("\""),
        "&#39;" => Some("'"),
        "&apos;" => Some("'"),
        _ => None,
    }
}

fn normalize_reaction_values(raw: Option<Vec<Value>>) -> Result<Vec<Value>, ApiError> {
    let Some(items) = raw else {
        return Ok(Vec::new());
    };

    let mut normalized = Vec::<Value>::new();
    for item in items {
        let obj = item
            .as_object()
            .ok_or_else(|| ApiError::bad_request("reaction item must be an object"))?;

        let reaction_type = obj
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or("emoji")
            .to_ascii_lowercase();

        if reaction_type != "emoji" {
            return Err(ApiError::bad_request(
                "only emoji reactions are supported in simulator",
            ));
        }

        let emoji = obj
            .get("emoji")
            .and_then(Value::as_str)
            .ok_or_else(|| ApiError::bad_request("reaction emoji is required"))?
            .trim()
            .to_string();

        if emoji.is_empty() {
            return Err(ApiError::bad_request("reaction emoji is empty"));
        }

        if !is_allowed_telegram_reaction_emoji(&emoji) {
            return Err(ApiError::bad_request("reaction emoji is not allowed"));
        }

        let value = json!({
            "type": "emoji",
            "emoji": emoji,
        });

        if !normalized.iter().any(|existing| existing == &value) {
            normalized.push(value);
        }
    }

    Ok(normalized)
}

fn is_allowed_telegram_reaction_emoji(emoji: &str) -> bool {
    const ALLOWED: &[&str] = &[
        "👍", "👎", "❤", "🔥", "🥰", "👏", "😁", "🤔", "🤯", "😱", "🤬", "😢",
        "🎉", "🤩", "🤮", "💩", "🙏", "👌", "🕊", "🤡", "🥱", "🥴", "😍", "🐳",
        "❤‍🔥", "🌚", "🌭", "💯", "🤣", "⚡", "🍌", "🏆", "💔", "🤨", "😐", "🍓",
        "🍾", "💋", "🖕", "😈", "😴", "😭", "🤓", "👻", "👨‍💻", "👀", "🎃", "🙈",
        "😇", "😨", "🤝", "✍", "🤗", "🫡", "🎅", "🎄", "☃", "💅", "🤪", "🗿",
        "🆒", "💘", "🙉", "🦄", "😘", "💊", "🙊", "😎", "👾", "🤷‍♂", "🤷", "😡",
    ];

    ALLOWED.contains(&emoji)
}

fn apply_message_reaction_change(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    token: &str,
    chat_key: &str,
    chat_id: i64,
    message_id: i64,
    actor: User,
    new_reaction: Vec<Value>,
) -> ApiResult {
    let message_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if message_exists.is_none() {
        return Err(ApiError::not_found("message to react was not found"));
    }

    let now = Utc::now().timestamp();
    let actor_is_bot = if actor.is_bot { 1 } else { 0 };

    let old_reaction_json: Option<String> = conn
        .query_row(
            "SELECT reactions_json FROM message_reactions
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND actor_user_id = ?4 AND actor_is_bot = ?5",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let old_reaction: Vec<Value> = old_reaction_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<Value>>(raw).ok())
        .unwrap_or_default();

    if new_reaction.is_empty() {
        conn.execute(
            "DELETE FROM message_reactions
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3 AND actor_user_id = ?4 AND actor_is_bot = ?5",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot],
        )
        .map_err(ApiError::internal)?;
    } else {
        let serialized = serde_json::to_string(&new_reaction).map_err(ApiError::internal)?;
        conn.execute(
            "INSERT INTO message_reactions (bot_id, chat_key, message_id, actor_user_id, actor_is_bot, reactions_json, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(bot_id, chat_key, message_id, actor_user_id, actor_is_bot)
             DO UPDATE SET reactions_json = excluded.reactions_json, updated_at = excluded.updated_at",
            params![bot.id, chat_key, message_id, actor.id, actor_is_bot, serialized, now],
        )
        .map_err(ApiError::internal)?;
    }

    let count_payload = {
        let mut counts: HashMap<String, (Value, i64)> = HashMap::new();
        let mut stmt = conn
            .prepare(
                "SELECT reactions_json FROM message_reactions
                 WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            )
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![bot.id, chat_key, message_id], |row| row.get::<_, String>(0))
            .map_err(ApiError::internal)?;

        for row in rows {
            let raw = row.map_err(ApiError::internal)?;
            if let Ok(reactions) = serde_json::from_str::<Vec<Value>>(&raw) {
                for reaction in reactions {
                    let key = serde_json::to_string(&reaction).map_err(ApiError::internal)?;
                    let entry = counts.entry(key).or_insert((reaction, 0));
                    entry.1 += 1;
                }
            }
        }

        let mut payload = Vec::<Value>::new();
        for (_, (reaction_type, total_count)) in counts {
            payload.push(json!({
                "type": reaction_type,
                "total_count": total_count,
            }));
        }
        payload
    };

    let chat = json!({
        "id": chat_id,
        "type": "private",
    });

    let reaction_update = json!({
        "update_id": 0,
        "message_reaction": {
            "chat": chat.clone(),
            "message_id": message_id,
            "user": actor,
            "date": now,
            "old_reaction": old_reaction,
            "new_reaction": new_reaction,
        }
    });

    persist_and_dispatch_update(state, conn, token, bot.id, reaction_update)?;

    let reaction_count_update = json!({
        "update_id": 0,
        "message_reaction_count": {
            "chat": chat,
            "message_id": message_id,
            "date": now,
            "reactions": count_payload,
        }
    });

    persist_and_dispatch_update(state, conn, token, bot.id, reaction_count_update)?;

    Ok(json!(true))
}

fn persist_and_dispatch_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    mut update_value: Value,
) -> Result<(), ApiError> {
    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot_id, update_value.to_string()],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    update_value["update_id"] = json!(update_id);

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    dispatch_webhook_if_configured(conn, bot_id, clean_update);
    Ok(())
}

fn load_message_value(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    message_id: i64,
) -> Result<Value, ApiError> {
    let row: Option<(String, i64, String, i64)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, text, date FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, text, date)) = row else {
        return Err(ApiError::not_found("message not found"));
    };

    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(&chat_key));
    let (is_bot, first_name, username) = if from_user_id == bot.id {
        (true, bot.first_name.clone(), Some(bot.username.clone()))
    } else {
        let user: Option<(String, Option<String>)> = conn
            .query_row(
                "SELECT first_name, username FROM users WHERE id = ?1",
                params![from_user_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;
        let (first, uname) = user.unwrap_or_else(|| ("User".to_string(), None));
        (false, first, uname)
    };

    let mut message = find_message_snapshot(conn, bot.id, message_id).unwrap_or_else(|| {
        json!({
            "message_id": message_id,
            "date": date,
            "chat": {
                "id": chat_id,
                "type": "private"
            },
            "from": {
                "id": from_user_id,
                "is_bot": is_bot,
                "first_name": first_name,
                "username": username
            }
        })
    });

    message["message_id"] = Value::from(message_id);
    message["date"] = Value::from(date);
    message["edit_date"] = Value::from(Utc::now().timestamp());
    message["chat"] = json!({
        "id": chat_id,
        "type": "private"
    });
    message["from"] = json!({
        "id": from_user_id,
        "is_bot": is_bot,
        "first_name": first_name,
        "username": username
    });

    if message_has_media(&message) {
        message.as_object_mut().map(|obj| obj.remove("text"));
        message["caption"] = Value::String(text);
    } else {
        message.as_object_mut().map(|obj| obj.remove("caption"));
        message["text"] = Value::String(text);
    }

    Ok(message)
}

fn find_message_snapshot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    message_id: i64,
) -> Option<Value> {
    let mut stmt = conn
        .prepare(
            "SELECT update_json FROM updates WHERE bot_id = ?1 ORDER BY update_id DESC LIMIT 5000",
        )
        .ok()?;

    let rows = stmt
        .query_map(params![bot_id], |row| row.get::<_, String>(0))
        .ok()?;

    for row in rows {
        let raw = row.ok()?;
        let update_value: Value = serde_json::from_str(&raw).ok()?;

        if let Some(msg) = update_value
            .get("edited_message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("message")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }
    }

    None
}

fn message_has_media(message: &Value) -> bool {
    ["photo", "video", "audio", "voice", "document", "animation", "video_note"]
        .iter()
        .any(|key| message.get(*key).is_some())
}

fn sql_value_to_rusqlite(v: &Value) -> rusqlite::types::Value {
    match v {
        Value::Null => rusqlite::types::Value::Null,
        Value::Bool(b) => rusqlite::types::Value::Integer(if *b { 1 } else { 0 }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                rusqlite::types::Value::Integer(i)
            } else if let Some(f) = n.as_f64() {
                rusqlite::types::Value::Real(f)
            } else {
                rusqlite::types::Value::Null
            }
        }
        Value::String(s) => rusqlite::types::Value::Text(s.clone()),
        _ => rusqlite::types::Value::Text(v.to_string()),
    }
}

fn value_to_chat_key(v: &Value) -> Result<String, ApiError> {
    match v {
        Value::String(s) if !s.trim().is_empty() => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        _ => Err(ApiError::bad_request("chat_id is empty or invalid")),
    }
}

fn chat_id_as_i64(chat_id: &Value, chat_key: &str) -> i64 {
    match chat_id {
        Value::Number(n) => n.as_i64().unwrap_or_else(|| fallback_chat_id(chat_key)),
        Value::String(s) => s
            .parse::<i64>()
            .unwrap_or_else(|_| fallback_chat_id(s)),
        _ => fallback_chat_id(chat_key),
    }
}

fn fallback_chat_id(input: &str) -> i64 {
    let mut acc: i64 = 0;
    for b in input.as_bytes() {
        acc = acc.wrapping_mul(31).wrapping_add(*b as i64);
    }
    -acc.abs().max(1)
}

#[derive(Debug)]
struct SimUserRecord {
    id: i64,
    first_name: String,
    username: Option<String>,
}

fn ensure_default_user(conn: &mut rusqlite::Connection) -> Result<SimUserRecord, ApiError> {
    ensure_user(conn, Some(10001), Some("Test User".to_string()), Some("test_user".to_string()))
}

fn ensure_user(
    conn: &mut rusqlite::Connection,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
) -> Result<SimUserRecord, ApiError> {
    let id = user_id.unwrap_or(10001);
    let effective_first_name = first_name.unwrap_or_else(|| "Test User".to_string());
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO users (id, username, first_name, created_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(id) DO UPDATE SET
            username = COALESCE(excluded.username, users.username),
            first_name = excluded.first_name",
        params![id, username, effective_first_name, now],
    )
    .map_err(ApiError::internal)?;

    conn.query_row(
        "SELECT id, first_name, username FROM users WHERE id = ?1",
        params![id],
        |row| {
            Ok(SimUserRecord {
                id: row.get(0)?,
                first_name: row.get(1)?,
                username: row.get(2)?,
            })
        },
    )
    .map_err(ApiError::internal)
}

fn dispatch_webhook_if_configured(conn: &mut rusqlite::Connection, bot_id: i64, update: Value) {
    let webhook: Result<Option<(String, String)>, ApiError> = conn
        .query_row(
            "SELECT url, secret_token FROM webhooks WHERE bot_id = ?1",
            params![bot_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal);

    let Ok(Some((url, secret_token))) = webhook else {
        return;
    };

    let payload = strip_nulls(update);
    std::thread::spawn(move || {
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
        {
            Ok(client) => client,
            Err(_) => return,
        };

        let mut request = client.post(url).json(&payload);
        if !secret_token.is_empty() {
            request = request.header("X-Telegram-Bot-Api-Secret-Token", secret_token);
        }

        let _ = request.send();
    });
}

fn token_suffix(token: &str) -> String {
    token
        .chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .rev()
        .take(8)
        .collect::<String>()
        .chars()
        .rev()
        .collect()
}

fn generate_telegram_token() -> String {
    let left = ((Utc::now().timestamp_millis().abs() as u64) % 900_000_000) + 100_000_000;
    let right = format!("{}{}", uuid::Uuid::new_v4().simple(), uuid::Uuid::new_v4().simple());
    let compact = right.chars().take(35).collect::<String>();
    format!("{}:{}", left, compact)
}

fn sanitize_username(input: &str) -> String {
    input
        .chars()
        .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
        .collect::<String>()
        .to_lowercase()
}

