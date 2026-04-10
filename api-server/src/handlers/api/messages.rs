use super::*;
use crate::generated::methods::{
    CopyMessageRequest, CopyMessagesRequest,
    DeleteMessageRequest, DeleteMessagesRequest,
    EditMessageCaptionRequest, EditMessageChecklistRequest,
    EditMessageLiveLocationRequest,
    EditMessageMediaRequest, EditMessageReplyMarkupRequest, EditMessageTextRequest,
    ForwardMessageRequest, ForwardMessagesRequest,
    SendAnimationRequest, SendAudioRequest, SendContactRequest, SendDiceRequest, SendDocumentRequest,
    SendChatActionRequest, SendChecklistRequest, SendGameRequest, SendInvoiceRequest,
    SendLocationRequest, SendMediaGroupRequest,
    SendPaidMediaRequest, SendGiftRequest, SendMessageDraftRequest, SendMessageRequest, SendPhotoRequest,
    SendPollRequest, SendStickerRequest,
    SendVenueRequest, SendVideoNoteRequest, SendVideoRequest, SendVoiceRequest,
    SetMessageReactionRequest,
    StopMessageLiveLocationRequest, StopPollRequest,
};

use crate::handlers::utils::updates::{current_request_actor_user_id, value_to_chat_key};

use crate::handlers::client::chats::ChatSendKind;

use crate::handlers::client::{channels, chats, groups, messages, users, webhook};

pub fn handle_copy_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CopyMessageRequest = parse_request(params)?;
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|entities| serde_json::to_value(entities).ok());
    let (caption_override, caption_entities_override) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let copied_message = messages::copy_message_internal(
        state,
        &mut conn,
        token,
        &bot,
        &request.from_chat_id,
        &request.chat_id,
        request.message_id,
        request.message_thread_id,
        caption_override,
        caption_entities_override,
        false,
        request.show_caption_above_media,
        request.reply_markup,
        request.protect_content,
        None,
        None,
        None,
        None,
        false,
    )?;

    let message_id = copied_message
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("copyMessage result is missing message_id"))?;

    Ok(json!({ "message_id": message_id }))
}

pub fn handle_copy_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CopyMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut copied = Vec::new();

    for message_id in request.message_ids {
        match messages::copy_message_internal(
            state,
            &mut conn,
            token,
            &bot,
            &request.from_chat_id,
            &request.chat_id,
            message_id,
            request.message_thread_id,
            None,
            None,
            request.remove_caption.unwrap_or(false),
            None,
            None,
            request.protect_content,
            None,
            None,
            None,
            None,
            false,
        ) {
            Ok(copied_message) => {
                if let Some(id) = copied_message.get("message_id").and_then(Value::as_i64) {
                    copied.push(json!({ "message_id": id }));
                }
            }
            Err(error) => {
                if error.code >= 500 {
                    return Err(error);
                }
            }
        }
    }

    Ok(Value::Array(copied))
}

pub fn handle_delete_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessageRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat_id) = resolve_chat_key_and_id(&mut conn, bot.id, &request.chat_id)?;
    let direct_messages_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .filter(|chat| channels::is_direct_messages_chat(chat));

    match ensure_message_can_be_deleted_by_actor(&mut conn, bot.id, &chat_key, request.message_id) {
        Ok(()) => {}
        Err(err) if err.code == 404 => return Ok(Value::Bool(false)),
        Err(err) => return Err(err),
    }

    if let Some(chat) = direct_messages_chat.as_ref() {
        emit_suggested_post_refunded_updates_before_delete(
            state,
            &mut conn,
            token,
            &bot,
            chat,
            &[request.message_id],
        )?;
    }

    let deleted = delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        chat_id,
        &[request.message_id],
    )?;

    Ok(Value::Bool(deleted > 0))
}

pub fn handle_delete_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeleteMessagesRequest = parse_request(params)?;
    let message_ids = request.message_ids.clone();

    if message_ids.is_empty() {
        return Ok(Value::Bool(true));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat_id) = resolve_chat_key_and_id(&mut conn, bot.id, &request.chat_id)?;
    let direct_messages_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?
        .filter(|chat| channels::is_direct_messages_chat(chat));

    let placeholders = std::iter::repeat("?")
        .take(message_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let existing_sql = format!(
        "SELECT message_id
         FROM messages
         WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders,
    );
    let mut existing_bind_values = Vec::with_capacity(2 + message_ids.len());
    existing_bind_values.push(Value::from(bot.id));
    existing_bind_values.push(Value::from(chat_key.clone()));
    for id in &message_ids {
        existing_bind_values.push(Value::from(*id));
    }

    let mut existing_stmt = conn.prepare(&existing_sql).map_err(ApiError::internal)?;
    let existing_rows = existing_stmt
        .query_map(
            rusqlite::params_from_iter(existing_bind_values.iter().map(sql_value_to_rusqlite)),
            |row| row.get::<_, i64>(0),
        )
        .map_err(ApiError::internal)?;

    let existing_ids: Vec<i64> = existing_rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(existing_stmt);

    for message_id in &existing_ids {
        ensure_message_can_be_deleted_by_actor(&mut conn, bot.id, &chat_key, *message_id)?;
    }

    if let Some(chat) = direct_messages_chat.as_ref() {
        emit_suggested_post_refunded_updates_before_delete(
            state,
            &mut conn,
            token,
            &bot,
            chat,
            &existing_ids,
        )?;
    }

    let deleted = delete_messages_with_dependencies(
        &mut conn,
        bot.id,
        &chat_key,
        chat_id,
        &existing_ids,
    )?;

    Ok(Value::Bool(deleted > 0))
}

pub fn handle_edit_message_caption(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageCaptionRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let explicit_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_caption, parsed_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        if let Some(caption_text) = parsed_caption.as_deref() {
            merge_auto_message_entities(caption_text, parsed_entities)
        } else {
            None
        }
    } else {
        parsed_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageCaption",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    if !messages::message_has_media(&edited_message) {
        return Err(ApiError::bad_request(
            "message has no media caption to edit; use editMessageText",
        ));
    }
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

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
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_edit_message_checklist(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageChecklistRequest = parse_request_with_legacy_checklist(params)?;
    if request.business_connection_id.trim().is_empty() {
        return Err(ApiError::bad_request("business_connection_id is empty"));
    }

    let checklist = normalize_input_checklist(&request.checklist)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let connection = load_business_connection_or_404(
        &mut conn,
        bot.id,
        request.business_connection_id.trim(),
    )?;
    if !connection.is_enabled {
        return Err(ApiError::bad_request("business connection is disabled"));
    }
    if connection.user_chat_id != request.chat_id {
        return Err(ApiError::bad_request(
            "business connection does not match target private chat",
        ));
    }

    let business_connection = build_business_connection(&mut conn, bot.id, &connection)?;
    ensure_business_right(
        &business_connection,
        |rights| rights.can_reply,
        "not enough rights to edit business checklists",
    )?;

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, _) = resolve_chat_key_and_id(&mut conn, bot.id, &chat_id_value)?;

    let exists_in_chat: Option<i64> = conn
        .query_row(
            "SELECT 1 FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &chat_key, request.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists_in_chat.is_none() {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    ensure_message_can_be_edited_by_bot(&mut conn, bot.id, &chat_key, request.message_id, false)?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, request.message_id)?;
    if edited_message.get("checklist").is_none() {
        return Err(ApiError::bad_request("message has no checklist to edit"));
    }

    let message_connection_id = edited_message
        .get("business_connection_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if message_connection_id.as_deref() != Some(connection.connection_id.as_str()) {
        return Err(ApiError::bad_request(
            "business connection does not match message",
        ));
    }

    conn.execute(
        "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![checklist.title.clone(), bot.id, &chat_key, request.message_id],
    )
    .map_err(ApiError::internal)?;

    edited_message["checklist"] = serde_json::to_value(&checklist).map_err(ApiError::internal)?;
    edited_message["business_connection_id"] = Value::String(connection.connection_id);
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;
    Ok(edited_message)
}

pub fn handle_edit_message_live_location(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageLiveLocationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageLiveLocation",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;

    if edited_message.get("location").is_none() && edited_message.get("venue").is_none() {
        return Err(ApiError::bad_request("message has no live location to edit"));
    }

    let updated_location = Location {
        latitude: request.latitude,
        longitude: request.longitude,
        horizontal_accuracy: request.horizontal_accuracy,
        live_period: request.live_period,
        heading: request.heading,
        proximity_alert_radius: request.proximity_alert_radius,
    };

    if edited_message.get("venue").is_some() {
        if let Some(venue_obj) = edited_message.get_mut("venue").and_then(Value::as_object_mut) {
            venue_obj.insert("location".to_string(), serde_json::to_value(updated_location).map_err(ApiError::internal)?);
        }
    } else {
        edited_message["location"] = serde_json::to_value(updated_location).map_err(ApiError::internal)?;
    }

    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_edit_message_media(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageMediaRequest = parse_request(params)?;

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
    let should_auto_detect_entities = explicit_caption_entities.is_none();
    let parse_mode = media_obj.get("parse_mode").and_then(Value::as_str);
    let sim_parse_mode = normalize_sim_parse_mode(parse_mode);
    let (caption, caption_entities) = parse_optional_formatted_text(
        media_obj.get("caption").and_then(Value::as_str),
        parse_mode,
        explicit_caption_entities,
    );
    let caption_entities = if should_auto_detect_entities {
        if let Some(caption_text) = caption.as_deref() {
            merge_auto_message_entities(caption_text, caption_entities)
        } else {
            None
        }
    } else {
        caption_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageMedia",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;
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
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
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
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_edit_message_reply_markup(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageReplyMarkupRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageReplyMarkup",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_edit_message_text(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageTextRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        merge_auto_message_entities(&parsed_text, parsed_entities)
    } else {
        parsed_entities
    };

    if parsed_text.trim().is_empty() {
        return Err(ApiError::bad_request("text is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageText",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let updated = conn
        .execute(
            "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
            params![parsed_text, bot.id, chat_key, message_id],
        )
        .map_err(ApiError::internal)?;

    if updated == 0 {
        return Err(ApiError::not_found("message to edit was not found"));
    }

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    if let Some(entities) = parsed_entities {
        edited_message["entities"] = entities;
    } else {
        edited_message.as_object_mut().map(|obj| obj.remove("entities"));
    }
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_forward_message(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ForwardMessageRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    forward_message_internal(
        state,
        &mut conn,
        token,
        &bot,
        &request.from_chat_id,
        &request.chat_id,
        request.message_id,
        request.message_thread_id,
        request.protect_content,
    )
}

pub fn handle_forward_messages(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ForwardMessagesRequest = parse_request(params)?;
    if request.message_ids.is_empty() {
        return Err(ApiError::bad_request("message_ids must not be empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut forwarded = Vec::new();

    for message_id in request.message_ids {
        match forward_message_internal(
            state,
            &mut conn,
            token,
            &bot,
            &request.from_chat_id,
            &request.chat_id,
            message_id,
            request.message_thread_id,
            request.protect_content,
        ) {
            Ok(forwarded_message) => {
                if let Some(id) = forwarded_message.get("message_id").and_then(Value::as_i64) {
                    forwarded.push(json!({ "message_id": id }));
                }
            }
            Err(error) => {
                if error.code >= 500 {
                    return Err(error);
                }
            }
        }
    }

    Ok(Value::Array(forwarded))
}

pub fn handle_send_animation(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendAnimationRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let animation_input = parse_input_file_value(&request.animation, "animation")?;
    let file = resolve_media_file(state, token, &animation_input, "animation")?;

    let animation = serde_json::to_value(Animation {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        width: request.width.unwrap_or(512),
        height: request.height.unwrap_or(512),
        duration: request.duration.unwrap_or(0),
        thumbnail: None,
        file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
        mime_type: file.mime_type,
        file_size: file.file_size,
    })
    .map_err(ApiError::internal)?;

    send_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        "animation",
        animation,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_audio(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendAudioRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
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
        request.reply_markup,
        "audio",
        audio,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_contact(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendContactRequest = parse_request(params)?;
    if request.phone_number.trim().is_empty() {
        return Err(ApiError::bad_request("phone_number is empty"));
    }
    if request.first_name.trim().is_empty() {
        return Err(ApiError::bad_request("first_name is empty"));
    }

    let contact = Contact {
        phone_number: request.phone_number,
        first_name: request.first_name,
        last_name: request.last_name,
        user_id: None,
        vcard: request.vcard,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "contact",
        serde_json::to_value(contact).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

pub fn handle_send_dice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendDiceRequest = parse_request(params)?;
    let emoji = request.emoji.unwrap_or_else(|| "🎲".to_string());

    let max_value = match emoji.as_str() {
        "🎯" | "🎲" | "🏀" | "🎳" => 6,
        "⚽" | "🏐" => 5,
        "🎰" => 64,
        _ => return Err(ApiError::bad_request("unsupported dice emoji")),
    };
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default().unsigned_abs();
    let value = (now_nanos % (max_value as u64)) as i64 + 1;

    let dice = Dice { emoji, value };
    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "dice",
        serde_json::to_value(dice).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

pub fn handle_send_document(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendDocumentRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
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
        request.reply_markup,
        "document",
        document,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_chat_action(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendChatActionRequest = parse_request(params)?;

    if request.business_connection_id.is_some() {
        return Err(ApiError::bad_request("business_connection_id is not supported in simulator"));
    }

    let normalized_action = request.action.trim().to_ascii_lowercase();
    if normalized_action.is_empty() {
        return Err(ApiError::bad_request("action is empty"));
    }
    if !is_supported_chat_action(&normalized_action) {
        return Err(ApiError::bad_request("unsupported action type"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chats::chat_id_as_i64(&request.chat_id, &chat_key);
    let sim_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?;

    if sim_chat.is_none() && chat_id <= 0 {
        return Err(ApiError::not_found("chat not found"));
    }

    let chat_type = sim_chat
        .as_ref()
        .map(|chat| chat.chat_type.as_str())
        .unwrap_or("private");

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    let actor_name = if actor_user_id == bot.id {
        bot.first_name.clone()
    } else {
        users::ensure_sim_user_record(&mut conn, actor_user_id)?.first_name
    };

    if chat_type != "private" {
        chats::ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &chat_key,
            actor_user_id,
            ChatSendKind::Other,
        )?;
    }

    let target_chat_id = sim_chat.as_ref().map(|chat| chat.chat_id).unwrap_or(chat_id);
    publish_sim_client_event(
        state,
        token,
        json!({
            "sim_event": "chat_action",
            "chat_id": target_chat_id,
            "action": normalized_action,
            "from_user_id": actor_user_id,
            "from_name": actor_name,
            "date": Utc::now().timestamp(),
        }),
    );

    Ok(Value::Bool(true))
}

pub fn handle_send_checklist(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendChecklistRequest = parse_request_with_legacy_checklist(params)?;
    if request.business_connection_id.trim().is_empty() {
        return Err(ApiError::bad_request("business_connection_id is empty"));
    }

    let checklist = normalize_input_checklist(&request.checklist)?;
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, chat) = chats::resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &chat_id_value,
        ChatSendKind::Other,
    )?;
    if chat.r#type != "private" {
        return Err(ApiError::bad_request(
            "sendChecklist is available only in private chats",
        ));
    }

    let business_connection_id = resolve_outbound_business_connection_for_bot_message(
        &mut conn,
        bot.id,
        &chat,
        Some(request.business_connection_id.as_str()),
    )?
    .ok_or_else(|| ApiError::bad_request("business_connection_id is empty"))?;

    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Other,
    )?;

    let reply_markup_value = request
        .reply_markup
        .as_ref()
        .and_then(|markup| serde_json::to_value(markup).ok());
    let reply_markup = messages::handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup_value.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, &checklist.title, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_value = messages::load_message_value(&mut conn, &bot, message_id)?;
    message_value["checklist"] = serde_json::to_value(&checklist).map_err(ApiError::internal)?;
    message_value["business_connection_id"] = Value::String(business_connection_id);

    if let Some(message_effect_id) = request
        .message_effect_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        message_value["message_effect_id"] = Value::String(message_effect_id.to_string());
    }

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = messages::load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?),
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

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
}

pub fn handle_send_game(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendGameRequest = parse_request(params)?;
    if request.game_short_name.trim().is_empty() {
        return Err(ApiError::bad_request("game_short_name is empty"));
    }

    let game = Game {
        title: request.game_short_name.clone(),
        description: format!("Game {}", request.game_short_name),
        photo: vec![PhotoSize {
            file_id: generate_telegram_file_id("game_photo"),
            file_unique_id: generate_telegram_file_unique_id(),
            width: 512,
            height: 512,
            file_size: None,
        }],
        text: None,
        text_entities: None,
        animation: None,
    };

    let message_value = send_payload_message(
        state,
        token,
        &Value::from(request.chat_id),
        None,
        None,
        request.reply_markup.as_ref().and_then(|m| serde_json::to_value(m).ok()),
        "game",
        serde_json::to_value(&game).map_err(ApiError::internal)?,
        request.message_thread_id,
    )?;

    let message_id = message_value
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("missing message_id in sendGame result"))?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = request.chat_id.to_string();
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO games (bot_id, chat_key, message_id, game_short_name, title, description, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            bot.id,
            chat_key,
            message_id,
            request.game_short_name,
            game.title,
            game.description,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(message_value)
}

pub fn handle_send_invoice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendInvoiceRequest = parse_request(params)?;
    let normalized_currency = request.currency.trim().to_ascii_uppercase();
    let max_tip_amount = request.max_tip_amount.unwrap_or(0);
    let suggested_tip_amounts = request.suggested_tip_amounts.clone().unwrap_or_default();

    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.description.trim().is_empty() {
        return Err(ApiError::bad_request("description is empty"));
    }
    if request.payload.trim().is_empty() {
        return Err(ApiError::bad_request("payload is empty"));
    }
    if normalized_currency.is_empty() {
        return Err(ApiError::bad_request("currency is empty"));
    }
    if request.prices.is_empty() {
        return Err(ApiError::bad_request("prices must include at least one item"));
    }
    if max_tip_amount < 0 {
        return Err(ApiError::bad_request("max_tip_amount must be non-negative"));
    }

    if let Some(photo_size) = request.photo_size {
        if photo_size <= 0 {
            return Err(ApiError::bad_request("photo_size must be greater than zero"));
        }
    }
    if let Some(photo_width) = request.photo_width {
        if photo_width <= 0 {
            return Err(ApiError::bad_request("photo_width must be greater than zero"));
        }
    }
    if let Some(photo_height) = request.photo_height {
        if photo_height <= 0 {
            return Err(ApiError::bad_request("photo_height must be greater than zero"));
        }
    }

    if request.is_flexible.unwrap_or(false) && !request.need_shipping_address.unwrap_or(false) {
        return Err(ApiError::bad_request("is_flexible requires need_shipping_address=true"));
    }

    if !suggested_tip_amounts.is_empty() {
        if suggested_tip_amounts.len() > 4 {
            return Err(ApiError::bad_request("suggested_tip_amounts can have at most 4 values"));
        }
        if max_tip_amount <= 0 {
            return Err(ApiError::bad_request("max_tip_amount must be positive when suggested_tip_amounts is set"));
        }

        let mut previous = 0;
        for tip in &suggested_tip_amounts {
            if *tip <= 0 {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be greater than zero"));
            }
            if *tip > max_tip_amount {
                return Err(ApiError::bad_request("suggested_tip_amounts values must be <= max_tip_amount"));
            }
            if *tip <= previous {
                return Err(ApiError::bad_request("suggested_tip_amounts must be strictly increasing"));
            }
            previous = *tip;
        }
    }

    let is_stars_invoice = normalized_currency == "XTR";
    let provider_token = request
        .provider_token
        .as_deref()
        .map(str::trim)
        .unwrap_or("");

    if is_stars_invoice {
        if !provider_token.is_empty() {
            return Err(ApiError::bad_request("provider_token must be empty for Telegram Stars invoices"));
        }
        if request.prices.len() != 1 {
            return Err(ApiError::bad_request("prices must contain exactly one item for Telegram Stars invoices"));
        }
        if max_tip_amount > 0 || !suggested_tip_amounts.is_empty() {
            return Err(ApiError::bad_request("tip fields are not supported for Telegram Stars invoices"));
        }
        if request.need_name.unwrap_or(false)
            || request.need_phone_number.unwrap_or(false)
            || request.need_email.unwrap_or(false)
            || request.need_shipping_address.unwrap_or(false)
            || request.send_phone_number_to_provider.unwrap_or(false)
            || request.send_email_to_provider.unwrap_or(false)
            || request.is_flexible.unwrap_or(false)
        {
            return Err(ApiError::bad_request("shipping/contact collection fields are not supported for Telegram Stars invoices"));
        }
    } else if provider_token.is_empty() {
        return Err(ApiError::bad_request("provider_token is required for non-Stars invoices"));
    }

    let mut total_amount: i64 = 0;
    for price in &request.prices {
        if price.label.trim().is_empty() {
            return Err(ApiError::bad_request("price label is empty"));
        }
        if price.amount <= 0 {
            return Err(ApiError::bad_request("price amount must be greater than zero"));
        }

        total_amount = total_amount
            .checked_add(price.amount)
            .ok_or_else(|| ApiError::bad_request("total amount overflow"))?;
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = chats::resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Invoice,
    )?;
    let message_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Invoice,
    )?;

    let reply_markup_value = request
        .reply_markup
        .as_ref()
        .and_then(|markup| serde_json::to_value(markup).ok());

    let reply_markup = messages::handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup_value.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, sender.id, request.description, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_value = messages::load_message_value(&mut conn, &bot, message_id)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));
    if let Some(thread_id) = message_thread_id {
        message_value["message_thread_id"] = Value::from(thread_id);
        message_value["is_topic_message"] = Value::Bool(true);
    }
    let message_chat_id = message_value
        .get("chat")
        .and_then(|chat| chat.get("id"))
        .and_then(Value::as_i64)
        .unwrap_or_else(|| chats::chat_id_as_i64(&request.chat_id, &chat_key));

    let invoice_title = request.title.clone();
    let invoice_description = request.description.clone();
    let invoice_payload = request.payload.clone();
    let invoice_currency = normalized_currency;

    let start_parameter = request.start_parameter.clone().unwrap_or_default();

    let invoice = Invoice {
        title: invoice_title.clone(),
        description: invoice_description.clone(),
        start_parameter,
        currency: invoice_currency.clone(),
        total_amount,
    };

    message_value["invoice"] = serde_json::to_value(invoice).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT OR REPLACE INTO invoices
         (bot_id, chat_key, message_id, title, description, payload, currency, total_amount,
          max_tip_amount, suggested_tip_amounts_json, start_parameter, provider_data,
          photo_url, photo_size, photo_width, photo_height,
          need_name, need_phone_number, need_email, need_shipping_address,
          send_phone_number_to_provider, send_email_to_provider,
          is_flexible, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8,
                 ?9, ?10, ?11, ?12,
                 ?13, ?14, ?15, ?16,
                 ?17, ?18, ?19, ?20,
                 ?21, ?22,
                 ?23, ?24)",
        params![
            bot.id,
            chat_key,
            message_id,
            invoice_title,
            invoice_description,
            invoice_payload,
            invoice_currency,
            total_amount,
            max_tip_amount,
            if suggested_tip_amounts.is_empty() {
                None::<String>
            } else {
                Some(serde_json::to_string(&suggested_tip_amounts).map_err(ApiError::internal)?)
            },
            request.start_parameter,
            request.provider_data,
            request.photo_url,
            request.photo_size,
            request.photo_width,
            request.photo_height,
            if request.need_name.unwrap_or(false) { 1 } else { 0 },
            if request.need_phone_number.unwrap_or(false) { 1 } else { 0 },
            if request.need_email.unwrap_or(false) { 1 } else { 0 },
            if request.need_shipping_address.unwrap_or(false) { 1 } else { 0 },
            if request.send_phone_number_to_provider.unwrap_or(false) { 1 } else { 0 },
            if request.send_email_to_provider.unwrap_or(false) { 1 } else { 0 },
            if request.is_flexible.unwrap_or(false) { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = messages::load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = if is_channel_post {
        json!({
            "update_id": 0,
            "channel_post": message_value.clone(),
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_value.clone(),
        })
    };

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    publish_sim_client_event(
        state,
        token,
        json!({
            "sim_event": "invoice_meta",
            "chat_id": message_chat_id,
            "message_id": message_id,
            "invoice_meta": {
                "photo_url": request.photo_url,
                "max_tip_amount": max_tip_amount,
                "suggested_tip_amounts": if suggested_tip_amounts.is_empty() { Value::Null } else { json!(suggested_tip_amounts) },
                "need_name": request.need_name.unwrap_or(false),
                "need_phone_number": request.need_phone_number.unwrap_or(false),
                "need_email": request.need_email.unwrap_or(false),
                "need_shipping_address": request.need_shipping_address.unwrap_or(false),
                "is_flexible": request.is_flexible.unwrap_or(false),
                "send_phone_number_to_provider": request.send_phone_number_to_provider.unwrap_or(false),
                "send_email_to_provider": request.send_email_to_provider.unwrap_or(false)
            }
        }),
    );
    Ok(message_value)
}

pub fn handle_send_location(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendLocationRequest = parse_request(params)?;

    let location = Location {
        latitude: request.latitude,
        longitude: request.longitude,
        horizontal_accuracy: request.horizontal_accuracy,
        live_period: request.live_period,
        heading: request.heading,
        proximity_alert_radius: request.proximity_alert_radius,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "location",
        serde_json::to_value(location).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

pub fn handle_send_media_group(
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

    let media_group_id = generate_telegram_numeric_id();
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
        let sim_parse_mode = normalize_sim_parse_mode(parse_mode);
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
                    None,
                    "photo",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
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
                    None,
                    "video",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
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
                    None,
                    "audio",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
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
                    None,
                    "document",
                    payload,
                    Some(&media_group_id),
                    request.message_thread_id,
                    sim_parse_mode,
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

pub fn handle_send_paid_media(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendPaidMediaRequest =
        parse_request_ignoring_prefixed_fields(params, &["paid_media_"])?;

    if request.star_count <= 0 {
        return Err(ApiError::bad_request(
            "star_count must be greater than zero",
        ));
    }

    if request.media.is_empty() || request.media.len() > 10 {
        return Err(ApiError::bad_request("media must include 1-10 items"));
    }

    let explicit_caption_entities = request
        .caption_entities
        .as_ref()
        .map(|entities| serde_json::to_value(entities).map_err(ApiError::internal))
        .transpose()?;
    let (caption, caption_entities) = parse_optional_formatted_text(
        request.caption.as_deref(),
        request.parse_mode.as_deref(),
        explicit_caption_entities,
    );
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());

    let mut paid_media_items = Vec::<Value>::with_capacity(request.media.len());
    for raw_item in &request.media {
        let item = raw_item
            .extra
            .as_object()
            .ok_or_else(|| ApiError::bad_request("paid media item must be an object"))?;

        let media_type = item
            .get("type")
            .and_then(Value::as_str)
            .map(|value| value.to_ascii_lowercase())
            .ok_or_else(|| ApiError::bad_request("paid media item type is required"))?;
        let media_ref = item
            .get("media")
            .ok_or_else(|| ApiError::bad_request("paid media item media is required"))?;

        let mapped = match media_type.as_str() {
            "photo" => {
                let file = resolve_media_file(state, token, media_ref, "photo")?;
                json!({
                    "type": "photo",
                    "photo": [{
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": 1280,
                        "height": 720,
                        "file_size": file.file_size,
                    }],
                })
            }
            "video" => {
                let file = resolve_media_file(state, token, media_ref, "video")?;
                json!({
                    "type": "video",
                    "video": {
                        "file_id": file.file_id,
                        "file_unique_id": file.file_unique_id,
                        "width": item.get("width").and_then(Value::as_i64).unwrap_or(1280),
                        "height": item.get("height").and_then(Value::as_i64).unwrap_or(720),
                        "duration": item.get("duration").and_then(Value::as_i64).unwrap_or(0),
                        "mime_type": file.mime_type,
                        "file_size": file.file_size,
                    }
                })
            }
            _ => {
                return Err(ApiError::bad_request(
                    "sendPaidMedia supports only photo and video",
                ));
            }
        };

        paid_media_items.push(mapped);
    }

    let payload = json!({
        "star_count": request.star_count,
        "paid_media": paid_media_items,
    });

    send_paid_media_message(
        state,
        token,
        &request.chat_id,
        caption,
        caption_entities,
        request.reply_markup,
        payload,
        request.star_count,
        request.show_caption_above_media,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_gift(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SendGiftRequest = parse_request(params)?;

    if request.user_id.is_some() == request.chat_id.is_some() {
        return Err(ApiError::bad_request(
            "exactly one of user_id or chat_id must be provided",
        ));
    }

    let gift_entry = find_sim_catalog_gift(request.gift_id.as_str())
        .ok_or_else(|| ApiError::bad_request("gift_id is invalid"))?;

    let base_star_count = gift_entry.gift.star_count.max(0);
    let upgrade_star_count = gift_entry.gift.upgrade_star_count.unwrap_or(0).max(0);
    let pay_for_upgrade = request.pay_for_upgrade.unwrap_or(false);

    if pay_for_upgrade && upgrade_star_count <= 0 {
        return Err(ApiError::bad_request(
            "selected gift doesn't support prepaid upgrade",
        ));
    }

    let charge_star_count = if pay_for_upgrade {
        base_star_count.saturating_add(upgrade_star_count)
    } else {
        base_star_count
    };
    if charge_star_count <= 0 {
        return Err(ApiError::bad_request("gift star_count is invalid"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sender = ensure_user(&mut conn, current_request_actor_user_id(), None, None)?;

    let mut owner_user_id: Option<i64> = None;
    let mut owner_chat_id: Option<i64> = None;
    let mut gift_message_chat: Option<(String, Chat)> = None;

    if let Some(user_id) = request.user_id {
        if user_id <= 0 {
            return Err(ApiError::bad_request("user_id is invalid"));
        }
        let recipient = users::ensure_user(&mut conn, Some(user_id), None, None)?;
        owner_user_id = Some(recipient.id);

        let chat_key = recipient.id.to_string();
        ensure_chat(&mut conn, &chat_key)?;
        gift_message_chat = Some((
            chat_key,
            Chat {
                id: recipient.id,
                r#type: "private".to_string(),
                title: None,
                username: recipient.username.clone(),
                first_name: Some(recipient.first_name.clone()),
                last_name: recipient.last_name.clone(),
                is_forum: None,
                is_direct_messages: None,
            },
        ));
    } else if let Some(chat_value) = request.chat_id.as_ref() {
        let chat_key = value_to_chat_key(chat_value)?;
        let chat_id = chats::chat_id_as_i64(chat_value, &chat_key);
        let sim_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?
            .or(chats::load_sim_chat_record_by_chat_id(&mut conn, bot.id, chat_id)?)
            .ok_or_else(|| ApiError::not_found("chat not found"))?;

        if sim_chat.chat_type != "channel" {
            return Err(ApiError::bad_request(
                "chat_id must refer to a channel chat",
            ));
        }

        owner_chat_id = Some(sim_chat.chat_id);
        ensure_chat(&mut conn, &sim_chat.chat_key)?;
        gift_message_chat = Some((
            sim_chat.chat_key.clone(),
            build_chat_from_group_record(&sim_chat),
        ));
    }

    let now = Utc::now().timestamp();
    ensure_bot_star_balance_for_charge(&mut conn, bot.id, charge_star_count, now)?;

    let owned_gift_id = format!("owned_gift_{}", generate_telegram_numeric_id());
    let ledger_user_id = owner_user_id.unwrap_or(sender.id);
    let ledger_charge_id = format!("gift_send_{}", owned_gift_id);
    let text = request
        .text
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let gift_message_text = text.clone();
    let entities_json = request
        .text_entities
        .as_ref()
        .map(|entities| serde_json::to_string(entities).map_err(ApiError::internal))
        .transpose()?;
    let gift_json = serde_json::to_string(&gift_entry.gift).map_err(ApiError::internal)?;
    let can_be_upgraded = upgrade_star_count > 0;

    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'gift_send')",
        params![
            format!("gift_send_{}", generate_telegram_numeric_id()),
            bot.id,
            ledger_user_id,
            ledger_charge_id,
            -charge_star_count,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO sim_owned_gifts
         (bot_id, owned_gift_id, owner_user_id, owner_chat_id, sender_user_id,
          gift_id, gift_json, gift_star_count, is_unique, is_unlimited, is_from_blockchain,
          send_date, text, entities_json, is_private, is_saved, can_be_upgraded, was_refunded,
          convert_star_count, prepaid_upgrade_star_count, is_upgrade_separate,
          unique_gift_number, transfer_star_count, next_transfer_date, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5,
                 ?6, ?7, ?8, 0, ?9, ?10,
                 ?11, ?12, ?13, 0, 0, ?14, 0,
                 ?15, ?16, ?17,
                 NULL, NULL, NULL, ?18, ?18)",
        params![
            bot.id,
            owned_gift_id.clone(),
            owner_user_id,
            owner_chat_id,
            sender.id,
            gift_entry.gift.id,
            gift_json,
            gift_entry.gift.star_count,
            if gift_entry.is_unlimited { 1 } else { 0 },
            if gift_entry.is_from_blockchain { 1 } else { 0 },
            now,
            text.clone(),
            entities_json,
            if can_be_upgraded { 1 } else { 0 },
            if can_be_upgraded {
                Some((gift_entry.gift.star_count / 2).max(1))
            } else {
                None
            },
            if pay_for_upgrade {
                Some(upgrade_star_count)
            } else {
                None
            },
            if can_be_upgraded && !pay_for_upgrade { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    if let Some(target_user_id) = owner_user_id {
        conn.execute(
            "UPDATE users
             SET gift_count = COALESCE(gift_count, 0) + 1
             WHERE id = ?1",
            params![target_user_id],
        )
        .map_err(ApiError::internal)?;
    }

    if let Some((chat_key, chat)) = gift_message_chat {
        let sender_user = users::build_user_from_sim_record(&sender, false);
        let mut gift_payload = Map::<String, Value>::new();
        gift_payload.insert(
            "gift".to_string(),
            serde_json::to_value(&gift_entry.gift).map_err(ApiError::internal)?,
        );
        gift_payload.insert(
            "owned_gift_id".to_string(),
            Value::String(owned_gift_id.clone()),
        );
        gift_payload.insert(
            "can_be_upgraded".to_string(),
            Value::Bool(can_be_upgraded),
        );
        if can_be_upgraded {
            gift_payload.insert(
                "convert_star_count".to_string(),
                Value::from((gift_entry.gift.star_count / 2).max(1)),
            );
        }
        if pay_for_upgrade {
            gift_payload.insert(
                "prepaid_upgrade_star_count".to_string(),
                Value::from(upgrade_star_count),
            );
        }
        gift_payload.insert(
            "is_upgrade_separate".to_string(),
            Value::Bool(can_be_upgraded && !pay_for_upgrade),
        );
        if let Some(gift_text) = gift_message_text {
            gift_payload.insert("text".to_string(), Value::String(gift_text));
        }
        if let Some(entities) = request.text_entities.as_ref() {
            gift_payload.insert(
                "entities".to_string(),
                serde_json::to_value(entities).map_err(ApiError::internal)?,
            );
        }

        let mut service_fields = Map::<String, Value>::new();
        service_fields.insert("gift".to_string(), Value::Object(gift_payload));

        emit_service_message_update(
            state,
            &mut conn,
            token,
            bot.id,
            &chat_key,
            &chat,
            &sender_user,
            now,
            format!(
                "{} sent a gift",
                display_name_for_service_user(&sender_user)
            ),
            service_fields,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_send_message_draft(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let mut normalized_params = params.clone();
    if let Some(raw_text) = normalized_params.get("text").cloned() {
        if !raw_text.is_string() {
            if let Some(text) = value_to_optional_string(&raw_text) {
                normalized_params.insert("text".to_string(), Value::String(text));
            }
        }
    }

    let request: SendMessageDraftRequest = parse_request(&normalized_params)?;
    if request.draft_id <= 0 {
        return Err(ApiError::bad_request("draft_id is invalid"));
    }

    let chat_id_value = Value::from(request.chat_id);
    let (chat_key, resolved_message_thread_id, existing_message_id) = {
        let mut conn = lock_db(state)?;
        let bot = ensure_bot(&mut conn, token)?;

        let (chat_key, chat) = chats::resolve_bot_outbound_chat(
            &mut conn,
            bot.id,
            &chat_id_value,
            ChatSendKind::Text,
        )?;
        if chat.r#type != "private" {
            return Err(ApiError::bad_request(
                "sendMessageDraft is available only in private chats",
            ));
        }
        let resolved_message_thread_id = groups::resolve_forum_message_thread_for_chat_key(
            &mut conn,
            bot.id,
            &chat_key,
            request.message_thread_id,
        )?;
        let message_thread_scope = resolved_message_thread_id.unwrap_or(0);

        let existing_message_id: Option<i64> = conn
            .query_row(
                "SELECT message_id
                 FROM sim_message_drafts
                 WHERE bot_id = ?1
                   AND chat_key = ?2
                   AND message_thread_id = ?3
                   AND draft_id = ?4",
                params![bot.id, &chat_key, message_thread_scope, request.draft_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        (chat_key, resolved_message_thread_id, existing_message_id)
    };

    let message_thread_scope = resolved_message_thread_id.unwrap_or(0);

    if let Some(message_id) = existing_message_id {
        let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
        let explicit_entities = request
            .entities
            .as_ref()
            .and_then(|v| serde_json::to_value(v).ok());
        let should_auto_detect_entities = explicit_entities.is_none();
        let (parsed_text, parsed_entities) = parse_formatted_text(
            &request.text,
            request.parse_mode.as_deref(),
            explicit_entities,
        );
        let parsed_entities = if should_auto_detect_entities {
            merge_auto_message_entities(&parsed_text, parsed_entities)
        } else {
            parsed_entities
        };

        if parsed_text.trim().is_empty() {
            return Err(ApiError::bad_request("text is empty"));
        }

        let mut conn = lock_db(state)?;
        let bot = ensure_bot(&mut conn, token)?;
        let updated = conn
            .execute(
                "UPDATE messages SET text = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
                params![parsed_text, bot.id, &chat_key, message_id],
            )
            .map_err(ApiError::internal)?;

        if updated == 0 {
            conn.execute(
                "DELETE FROM sim_message_drafts
                 WHERE bot_id = ?1
                   AND chat_key = ?2
                   AND message_thread_id = ?3
                   AND draft_id = ?4",
                params![bot.id, &chat_key, message_thread_scope, request.draft_id],
            )
            .map_err(ApiError::internal)?;
        } else {
            let mut streamed_message = messages::load_message_value(&mut conn, &bot, message_id)?;
            if let Some(entities) = parsed_entities {
                streamed_message["entities"] = entities;
            } else {
                streamed_message.as_object_mut().map(|obj| obj.remove("entities"));
            }
            if let Some(mode) = sim_parse_mode {
                streamed_message["sim_parse_mode"] = Value::String(mode);
            } else {
                streamed_message
                    .as_object_mut()
                    .map(|obj| obj.remove("sim_parse_mode"));
            }
            streamed_message.as_object_mut().map(|obj| obj.remove("edit_date"));

            let update_value = serde_json::to_value(Update {
                update_id: 0,
                message: Some(
                    serde_json::from_value(streamed_message.clone()).map_err(ApiError::internal)?,
                ),
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
                managed_bot: None,
            })
            .map_err(ApiError::internal)?;
            persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

            let now = Utc::now().timestamp();
            conn.execute(
                "UPDATE sim_message_drafts
                 SET updated_at = ?1
                 WHERE bot_id = ?2
                   AND chat_key = ?3
                   AND message_thread_id = ?4
                   AND draft_id = ?5",
                params![
                    now,
                    bot.id,
                    &chat_key,
                    message_thread_scope,
                    request.draft_id,
                ],
            )
            .map_err(ApiError::internal)?;

            return Ok(Value::Bool(true));
        }
    }

    let mut send_params = HashMap::<String, Value>::new();
    send_params.insert("chat_id".to_string(), chat_id_value);
    if let Some(thread_id) = resolved_message_thread_id {
        send_params.insert("message_thread_id".to_string(), Value::from(thread_id));
    }
    send_params.insert("text".to_string(), Value::String(request.text));

    if let Some(parse_mode) = request.parse_mode {
        send_params.insert("parse_mode".to_string(), Value::String(parse_mode));
    }

    if let Some(entities) = request.entities {
        send_params.insert(
            "entities".to_string(),
            serde_json::to_value(entities).map_err(ApiError::internal)?,
        );
    }

    let created_message = handle_send_message(state, token, &send_params)?;
    let message_id = created_message
        .get("message_id")
        .and_then(Value::as_i64)
        .ok_or_else(|| ApiError::internal("sendMessageDraft failed to return a message_id"))?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO sim_message_drafts
         (bot_id, chat_key, message_thread_id, draft_id, message_id, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         ON CONFLICT(bot_id, chat_key, message_thread_id, draft_id)
         DO UPDATE SET
            message_id = excluded.message_id,
            updated_at = excluded.updated_at",
        params![
            bot.id,
            &chat_key,
            message_thread_scope,
            request.draft_id,
            message_id,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(Value::Bool(true))
}

pub fn handle_send_message(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendMessageRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
    let explicit_entities = request
        .entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let should_auto_detect_entities = explicit_entities.is_none();
    let (parsed_text, parsed_entities) = parse_formatted_text(
        &request.text,
        request.parse_mode.as_deref(),
        explicit_entities,
    );
    let parsed_entities = if should_auto_detect_entities {
        merge_auto_message_entities(&parsed_text, parsed_entities)
    } else {
        parsed_entities
    };

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = chats::resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Text,
    )?;
    let business_connection_id = resolve_outbound_business_connection_for_bot_message(
        &mut conn,
        bot.id,
        &chat,
        request.business_connection_id.as_deref(),
    )?;
    let message_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Text,
    )?;

    let reply_markup = messages::handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let base_message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender,
        "text": parsed_text,
    });

    let mut base_message_json = base_message_json;
    if let Some(entities) = parsed_entities {
        base_message_json["entities"] = entities;
    }
    if let Some(thread_id) = message_thread_id {
        base_message_json["message_thread_id"] = Value::from(thread_id);
        base_message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        base_message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if let Some(mode) = sim_parse_mode {
        base_message_json["sim_parse_mode"] = Value::String(mode);
    }
    let message: Message = serde_json::from_value(base_message_json).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let is_business_message = business_connection_id.is_some();

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post || is_business_message {
            None
        } else {
            Some(message.clone())
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(message.clone())
        } else {
            None
        },
        edited_channel_post: None,
        business_connection: None,
        business_message: if is_business_message {
            Some(message.clone())
        } else {
            None
        },
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();

    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }
    if is_business_message {
        update_value["business_message"] = message_value.clone();
    } else if is_channel_post {
        update_value["channel_post"] = message_value.clone();
    } else {
        update_value["message"] = message_value.clone();
    }

    channels::enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("channel_post").cloned() {
            message_value = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    webhook::dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update.clone());

    if is_channel_post {
        channels::ensure_linked_discussion_forward_for_channel_post(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        )?;
    }

    Ok(message_value)
}

pub fn handle_send_photo(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendPhotoRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
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
        request.reply_markup,
        "photo",
        photo,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendPollRequest = parse_request(params)?;
    let explicit_question_entities = request
        .question_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (question, question_entities) = parse_formatted_text(
        &request.question,
        request.question_parse_mode.as_deref(),
        explicit_question_entities,
    );

    if question.trim().is_empty() {
        return Err(ApiError::bad_request("question is empty"));
    }
    if question.chars().count() > 300 {
        return Err(ApiError::bad_request("question is too long"));
    }

    if request.options.len() < 2 || request.options.len() > 10 {
        return Err(ApiError::bad_request("options must include 2-10 items"));
    }

    if request.open_period.is_some() && request.close_date.is_some() {
        return Err(ApiError::bad_request("open_period and close_date are mutually exclusive"));
    }

    if let Some(open_period) = request.open_period {
        if !(5..=2_628_000).contains(&open_period) {
            return Err(ApiError::bad_request("open_period must be between 5 and 2628000"));
        }
    }

    let now = Utc::now().timestamp();
    if let Some(close_date) = request.close_date {
        let delta = close_date - now;
        if !(5..=2_628_000).contains(&delta) {
            return Err(ApiError::bad_request("close_date must be 5-2628000 seconds in the future"));
        }
    }

    let poll_type = request
        .type_param
        .clone()
        .unwrap_or_else(|| "regular".to_string());
    if poll_type != "regular" && poll_type != "quiz" {
        return Err(ApiError::bad_request("poll type must be regular or quiz"));
    }

    let allows_multiple_answers = request.allows_multiple_answers.unwrap_or(false);

    let correct_option_ids = request.correct_option_ids.clone();
    if poll_type == "quiz" {
        let Some(correct_ids) = correct_option_ids.as_ref() else {
            return Err(ApiError::bad_request("quiz poll requires correct_option_ids"));
        };
        if correct_ids.is_empty() {
            return Err(ApiError::bad_request("correct_option_ids must not be empty"));
        }
        if !allows_multiple_answers && correct_ids.len() != 1 {
            return Err(ApiError::bad_request(
                "quiz poll must have exactly one correct option when allows_multiple_answers is false",
            ));
        }
        for idx in correct_ids {
            if *idx < 0 || *idx >= request.options.len() as i64 {
                return Err(ApiError::bad_request("correct_option_ids contains out-of-range value"));
            }
        }
    } else if correct_option_ids.is_some() {
        return Err(ApiError::bad_request("correct_option_ids is allowed only for quiz polls"));
    }

    let allows_revoting = request
        .allows_revoting
        .unwrap_or_else(|| poll_type != "quiz");

    let explicit_explanation_entities = request
        .explanation_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (explanation, explanation_entities) = parse_optional_formatted_text(
        request.explanation.as_deref(),
        request.explanation_parse_mode.as_deref(),
        explicit_explanation_entities,
    );

    if poll_type == "quiz" {
        if let Some(exp) = explanation.as_ref() {
            if exp.chars().count() > 200 {
                return Err(ApiError::bad_request("explanation is too long"));
            }
        }
    }

    let explicit_description_entities = request
        .description_entities
        .as_ref()
        .and_then(|v| serde_json::to_value(v).ok());
    let (description, description_entities) = parse_optional_formatted_text(
        request.description.as_deref(),
        request.description_parse_mode.as_deref(),
        explicit_description_entities,
    );

    let mut poll_options: Vec<PollOption> = Vec::with_capacity(request.options.len());
    for item in &request.options {
        let explicit_option_entities = item
            .text_entities
            .as_ref()
            .and_then(|v| serde_json::to_value(v).ok());
        let (option_text, option_entities) = parse_formatted_text(
            &item.text,
            item.text_parse_mode.as_deref(),
            explicit_option_entities,
        );

        if option_text.trim().is_empty() {
            return Err(ApiError::bad_request("poll option text is empty"));
        }
        if option_text.chars().count() > 100 {
            return Err(ApiError::bad_request("poll option text is too long"));
        }

        let text_entities = option_entities
            .and_then(|value| serde_json::from_value(value).ok());

        poll_options.push(PollOption {
            persistent_id: generate_telegram_numeric_id(),
            text: option_text,
            text_entities,
            voter_count: 0,
            added_by_user: None,
            added_by_chat: None,
            addition_date: None,
        });
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, chat) = chats::resolve_bot_outbound_chat(
        &mut conn,
        bot.id,
        &request.chat_id,
        ChatSendKind::Poll,
    )?;
    let message_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        request.message_thread_id,
    )?;
    let sender = resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Poll,
    )?;

    let reply_markup = messages::handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, sender.id, question, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let poll_id = generate_telegram_numeric_id();
    let correct_option_id_for_storage = correct_option_ids
        .as_ref()
        .and_then(|ids| ids.first().copied());
    let poll = Poll {
        id: poll_id.clone(),
        question,
        question_entities: question_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
        options: poll_options.clone(),
        total_voter_count: 0,
        is_closed: request.is_closed.unwrap_or(false),
        is_anonymous: request.is_anonymous.unwrap_or(true),
        r#type: poll_type,
        allows_multiple_answers,
        allows_revoting,
        correct_option_ids,
        explanation,
        explanation_entities: explanation_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
        open_period: request.open_period,
        close_date: request.close_date,
        description,
        description_entities: description_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
    };

    conn.execute(
        "INSERT INTO polls (id, bot_id, chat_key, message_id, question, options_json, total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19)",
        params![
            poll.id,
            bot.id,
            chat_key,
            message_id,
            poll.question,
            serde_json::to_string(&poll.options).map_err(ApiError::internal)?,
            poll.total_voter_count,
            if poll.is_closed { 1 } else { 0 },
            if poll.is_anonymous { 1 } else { 0 },
            poll.r#type,
            if poll.allows_multiple_answers { 1 } else { 0 },
            if poll.allows_revoting { 1 } else { 0 },
            correct_option_id_for_storage,
            poll.correct_option_ids
                .as_ref()
                .map(|ids| serde_json::to_string(ids).map_err(ApiError::internal))
                .transpose()?,
            poll.explanation,
            poll.description,
            poll.open_period,
            poll.close_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT OR REPLACE INTO poll_metadata (poll_id, question_entities_json, explanation_entities_json, description_entities_json)
         VALUES (?1, ?2, ?3, ?4)",
        params![
            poll.id,
            question_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| question_entities.as_ref().unwrap().to_string()),
            explanation_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| explanation_entities.as_ref().unwrap().to_string()),
            description_entities
                .as_ref()
                .and_then(Value::as_array)
                .map(|_| description_entities.as_ref().unwrap().to_string()),
        ],
    )
    .map_err(ApiError::internal)?;

    let mut message_value = messages::load_message_value(&mut conn, &bot, message_id)?;
    message_value["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));
    message_value.as_object_mut().map(|obj| obj.remove("edit_date"));
    if let Some(thread_id) = message_thread_id {
        message_value["message_thread_id"] = Value::from(thread_id);
        message_value["is_topic_message"] = Value::Bool(true);
    }

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = messages::load_message_value(&mut conn, &bot, reply_parameters.message_id) {
            let belongs_to_chat = reply_value
                .get("chat")
                .and_then(|v| v.get("id"))
                .and_then(Value::as_i64)
                .map(|chat_id| chat_id.to_string() == reply_chat_key)
                .unwrap_or(false);

            if belongs_to_chat {
                message_value["reply_to_message"] = reply_value;
            } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
                return Err(ApiError::bad_request("replied message not found"));
            }
        } else if !reply_parameters.allow_sending_without_reply.unwrap_or(false) {
            return Err(ApiError::bad_request("replied message not found"));
        }
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: if is_channel_post {
            None
        } else {
            Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?)
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
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
        managed_bot: None,
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
}

pub fn handle_send_sticker(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendStickerRequest = parse_request(params)?;
    let sticker_input = parse_input_file_value(&request.sticker, "sticker")?;
    let file = resolve_media_file(state, token, &sticker_input, "sticker")?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = load_sticker_meta(&mut conn, bot.id, &file.file_id)?;
    drop(conn);

    let format = sticker_meta
        .as_ref()
        .map(|m| m.format.as_str())
        .or_else(|| infer_sticker_format_from_file(&file))
        .unwrap_or("static");
    let is_animated = format == "animated";
    let is_video = format == "video";

    let sticker = Sticker {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        r#type: sticker_meta
            .as_ref()
            .map(|m| m.sticker_type.clone())
            .unwrap_or_else(|| "regular".to_string()),
        width: 512,
        height: 512,
        is_animated,
        is_video,
        thumbnail: None,
        emoji: request.emoji.or_else(|| sticker_meta.as_ref().and_then(|m| m.emoji.clone())),
        set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
        premium_animation: None,
        mask_position: sticker_meta
            .as_ref()
            .and_then(|m| m.mask_position_json.as_ref())
            .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
        custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
        needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
        file_size: file.file_size,
    };

    send_media_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "sticker",
        serde_json::to_value(sticker).map_err(ApiError::internal)?,
        request.message_thread_id,
        None,
    )
}

pub fn handle_send_venue(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVenueRequest = parse_request(params)?;
    if request.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if request.address.trim().is_empty() {
        return Err(ApiError::bad_request("address is empty"));
    }

    let venue = Venue {
        location: Location {
            latitude: request.latitude,
            longitude: request.longitude,
            horizontal_accuracy: None,
            live_period: None,
            heading: None,
            proximity_alert_radius: None,
        },
        title: request.title,
        address: request.address,
        foursquare_id: request.foursquare_id,
        foursquare_type: request.foursquare_type,
        google_place_id: request.google_place_id,
        google_place_type: request.google_place_type,
    };

    send_payload_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "venue",
        serde_json::to_value(venue).map_err(ApiError::internal)?,
        request.message_thread_id,
    )
}

pub fn handle_send_video_note(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVideoNoteRequest = parse_request(params)?;
    let video_note_input = parse_input_file_value(&request.video_note, "video_note")?;
    let file = resolve_media_file(state, token, &video_note_input, "video_note")?;

    let length = request.length.unwrap_or(384).max(1);
    let video_note = serde_json::to_value(VideoNote {
        file_id: file.file_id,
        file_unique_id: file.file_unique_id,
        length,
        duration: request.duration.unwrap_or(0),
        thumbnail: None,
        file_size: file.file_size,
    })
    .map_err(ApiError::internal)?;

    send_media_message(
        state,
        token,
        &request.chat_id,
        None,
        None,
        request.reply_markup,
        "video_note",
        video_note,
        request.message_thread_id,
        None,
    )
}

pub fn handle_send_video(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVideoRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
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
        request.reply_markup,
        "video",
        video,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_send_voice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: SendVoiceRequest = parse_request(params)?;
    let sim_parse_mode = normalize_sim_parse_mode(request.parse_mode.as_deref());
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
        request.reply_markup,
        "voice",
        voice,
        request.message_thread_id,
        sim_parse_mode,
    )
}

pub fn handle_set_message_reaction(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetMessageReactionRequest = parse_request(params)?;

    let reactions = normalize_reaction_values(request.reaction.as_ref().map(|r| {
        r.iter().map(|item| item.extra.clone()).collect::<Vec<Value>>()
    }))?;

    let chat_key = value_to_chat_key(&request.chat_id)?;
    let chat_id = chats::chat_id_as_i64(&request.chat_id, &chat_key);

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
        can_manage_bots: None,
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

pub fn handle_stop_message_live_location(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: StopMessageLiveLocationRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "stopMessageLiveLocation",
    )?;

    ensure_message_can_be_edited_by_bot(
        &mut conn,
        bot.id,
        &chat_key,
        message_id,
        via_inline_message,
    )?;

    let mut edited_message = messages::load_message_value(&mut conn, &bot, message_id)?;
    if let Some(location_obj) = edited_message.get_mut("location").and_then(Value::as_object_mut) {
        location_obj.remove("live_period");
        location_obj.remove("heading");
        location_obj.remove("proximity_alert_radius");
    }
    if let Some(venue_obj) = edited_message.get_mut("venue").and_then(Value::as_object_mut) {
        if let Some(location_obj) = venue_obj.get_mut("location").and_then(Value::as_object_mut) {
            location_obj.remove("live_period");
            location_obj.remove("heading");
            location_obj.remove("proximity_alert_radius");
        }
    }

    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

pub fn handle_stop_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: StopPollRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let row: Option<(
        String,
        String,
        String,
        i64,
        i64,
        i64,
        String,
        i64,
        i64,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<i64>,
        Option<i64>,
        Option<String>,
        Option<String>,
        Option<String>,
    )> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type,
                    p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                    p.explanation, p.description, p.open_period, p.close_date,
                    m.question_entities_json, m.explanation_entities_json, m.description_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, request.message_id],
            |r| Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
                r.get(9)?,
                r.get(10)?,
                r.get(11)?,
                r.get(12)?,
                r.get(13)?,
                r.get(14)?,
                r.get(15)?,
                r.get(16)?,
                r.get(17)?,
            )),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, total_voter_count, _is_closed, is_anonymous, poll_type, allows_multiple_answers, allows_revoting, correct_option_id, correct_option_ids_json, explanation, description, open_period, close_date, question_entities_json, explanation_entities_json, description_entities_json)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    conn.execute(
        "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2",
        params![poll_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
    let question_entities = question_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let explanation_entities = explanation_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let description_entities = description_entities_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok());
    let correct_option_ids = correct_option_ids_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
        .or_else(|| correct_option_id.map(|id| vec![id]));
    let poll = Poll {
        id: poll_id,
        question,
        question_entities,
        options,
        total_voter_count,
        is_closed: true,
        is_anonymous: is_anonymous == 1,
        r#type: poll_type,
        allows_multiple_answers: allows_multiple_answers == 1,
        allows_revoting: allows_revoting == 1,
        correct_option_ids,
        explanation,
        explanation_entities,
        open_period,
        close_date,
        description,
        description_entities,
    };

    let mut edited_message = messages::load_message_value(&mut conn, &bot, request.message_id)?;
    edited_message["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    edited_message.as_object_mut().map(|obj| obj.remove("text"));
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;
    Ok(edited_message)
}