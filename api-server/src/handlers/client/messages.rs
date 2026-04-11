use actix_web::web::Data;
use chrono::Utc;
use std::fs;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};
use serde::de::DeserializeOwned;
use std::path::Path;
use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{strip_nulls, ApiError, ApiResult};

use super::types::users::SimUserRecord;
use super::types::channels::LinkedDiscussionTransportContext;
use super::types::payments::SimPurchasePaidMediaRequest;
use super::types::messages::{
    StoredFile, SimSendUserMediaRequest, SimClearHistoryRequest, SimSendUserMessageRequest,
    SimEditUserMediaRequest, SimSendUserDiceRequest, SimSendUserContactRequest, SimSendUserLocationRequest,
    SimSendUserGameRequest, SimSendUserVenueRequest, SimSetUserReactionRequest, StickerMeta
};

use crate::generated::types::{
    Chat, User, ReplyKeyboardMarkup, ReplyKeyboardRemove, Message, Update, 
    ChecklistTask, InputChecklistTask, Sticker, MaskPosition, MessageEntity, Checklist,
    PhotoSize, Video, VideoNote, Voice, Document, ManagedBotCreated, Audio, Animation,
    MessageReactionUpdated, ManagedBotUpdated, SuggestedPostInfo, SuggestedPostPrice,
    Game, Dice, Location, Venue, Contact, Poll, PollOption, PaidMediaPurchased,
    ReactionType, ReactionCount, MessageReactionCountUpdated, InputChecklist, InputSticker
};
use crate::handlers::utils::updates::{value_to_chat_key, current_request_actor_user_id, value_to_optional_bool_loose};
use crate::handlers::utils::text::{
    merge_auto_message_entities, parse_optional_formatted_text, parse_formatted_text,
    utf16_len, entity_text_by_utf16_span
};
use crate::handlers::utils::storage::{download_remote_file, store_binary_file};
use crate::handlers::{
    parse_request, generate_telegram_numeric_id, sql_value_to_rusqlite,
    value_to_optional_string, generate_telegram_file_id, generate_telegram_file_unique_id,
    decode_request_value
};

use super::chats::ChatSendKind;
use super::users::SimUserPayload;
use super::{bot, business, chats, channels, groups, users, webhook};

pub fn is_service_message_for_transport(message: &Value) -> bool {
    [
        "new_chat_members",
        "left_chat_member",
        "new_chat_title",
        "new_chat_photo",
        "delete_chat_photo",
        "group_chat_created",
        "supergroup_chat_created",
        "channel_chat_created",
        "message_auto_delete_timer_changed",
        "pinned_message",
        "forum_topic_created",
        "forum_topic_edited",
        "forum_topic_closed",
        "forum_topic_reopened",
        "general_forum_topic_hidden",
        "general_forum_topic_unhidden",
        "write_access_allowed",
        "users_shared",
        "chat_shared",
        "giveaway_created",
        "video_chat_started",
        "video_chat_ended",
        "video_chat_participants_invited",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

pub fn message_has_transportable_content(message: &Value) -> bool {
    [
        "text",
        "photo",
        "video",
        "audio",
        "voice",
        "document",
        "animation",
        "video_note",
        "sticker",
        "poll",
        "dice",
        "game",
        "contact",
        "location",
        "venue",
        "invoice",
        "paid_media",
    ]
    .iter()
    .any(|key| message.get(*key).is_some())
}

pub fn copy_message_internal(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    to_chat_id_value: &Value,
    source_message_id: i64,
    message_thread_id: Option<i64>,
    caption_override: Option<String>,
    caption_entities_override: Option<Value>,
    remove_caption: bool,
    show_caption_above_media: Option<bool>,
    reply_markup_override: Option<Value>,
    protect_content: Option<bool>,
    reply_to_message_id_override: Option<i64>,
    sender_chat_override: Option<Value>,
    is_automatic_forward_override: Option<bool>,
    linked_discussion_context: Option<LinkedDiscussionTransportContext>,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_message = resolve_source_message_for_transport(
        conn,
        bot,
        from_chat_id_value,
        source_message_id,
        skip_source_membership_check,
    )?;

    let send_kind = send_kind_from_transport_source_message(&source_message);
    let (destination_chat_key, destination_chat) =
        chats::resolve_bot_outbound_chat(conn, bot.id, to_chat_id_value, send_kind)?;
    let sender_user = users::resolve_transport_sender_user(
        conn,
        bot,
        &destination_chat_key,
        &destination_chat,
        send_kind,
    )?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        conn,
        bot.id,
        &destination_chat_key,
        message_thread_id,
    )?;

    let normalized_reply_markup = match reply_markup_override {
        Some(markup) => {
            handle_reply_markup_state(conn, bot.id, &destination_chat_key, Some(&markup))?
        }
        None => None,
    };
    let reply_to_message_value = reply_to_message_id_override
        .map(|reply_id| load_reply_message_for_chat(conn, bot, &destination_chat_key, reply_id))
        .transpose()?;

    let mut message_value = source_message;
    let source_has_media = message_has_media(&message_value);
    let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("copied message payload is invalid"))?;
    object.remove("forward_origin");
    object.remove("is_automatic_forward");
    object.remove("reply_to_message");
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");
    object.remove("sender_chat");
    object.insert("from".to_string(), sender_user_value);

    if let Some(sender_chat_value) = sender_chat_override {
        object.insert("sender_chat".to_string(), sender_chat_value);
    }
    if let Some(is_automatic_forward) = is_automatic_forward_override {
        object.insert(
            "is_automatic_forward".to_string(),
            Value::Bool(is_automatic_forward),
        );
    }

    if source_has_media {
        if remove_caption {
            object.remove("caption");
            object.remove("caption_entities");
        } else if let Some(caption) = caption_override {
            object.insert("caption".to_string(), Value::String(caption));
            if let Some(entities) = caption_entities_override {
                object.insert("caption_entities".to_string(), entities);
            } else {
                object.remove("caption_entities");
            }
        }

        if let Some(show_above) = show_caption_above_media {
            object.insert("show_caption_above_media".to_string(), Value::Bool(show_above));
        }
    }

    if let Some(markup) = normalized_reply_markup {
        object.insert("reply_markup".to_string(), markup);
    }
    if let Some(reply_value) = reply_to_message_value {
        object.insert("reply_to_message".to_string(), reply_value);
    }

    if let Some(thread_id) = resolved_thread_id {
        object.insert("message_thread_id".to_string(), Value::from(thread_id));
        object.insert("is_topic_message".to_string(), Value::Bool(true));
    } else {
        object.remove("message_thread_id");
        object.remove("is_topic_message");
    }

    if let Some(should_protect) = protect_content {
        object.insert(
            "has_protected_content".to_string(),
            Value::Bool(should_protect),
        );
    }

    persist_transported_message(
        state,
        conn,
        token,
        bot,
        &destination_chat_key,
        &destination_chat,
        &sender_user,
        message_value,
        linked_discussion_context.as_ref(),
    )
}

pub fn resolve_source_message_for_transport(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    source_message_id: i64,
    skip_source_membership_check: bool,
) -> Result<Value, ApiError> {
    let source_chat_key = value_to_chat_key(from_chat_id_value)?;
    ensure_chat(conn, &source_chat_key)?;

    if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot.id, &source_chat_key)? {
        if sim_chat.chat_type != "private" && !skip_source_membership_check {
            chats::ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, bot.id)?;
            if let Some(actor_user_id) = current_request_actor_user_id() {
                if actor_user_id != bot.id {
                    chats::ensure_sender_is_chat_member(conn, bot.id, &source_chat_key, actor_user_id)?;
                }
            }
        }
    }

    let source_exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, &source_chat_key, source_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if source_exists.is_none() {
        return Err(ApiError::bad_request("source message was not found"));
    }

    let source_message = load_message_value(conn, bot, source_message_id)?;

    if is_service_message_for_transport(&source_message) {
        return Err(ApiError::bad_request(
            "service messages can't be forwarded or copied",
        ));
    }

    if !message_has_transportable_content(&source_message) {
        return Err(ApiError::bad_request(
            "message content can't be forwarded or copied",
        ));
    }

    if source_message
        .get("has_protected_content")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        return Err(ApiError::bad_request(
            "message has protected content and can't be forwarded or copied",
        ));
    }

    Ok(source_message)
}

pub fn send_kind_from_transport_source_message(message: &Value) -> ChatSendKind {
    if message.get("text").is_some() {
        return ChatSendKind::Text;
    }
    if message.get("photo").is_some() {
        return ChatSendKind::Photo;
    }
    if message.get("video").is_some() {
        return ChatSendKind::Video;
    }
    if message.get("audio").is_some() {
        return ChatSendKind::Audio;
    }
    if message.get("voice").is_some() {
        return ChatSendKind::Voice;
    }
    if message.get("document").is_some() {
        return ChatSendKind::Document;
    }
    if message.get("video_note").is_some() {
        return ChatSendKind::VideoNote;
    }
    if message.get("poll").is_some() {
        return ChatSendKind::Poll;
    }
    if message.get("invoice").is_some() {
        return ChatSendKind::Invoice;
    }

    ChatSendKind::Other
}

pub fn send_sim_user_payload_message(
    state: &Data<AppState>,
    token: &str,
    chat_id: Option<i64>,
    message_thread_id: Option<i64>,
    direct_messages_topic_id: Option<i64>,
    user_id: Option<i64>,
    first_name: Option<String>,
    username: Option<String>,
    sender_chat_id: Option<i64>,
    business_connection_id: Option<String>,
    payload: SimUserPayload,
    text: Option<String>,
    entities: Option<Value>,
    reply_to_message_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(&mut conn, user_id, first_name, username)?;
    let send_kind = match &payload {
        SimUserPayload::Dice(_) | SimUserPayload::Game(_) | SimUserPayload::Contact(_) | SimUserPayload::Location(_) | SimUserPayload::Venue(_) => ChatSendKind::Other,
    };

    let resolved_chat_id = chat_id.unwrap_or(user.id);
    let sim_chat = chats::resolve_sim_chat_for_user_message(&mut conn, bot.id, resolved_chat_id, &user)?;
    let is_direct_messages = channels::is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        chats::ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            send_kind,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }

        let (topic_id, topic_value, forced_sender_chat) = channels::resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }

        let sender_chat = chats::resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            sender_chat_id,
            send_kind,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = groups::resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            message_thread_id,
        )?;
    }

    let business_connection_record = business::normalize_business_connection_id(business_connection_id.as_deref())
        .map(|connection_id| business::load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        channels::direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, text.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = channels::load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            channels::upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    let from = users::build_user_from_sim_record(&user, false);
    let chat = chats::chat_from_sim_record(&sim_chat, &user);

    let mut message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
    });

    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }

    match payload {
        SimUserPayload::Dice(dice) => {
            message_json["dice"] = serde_json::to_value(dice).map_err(ApiError::internal)?;
        }
        SimUserPayload::Game(game) => {
            message_json["game"] = serde_json::to_value(game).map_err(ApiError::internal)?;
        }
        SimUserPayload::Contact(contact) => {
            message_json["contact"] = serde_json::to_value(contact).map_err(ApiError::internal)?;
        }
        SimUserPayload::Location(location) => {
            message_json["location"] = serde_json::to_value(location).map_err(ApiError::internal)?;
        }
        SimUserPayload::Venue(venue) => {
            message_json["venue"] = serde_json::to_value(venue).map_err(ApiError::internal)?;
        }
    }
    if let Some(t) = text {
        message_json["text"] = Value::String(t);
    }
    if let Some(e) = entities {
        message_json["entities"] = e;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(reply_id) = reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        channels::enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        channels::map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            reply_to_message_id,
        )?;
        channels::enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        bot::should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = channels::is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            reply_to_message_id,
        )?;
    }
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
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
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
    if bot_visible {
        webhook::dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
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
    let parsed_entities = merge_auto_message_entities(&parsed_text, parsed_entities);
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(
        &mut conn,
        body.user_id,
        body.first_name,
        body.username,
    )?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let sim_chat = chats::resolve_sim_chat_for_user_message(&mut conn, bot.id, chat_id, &user)?;
    let is_direct_messages = channels::is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        chats::ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            ChatSendKind::Text,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if body.message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if body.sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }

        let (topic_id, topic_value, forced_sender_chat) = channels::resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if body.direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }
        let sender_chat = chats::resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.sender_chat_id,
            ChatSendKind::Text,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = groups::resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            body.message_thread_id,
        )?;
    }

    let business_connection_record = business::normalize_business_connection_id(body.business_connection_id.as_deref())
        .map(|connection_id| business::load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let mut managed_bot_created: Option<ManagedBotCreated> = None;
    let mut managed_bot_update: Option<ManagedBotUpdated> = None;
    if let Some(managed_bot_request) = body.managed_bot_request.as_ref() {
        if managed_bot_request.request_id <= 0 {
            return Err(ApiError::bad_request("request_managed_bot.request_id is invalid"));
        }

        if is_direct_messages || sim_chat.chat_type != "private" {
            return Err(ApiError::bad_request(
                "request_managed_bot is available only in private chats",
            ));
        }

        bot::ensure_sim_managed_bots_storage(&mut conn)?;
        let managed_bot = bot::ensure_managed_bot_record(
            &mut conn,
            bot.id,
            user.id,
            managed_bot_request.suggested_name.as_deref(),
            managed_bot_request.suggested_username.as_deref(),
        )?;

        let managed_bot_user = bot::managed_bot_user_from_record(&managed_bot);
        managed_bot_created = Some(ManagedBotCreated {
            bot: managed_bot_user.clone(),
        });
        managed_bot_update = Some(ManagedBotUpdated {
            user: bot::build_user_with_manage_bots(&user),
            bot: managed_bot_user,
        });
    }

    let mut suggested_post_parameters = body.suggested_post_parameters.clone();
    let mut suggested_post_info: Option<SuggestedPostInfo> = None;
    if let Some(parameters) = suggested_post_parameters.as_mut() {
        if !is_direct_messages {
            return Err(ApiError::bad_request(
                "suggested_post_parameters is available only in channel direct messages chats",
            ));
        }

        let now = Utc::now().timestamp();

        if let Some(price) = parameters.price.as_ref() {
            let normalized_currency = price.currency.trim().to_ascii_uppercase();
            let normalized_amount = price.amount;

            match normalized_currency.as_str() {
                "XTR" => {
                    if !(5..=100_000).contains(&normalized_amount) {
                        return Err(ApiError::bad_request(
                            "suggested post XTR amount must be between 5 and 100000",
                        ));
                    }
                }
                "TON" => {
                    if !(10_000_000..=10_000_000_000_000).contains(&normalized_amount) {
                        return Err(ApiError::bad_request(
                            "suggested post TON amount must be between 10000000 and 10000000000000",
                        ));
                    }
                }
                _ => {
                    return Err(ApiError::bad_request(
                        "suggested post price currency must be XTR or TON",
                    ));
                }
            }

            parameters.price = Some(SuggestedPostPrice {
                currency: normalized_currency,
                amount: normalized_amount,
            });
        }

        if let Some(send_date) = parameters.send_date {
            let delta = send_date - now;
            if !(300..=2_678_400).contains(&delta) {
                return Err(ApiError::bad_request(
                    "suggested post send_date must be between 5 minutes and 30 days in the future",
                ));
            }
        }

        suggested_post_info = Some(SuggestedPostInfo {
            state: "pending".to_string(),
            price: parameters.price.clone(),
            send_date: parameters.send_date,
        });
    }

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        channels::direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, parsed_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = channels::load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            channels::upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    if let Some(info) = suggested_post_info.as_ref() {
        channels::ensure_sim_suggested_posts_storage(&mut conn)?;
        channels::upsert_suggested_post_state(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            "pending",
            info.send_date,
            None,
            now,
        )?;
    }

    let from = users::build_user_from_sim_record(&user, false);
    let chat = chats::chat_from_sim_record(&sim_chat, &user);

    let message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
        "text": parsed_text,
    });

    let mut message_json = message_json;
    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        channels::enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }
    if let Some(entities) = parsed_entities {
        message_json["entities"] = entities;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if let Some(parameters) = suggested_post_parameters.as_ref() {
        message_json["suggested_post_parameters"] =
            serde_json::to_value(parameters).map_err(ApiError::internal)?;
    }
    if let Some(info) = suggested_post_info.as_ref() {
        message_json["suggested_post_info"] =
            serde_json::to_value(info).map_err(ApiError::internal)?;
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(users_shared) = body.users_shared {
        message_json["users_shared"] = serde_json::to_value(users_shared).map_err(ApiError::internal)?;
    }
    if let Some(chat_shared) = body.chat_shared {
        message_json["chat_shared"] = serde_json::to_value(chat_shared).map_err(ApiError::internal)?;
    }
    if let Some(web_app_data) = body.web_app_data {
        message_json["web_app_data"] = serde_json::to_value(web_app_data).map_err(ApiError::internal)?;
    }
    if let Some(created) = managed_bot_created.as_ref() {
        message_json["managed_bot_created"] =
            serde_json::to_value(created).map_err(ApiError::internal)?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        channels::map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            body.reply_to_message_id,
        )?;
        channels::enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        bot::should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = channels::is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            body.reply_to_message_id,
        )?;
    }
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
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
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
    if bot_visible {
        webhook::dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    if let Some(managed_bot) = managed_bot_update {
        let managed_update = serde_json::to_value(Update {
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
            managed_bot: Some(managed_bot),
        })
        .map_err(ApiError::internal)?;

        webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, managed_update)?;
    }

    Ok(message_value)
}

pub fn handle_sim_send_user_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimSendUserMediaRequest = parse_request(&params)?;

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document", "sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let (caption, caption_entities) = parse_optional_formatted_text(
        body.caption.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );
    let caption_entities = if let Some(caption_text) = caption.as_deref() {
        merge_auto_message_entities(caption_text, caption_entities)
    } else {
        None
    };
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let media_input = if ["sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        parse_input_file_value(&body.media, &media_kind)?
    } else {
        body.media.clone()
    };

    let file = resolve_media_file(state, token, &media_input, &media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = if media_kind == "sticker" {
        load_sticker_meta(&mut conn, bot.id, &file.file_id)?
    } else {
        None
    };

    let media_value = match media_kind.as_str() {
        "photo" => serde_json::to_value(vec![PhotoSize {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            file_size: file.file_size,
        }])
        .map_err(ApiError::internal)?,
        "video" => serde_json::to_value(Video {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            duration: 0,
            thumbnail: None,
            cover: None,
            start_timestamp: None,
            qualities: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "audio" => serde_json::to_value(Audio {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            performer: None,
            title: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
            thumbnail: None,
        })
        .map_err(ApiError::internal)?,
        "voice" => serde_json::to_value(Voice {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "document" => serde_json::to_value(Document {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("document.bin").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "animation" => serde_json::to_value(Animation {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 512,
            height: 512,
            duration: 0,
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "video_note" => serde_json::to_value(VideoNote {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            length: 384,
            duration: 0,
            thumbnail: None,
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "sticker" => {
            let format = sticker_meta
                .as_ref()
                .map(|m| m.format.as_str())
                .or_else(|| infer_sticker_format_from_file(&file))
                .unwrap_or("static");
            let is_animated = format == "animated";
            let is_video = format == "video";

            serde_json::to_value(Sticker {
                file_id: file.file_id.clone(),
                file_unique_id: file.file_unique_id.clone(),
                r#type: sticker_meta
                    .as_ref()
                    .map(|m| m.sticker_type.clone())
                    .unwrap_or_else(|| "regular".to_string()),
                width: 512,
                height: 512,
                is_animated,
                is_video,
                thumbnail: None,
                emoji: sticker_meta.as_ref().and_then(|m| m.emoji.clone()),
                set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
                premium_animation: None,
                mask_position: sticker_meta
                    .as_ref()
                    .and_then(|m| m.mask_position_json.as_ref())
                    .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
                custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
                needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
                file_size: file.file_size,
            })
            .map_err(ApiError::internal)?
        }
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let sim_chat = chats::resolve_sim_chat_for_user_message(&mut conn, bot.id, chat_id, &user)?;
    let send_kind = send_kind_from_sim_user_media_kind(media_kind.as_str());
    let is_direct_messages = channels::is_direct_messages_chat(&sim_chat);
    if !is_direct_messages && sim_chat.chat_type != "private" {
        chats::ensure_sender_can_send_in_chat(
            &mut conn,
            bot.id,
            &sim_chat.chat_key,
            user.id,
            send_kind,
        )?;
    }

    let mut sender_chat_json: Option<Value> = None;
    let resolved_thread_id: Option<i64>;
    let mut direct_messages_topic_json: Option<Value> = None;

    if is_direct_messages {
        if body.message_thread_id.is_some() {
            return Err(ApiError::bad_request(
                "message_thread_id is not supported in channel direct messages simulation",
            ));
        }
        if body.sender_chat_id.is_some() {
            return Err(ApiError::bad_request(
                "sender_chat_id is not supported in channel direct messages simulation",
            ));
        }
        let (topic_id, topic_value, forced_sender_chat) = channels::resolve_direct_messages_topic_for_sender(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.direct_messages_topic_id,
        )?;
        resolved_thread_id = Some(topic_id);
        direct_messages_topic_json = Some(topic_value);
        if let Some(forced_sender_chat) = forced_sender_chat {
            sender_chat_json = Some(serde_json::to_value(forced_sender_chat).map_err(ApiError::internal)?);
        }
    } else {
        if body.direct_messages_topic_id.is_some() {
            return Err(ApiError::bad_request(
                "direct_messages_topic_id is available only in channel direct messages chats",
            ));
        }
        let sender_chat = chats::resolve_sender_chat_for_sim_user_message(
            &mut conn,
            bot.id,
            &sim_chat,
            &user,
            body.sender_chat_id,
            send_kind,
        )?;
        sender_chat_json = sender_chat
            .as_ref()
            .map(|chat| serde_json::to_value(chat).map_err(ApiError::internal))
            .transpose()?;
        resolved_thread_id = groups::resolve_forum_message_thread_id(
            &mut conn,
            bot.id,
            &sim_chat,
            body.message_thread_id,
        )?;
    }

    let business_connection_record = business::normalize_business_connection_id(body.business_connection_id.as_deref())
        .map(|connection_id| business::load_business_connection_or_404(&mut conn, bot.id, &connection_id))
        .transpose()?;
    let business_connection_id = if let Some(record) = business_connection_record.as_ref() {
        if is_direct_messages {
            return Err(ApiError::bad_request(
                "business_connection_id is not supported in channel direct messages simulation",
            ));
        }
        if !record.is_enabled {
            return Err(ApiError::bad_request("business connection is disabled"));
        }
        if sim_chat.chat_type != "private" || sim_chat.chat_id != record.user_chat_id || user.id != record.user_id {
            return Err(ApiError::bad_request("business connection does not match chat/user"));
        }
        Some(record.connection_id.clone())
    } else {
        None
    };

    let chat_key = sim_chat.chat_key.clone();
    let direct_messages_star_count = if is_direct_messages {
        channels::direct_messages_star_count_for_chat(&mut conn, bot.id, &sim_chat)?
    } else {
        0
    };

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, user.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    if is_direct_messages {
        if let Some(topic_id) = resolved_thread_id {
            let topic_owner_user_id = channels::load_direct_messages_topic_record(&mut conn, bot.id, &chat_key, topic_id)?
                .map(|record| record.user_id)
                .unwrap_or(user.id);
            channels::upsert_direct_messages_topic(
                &mut conn,
                bot.id,
                &chat_key,
                topic_id,
                topic_owner_user_id,
                Some(message_id),
                Some(now),
            )?;
        }
    }

    let from = users::build_user_from_sim_record(&user, false);
    let chat = chats::chat_from_sim_record(&sim_chat, &user);

    let mut message_json = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": from,
    });

    if let Some(sender_chat_value) = sender_chat_json {
        message_json["sender_chat"] = sender_chat_value;
    }

    message_json[media_kind.as_str()] = media_value;
    if let Some(c) = caption {
        message_json["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        message_json["caption_entities"] = entities;
    }
    if let Some(thread_id) = resolved_thread_id {
        message_json["message_thread_id"] = Value::from(thread_id);
        message_json["is_topic_message"] = Value::Bool(true);
    }
    if let Some(direct_messages_topic) = direct_messages_topic_json {
        message_json["direct_messages_topic"] = direct_messages_topic;
    }
    if let Some(connection_id) = business_connection_id.as_ref() {
        message_json["business_connection_id"] = Value::String(connection_id.clone());
    }
    if is_direct_messages && direct_messages_star_count > 0 {
        message_json["paid_message_star_count"] = Value::from(direct_messages_star_count);
    }
    if let Some(mode) = sim_parse_mode {
        message_json["sim_parse_mode"] = Value::String(mode);
    }
    if let Some(reply_id) = body.reply_to_message_id {
        let reply_value = load_reply_message_for_chat(&mut conn, &bot, &chat_key, reply_id)?;
        message_json["reply_to_message"] = reply_value;
        channels::enrich_reply_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            reply_id,
            &mut message_json,
        )?;
    }

    let is_channel_post = sim_chat.chat_type == "channel";
    if !is_channel_post && !is_direct_messages {
        channels::map_discussion_message_to_channel_post_if_needed(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            body.reply_to_message_id,
        )?;
        channels::enrich_message_with_linked_channel_context(
            &mut conn,
            bot.id,
            &chat_key,
            message_id,
            &mut message_json,
        )?;
    }

    let message: Message = serde_json::from_value(message_json).map_err(ApiError::internal)?;
    let is_business_message = business_connection_id.is_some();
    let mut bot_visible = if is_direct_messages || is_business_message {
        true
    } else {
        bot::should_emit_user_generated_update_to_bot(
            &mut conn,
            &bot,
            &sim_chat.chat_type,
            user.id,
            &message,
        )?
    };
    if !bot_visible && !is_direct_messages && (sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup") {
        bot_visible = channels::is_reply_to_linked_discussion_root_message(
            &mut conn,
            bot.id,
            &chat_key,
            body.reply_to_message_id,
        )?;
    }
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
        "INSERT INTO updates (bot_id, update_json, bot_visible) VALUES (?1, ?2, ?3)",
        params![
            bot.id,
            serde_json::to_string(&update_stub).map_err(ApiError::internal)?,
            if bot_visible { 1 } else { 0 },
        ],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if is_direct_messages && direct_messages_star_count > 0 {
        message_value["paid_message_star_count"] = Value::from(direct_messages_star_count);
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
    if bot_visible {
        webhook::dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);
    }

    if is_channel_post {
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn handle_sim_edit_user_message_media(
    state: &Data<AppState>,
    token: &str,
    params: HashMap<String, Value>,
) -> ApiResult {
    let body: SimEditUserMediaRequest = parse_request(&params)?;
    let sim_parse_mode = normalize_sim_parse_mode(body.parse_mode.as_deref());

    let media_kind = body.media_kind.to_ascii_lowercase();
    if !["photo", "video", "audio", "voice", "document", "sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        return Err(ApiError::bad_request("unsupported media_kind"));
    }

    let caption_text = body.caption.as_ref().and_then(value_to_optional_string);
    let (caption, caption_entities) = parse_optional_formatted_text(
        caption_text.as_deref(),
        body.parse_mode.as_deref(),
        None,
    );
    let caption_entities = if let Some(caption_text) = caption.as_deref() {
        merge_auto_message_entities(caption_text, caption_entities)
    } else {
        None
    };

    let media_input = if ["sticker", "animation", "video_note"].contains(&media_kind.as_str()) {
        parse_input_file_value(&body.media, &media_kind)?
    } else {
        body.media.clone()
    };
    let file = resolve_media_file(state, token, &media_input, &media_kind)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let sticker_meta = if media_kind == "sticker" {
        load_sticker_meta(&mut conn, bot.id, &file.file_id)?
    } else {
        None
    };

    let media_value = match media_kind.as_str() {
        "photo" => serde_json::to_value(vec![PhotoSize {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            file_size: file.file_size,
        }])
        .map_err(ApiError::internal)?,
        "video" => serde_json::to_value(Video {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 1280,
            height: 720,
            duration: 0,
            thumbnail: None,
            cover: None,
            start_timestamp: None,
            qualities: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "audio" => serde_json::to_value(Audio {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            performer: None,
            title: None,
            file_name: None,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
            thumbnail: None,
        })
        .map_err(ApiError::internal)?,
        "voice" => serde_json::to_value(Voice {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            duration: 0,
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "document" => serde_json::to_value(Document {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("document.bin").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "animation" => serde_json::to_value(Animation {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            width: 512,
            height: 512,
            duration: 0,
            thumbnail: None,
            file_name: Some(file.file_path.split('/').last().unwrap_or("animation.mp4").to_string()),
            mime_type: file.mime_type.clone(),
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "video_note" => serde_json::to_value(VideoNote {
            file_id: file.file_id.clone(),
            file_unique_id: file.file_unique_id.clone(),
            length: 384,
            duration: 0,
            thumbnail: None,
            file_size: file.file_size,
        })
        .map_err(ApiError::internal)?,
        "sticker" => {
            let format = sticker_meta
                .as_ref()
                .map(|m| m.format.as_str())
                .or_else(|| infer_sticker_format_from_file(&file))
                .unwrap_or("static");
            let is_animated = format == "animated";
            let is_video = format == "video";

            serde_json::to_value(Sticker {
                file_id: file.file_id.clone(),
                file_unique_id: file.file_unique_id.clone(),
                r#type: sticker_meta
                    .as_ref()
                    .map(|m| m.sticker_type.clone())
                    .unwrap_or_else(|| "regular".to_string()),
                width: 512,
                height: 512,
                is_animated,
                is_video,
                thumbnail: None,
                emoji: sticker_meta.as_ref().and_then(|m| m.emoji.clone()),
                set_name: sticker_meta.as_ref().and_then(|m| m.set_name.clone()),
                premium_animation: None,
                mask_position: sticker_meta
                    .as_ref()
                    .and_then(|m| m.mask_position_json.as_ref())
                    .and_then(|raw| serde_json::from_str::<MaskPosition>(raw).ok()),
                custom_emoji_id: sticker_meta.as_ref().and_then(|m| m.custom_emoji_id.clone()),
                needs_repainting: sticker_meta.as_ref().map(|m| m.needs_repainting),
                file_size: file.file_size,
            })
            .map_err(ApiError::internal)?
        }
        _ => return Err(ApiError::bad_request("unsupported media_kind")),
    };

    let chat_key = body.chat_id.to_string();

    let actor_user_id = current_request_actor_user_id()
        .ok_or_else(|| ApiError::bad_request("actor user is required for user media edit"))?;

    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to edit was not found"));
    };

    if owner_user_id != actor_user_id {
        return Err(ApiError::bad_request("message can't be edited"));
    }

    let mut edited_message = load_message_value(&mut conn, &bot, body.message_id)?;

    for key in ["photo", "video", "audio", "voice", "document", "animation", "video_note", "sticker"] {
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
    if let Some(mode) = sim_parse_mode {
        edited_message["sim_parse_mode"] = Value::String(mode);
    } else {
        edited_message
            .as_object_mut()
            .map(|obj| obj.remove("sim_parse_mode"));
    }
    edited_message.as_object_mut().map(|obj| obj.remove("text"));
    let is_channel_post = edited_message
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");

    let update_stub = Update {
        update_id: 0,
        message: None,
        edited_message: if is_channel_post {
            None
        } else {
            serde_json::from_value(edited_message.clone()).ok()
        },
        channel_post: None,
        edited_channel_post: if is_channel_post {
            serde_json::from_value(edited_message.clone()).ok()
        } else {
            None
        },
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
        update_value["edited_channel_post"] = edited_message.clone();
    } else {
        update_value["edited_message"] = edited_message.clone();
    }

    channels::enrich_channel_post_payloads(&mut conn, bot.id, &mut update_value)?;
    if is_channel_post {
        if let Some(enriched_message) = update_value.get("edited_channel_post").cloned() {
            edited_message = enriched_message;
        }
    }

    conn.execute(
        "UPDATE updates SET update_json = ?1 WHERE update_id = ?2",
        params![update_value.to_string(), update_id],
    )
    .map_err(ApiError::internal)?;

    let clean_update = strip_nulls(update_value);
    state.ws_hub.publish_json(token, &clean_update);
    webhook::dispatch_webhook_if_configured(state, &mut conn, bot.id, clean_update);

    Ok(edited_message)
}

pub fn handle_sim_send_user_dice(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserDiceRequest,
) -> ApiResult {
    let emoji = body.emoji.unwrap_or_else(|| "🎲".to_string());
    let max_value = match emoji.as_str() {
        "🎯" | "🎲" | "🏀" | "🎳" => 6,
        "⚽" | "🏐" => 5,
        "🎰" => 64,
        _ => return Err(ApiError::bad_request("unsupported dice emoji")),
    };
    let now_nanos = Utc::now().timestamp_nanos_opt().unwrap_or_default().unsigned_abs();
    let value = (now_nanos % (max_value as u64)) as i64 + 1;

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Dice(Dice { emoji, value }),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_game(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserGameRequest,
) -> ApiResult {
    if body.game_short_name.trim().is_empty() {
        return Err(ApiError::bad_request("game_short_name is empty"));
    }

    let game = Game {
        title: body.game_short_name.clone(),
        description: format!("Game {}", body.game_short_name),
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

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Game(game),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_contact(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserContactRequest,
) -> ApiResult {
    if body.phone_number.trim().is_empty() {
        return Err(ApiError::bad_request("phone_number is empty"));
    }
    if body.contact_first_name.trim().is_empty() {
        return Err(ApiError::bad_request("contact_first_name is empty"));
    }

    let contact = Contact {
        phone_number: body.phone_number,
        first_name: body.contact_first_name,
        last_name: body.contact_last_name,
        user_id: None,
        vcard: body.vcard,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Contact(contact),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_location(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserLocationRequest,
) -> ApiResult {
    let location = Location {
        latitude: body.latitude,
        longitude: body.longitude,
        horizontal_accuracy: body.horizontal_accuracy,
        live_period: body.live_period,
        heading: body.heading,
        proximity_alert_radius: body.proximity_alert_radius,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Location(location),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

pub fn handle_sim_send_user_venue(
    state: &Data<AppState>,
    token: &str,
    body: SimSendUserVenueRequest,
) -> ApiResult {
    if body.title.trim().is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if body.address.trim().is_empty() {
        return Err(ApiError::bad_request("address is empty"));
    }

    let venue = Venue {
        location: Location {
            latitude: body.latitude,
            longitude: body.longitude,
            horizontal_accuracy: None,
            live_period: None,
            heading: None,
            proximity_alert_radius: None,
        },
        title: body.title,
        address: body.address,
        foursquare_id: body.foursquare_id,
        foursquare_type: body.foursquare_type,
        google_place_id: body.google_place_id,
        google_place_type: body.google_place_type,
    };

    send_sim_user_payload_message(
        state,
        token,
        body.chat_id,
        body.message_thread_id,
        body.direct_messages_topic_id,
        body.user_id,
        body.first_name,
        body.username,
        body.sender_chat_id,
        None,
        SimUserPayload::Venue(venue),
        None,
        None,
        body.reply_to_message_id,
        None,
    )
}

fn build_forward_origin_from_source_message(source_message: &Value) -> Value {
    let source_date = source_message
        .get("date")
        .and_then(Value::as_i64)
        .unwrap_or_else(|| Utc::now().timestamp());

    if let Some(source_chat) = source_message.get("chat") {
        let chat_type = source_chat
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if chat_type == "channel" {
            return json!({
                "type": "channel",
                "date": source_date,
                "chat": source_chat,
                "message_id": source_message.get("message_id").and_then(Value::as_i64).unwrap_or(0),
                "author_signature": source_message.get("author_signature").cloned().unwrap_or(Value::Null),
            });
        }
    }

    if let Some(sender_chat) = source_message.get("sender_chat") {
        return json!({
            "type": "chat",
            "date": source_date,
            "sender_chat": sender_chat,
            "author_signature": source_message.get("author_signature").cloned().unwrap_or(Value::Null),
        });
    }

    if let Some(sender_user) = source_message.get("from") {
        return json!({
            "type": "user",
            "date": source_date,
            "sender_user": sender_user,
        });
    }

    json!({
        "type": "hidden_user",
        "date": source_date,
        "sender_user_name": source_message
            .get("author_signature")
            .and_then(Value::as_str)
            .unwrap_or("Hidden User"),
    })
}

pub fn resolve_game_target_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    request_chat_id: Option<i64>,
    request_message_id: Option<i64>,
    inline_message_id: Option<&str>,
) -> Result<(String, i64), ApiError> {
    if let Some(inline_id) = inline_message_id {
        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT chat_key, message_id FROM inline_messages WHERE inline_message_id = ?1 AND bot_id = ?2",
                params![inline_id, bot_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;
        return row.ok_or_else(|| ApiError::not_found("inline message not found"));
    }

    let chat_id = request_chat_id.ok_or_else(|| ApiError::bad_request("chat_id is required"))?;
    let message_id = request_message_id.ok_or_else(|| ApiError::bad_request("message_id is required"))?;
    Ok((chat_id.to_string(), message_id))
}

pub fn handle_auto_close_due_polls(state: &Data<AppState>) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let mut conn = lock_db(state)?;

    let due_rows: Vec<(
        i64,
        String,
        String,
        String,
        String,
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
    )> = {
        let mut stmt = conn
            .prepare(
                "SELECT p.bot_id, b.token, p.id, p.question, p.options_json, p.total_voter_count, p.is_anonymous, p.poll_type,
                        p.allows_multiple_answers, p.allows_revoting, p.correct_option_id, p.correct_option_ids_json,
                        p.explanation, p.description, p.open_period, p.close_date,
                        m.question_entities_json, m.explanation_entities_json, m.description_entities_json
                 FROM polls p
                 INNER JOIN bots b ON b.id = p.bot_id
                 LEFT JOIN poll_metadata m ON m.poll_id = p.id
                 WHERE p.is_closed = 0
                 AND (
                    (p.close_date IS NOT NULL AND p.close_date <= ?1)
                    OR
                    (p.open_period IS NOT NULL AND p.created_at + p.open_period <= ?1)
                 )",
            )
            .map_err(ApiError::internal)?;

        let rows = stmt
            .query_map(params![now], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                    r.get::<_, i64>(5)?,
                    r.get::<_, i64>(6)?,
                    r.get::<_, String>(7)?,
                    r.get::<_, i64>(8)?,
                    r.get::<_, i64>(9)?,
                    r.get::<_, Option<i64>>(10)?,
                    r.get::<_, Option<String>>(11)?,
                    r.get::<_, Option<String>>(12)?,
                    r.get::<_, Option<String>>(13)?,
                    r.get::<_, Option<i64>>(14)?,
                    r.get::<_, Option<i64>>(15)?,
                    r.get::<_, Option<String>>(16)?,
                    r.get::<_, Option<String>>(17)?,
                    r.get::<_, Option<String>>(18)?,
                ))
            })
            .map_err(ApiError::internal)?;

        let mut collected = Vec::new();
        for row in rows {
            collected.push(row.map_err(ApiError::internal)?);
        }
        collected
    };

    for (
        bot_id,
        token,
        poll_id,
        question,
        options_json,
        total_voter_count,
        is_anonymous,
        poll_type,
        allows_multiple_answers,
        allows_revoting,
        correct_option_id,
        correct_option_ids_json,
        explanation,
        description,
        open_period,
        close_date,
        question_entities_json,
        explanation_entities_json,
        description_entities_json,
    ) in due_rows
    {
        conn.execute(
            "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2 AND is_closed = 0",
            params![poll_id, bot_id],
        )
        .map_err(ApiError::internal)?;

        let options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
        let correct_option_ids = correct_option_ids_json
            .as_deref()
            .and_then(|raw| serde_json::from_str::<Vec<i64>>(raw).ok())
            .or_else(|| correct_option_id.map(|id| vec![id]));
        let poll = Poll {
            id: poll_id,
            question,
            question_entities: question_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            options,
            total_voter_count,
            is_closed: true,
            is_anonymous: is_anonymous == 1,
            r#type: poll_type,
            allows_multiple_answers: allows_multiple_answers == 1,
            allows_revoting: allows_revoting == 1,
            correct_option_ids,
            explanation,
            explanation_entities: explanation_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            open_period,
            close_date,
            description,
            description_entities: description_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
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
            deleted_business_messages: None,
            message_reaction: None,
            message_reaction_count: None,
            inline_query: None,
            chosen_inline_result: None,
            callback_query: None,
            shipping_query: None,
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: Some(poll),
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
        managed_bot: None,
        })
        .map_err(ApiError::internal)?;

        webhook::persist_and_dispatch_update(state, &mut conn, &token, bot_id, update_value)?;
    }

    Ok(())
}

pub fn forward_message_internal(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    from_chat_id_value: &Value,
    to_chat_id_value: &Value,
    source_message_id: i64,
    message_thread_id: Option<i64>,
    protect_content: Option<bool>,
) -> Result<Value, ApiError> {
    let source_message = resolve_source_message_for_transport(
        conn,
        bot,
        from_chat_id_value,
        source_message_id,
        false,
    )?;

    let send_kind = send_kind_from_transport_source_message(&source_message);
    let (destination_chat_key, destination_chat) =
        chats::resolve_bot_outbound_chat(conn, bot.id, to_chat_id_value, send_kind)?;
    let sender_user = users::resolve_transport_sender_user(
        conn,
        bot,
        &destination_chat_key,
        &destination_chat,
        send_kind,
    )?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        conn,
        bot.id,
        &destination_chat_key,
        message_thread_id,
    )?;

    let mut message_value = source_message;
    let sender_user_value = serde_json::to_value(&sender_user).map_err(ApiError::internal)?;
    let forward_origin = message_value
        .get("forward_origin")
        .cloned()
        .unwrap_or_else(|| build_forward_origin_from_source_message(&message_value));

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("forwarded message payload is invalid"))?;
    object.remove("reply_to_message");
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");
    object.remove("sender_chat");
    object.remove("is_automatic_forward");
    object.insert("from".to_string(), sender_user_value);
    object.insert("forward_origin".to_string(), forward_origin);

    if let Some(thread_id) = resolved_thread_id {
        object.insert("message_thread_id".to_string(), Value::from(thread_id));
        object.insert("is_topic_message".to_string(), Value::Bool(true));
    } else {
        object.remove("message_thread_id");
        object.remove("is_topic_message");
    }

    if let Some(should_protect) = protect_content {
        object.insert(
            "has_protected_content".to_string(),
            Value::Bool(should_protect),
        );
    }

    persist_transported_message(
        state,
        conn,
        token,
        bot,
        &destination_chat_key,
        &destination_chat,
        &sender_user,
        message_value,
        None,
    )
}

pub fn handle_sim_purchase_paid_media(
    state: &Data<AppState>,
    token: &str,
    body: SimPurchasePaidMediaRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;
    if exists.is_none() {
        return Err(ApiError::not_found("paid media message not found"));
    }

    let message_value = load_message_value(&mut conn, &bot, body.message_id)?;
    let is_paid_post = message_value
        .get("is_paid_post")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    if !is_paid_post {
        return Err(ApiError::bad_request("message is not a paid media post"));
    }

    let paid_star_count = message_value
        .get("paid_star_count")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    if paid_star_count <= 0 {
        return Err(ApiError::bad_request("paid media star count is invalid"));
    }

    let paid_media_payload = body
        .paid_media_payload
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
        .or_else(|| {
            message_value
                .get("paid_media_payload")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("paid_media:{}:{}", body.chat_id, body.message_id));

    let now = Utc::now().timestamp();
    let purchase_charge_id = format!("paid_media_purchase:{}:{}", paid_media_payload, user.id);
    let already_purchased = match conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'paid_media_purchase')",
        params![
            format!("paid_media_purchase_{}", generate_telegram_numeric_id()),
            bot.id,
            user.id,
            purchase_charge_id,
            paid_star_count,
            now,
        ],
    ) {
        Ok(_) => false,
        Err(rusqlite::Error::SqliteFailure(err, _))
            if err.code == rusqlite::ErrorCode::ConstraintViolation =>
        {
            true
        }
        Err(error) => return Err(ApiError::internal(error)),
    };

    if !already_purchased {
        let purchaser = User {
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
            can_manage_bots: None,
        };

        let purchased_update = serde_json::to_value(Update {
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
            purchased_paid_media: Some(PaidMediaPurchased {
                from: purchaser,
                paid_media_payload: paid_media_payload.clone(),
            }),
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
        webhook::persist_and_dispatch_update(state, &mut conn, token, bot.id, purchased_update)?;
    }

    Ok(json!({
        "status": "success",
        "paid_media_payload": paid_media_payload,
        "star_count": paid_star_count,
        "already_purchased": already_purchased,
    }))
}

pub fn send_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    send_media_message_with_group(
        state,
        token,
        chat_id_value,
        caption,
        caption_entities,
        reply_markup,
        media_field,
        media_payload,
        None,
        message_thread_id,
        sim_parse_mode,
    )
}

pub fn send_paid_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    paid_media_payload: Value,
    paid_star_count: i64,
    show_caption_above_media: Option<bool>,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat) = chats::resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, ChatSendKind::Other)?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = chats::resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        ChatSendKind::Other,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let paid_media_payload_id = format!("paid_media:{}:{}", chat.id, message_id);

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender,
        "is_paid_post": true,
        "paid_media_payload": paid_media_payload_id,
        "paid_media": paid_media_payload,
    });

    if paid_star_count > 0 {
        base["paid_star_count"] = Value::from(paid_star_count);
    }
    if let Some(c) = caption {
        base["caption"] = Value::String(c);
    }
    if let Some(entities) = caption_entities {
        base["caption_entities"] = entities;
    }
    if let Some(show_above) = show_caption_above_media {
        base["show_caption_above_media"] = Value::Bool(show_above);
    }
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }
    if let Some(mode) = sim_parse_mode {
        base["sim_parse_mode"] = Value::String(mode);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
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
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn send_payload_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    text: Option<String>,
    entities: Option<Value>,
    reply_markup: Option<Value>,
    payload_field: &str,
    payload_value: Value,
    message_thread_id: Option<i64>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let send_kind = send_kind_from_payload_field(payload_field);
    let (chat_key, chat) = chats::resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, send_kind)?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = chats::resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        send_kind,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, text.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender
    });

    base[payload_field] = payload_value;
    if let Some(t) = text {
        base["text"] = Value::String(t);
    }
    if let Some(e) = entities {
        base["entities"] = e;
    }
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
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
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn send_media_message_with_group(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
    media_group_id: Option<&str>,
    message_thread_id: Option<i64>,
    sim_parse_mode: Option<String>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let send_kind = send_kind_from_media_field(media_field);
    let (chat_key, chat) = chats::resolve_bot_outbound_chat(&mut conn, bot.id, chat_id_value, send_kind)?;
    let resolved_thread_id = groups::resolve_forum_message_thread_for_chat_key(
        &mut conn,
        bot.id,
        &chat_key,
        message_thread_id,
    )?;
    let sender = chats::resolve_sender_for_bot_outbound_chat(
        &mut conn,
        &bot,
        &chat_key,
        &chat,
        send_kind,
    )?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, &chat_key, sender.id, caption.clone().unwrap_or_default(), now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let mut base = json!({
        "message_id": message_id,
        "date": now,
        "chat": chat,
        "from": sender
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
    if let Some(thread_id) = resolved_thread_id {
        base["message_thread_id"] = Value::from(thread_id);
        base["is_topic_message"] = Value::Bool(true);
    }
    if let Some(mode) = sim_parse_mode {
        base["sim_parse_mode"] = Value::String(mode);
    }

    let message: Message = serde_json::from_value(base).map_err(ApiError::internal)?;
    let is_channel_post = chat.r#type == "channel";
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

    let update_stub = Update {
        update_id: 0,
        message: if is_channel_post { None } else { Some(message.clone()) },
        edited_message: None,
        channel_post: if is_channel_post { Some(message) } else { None },
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
    };

    conn.execute(
        "INSERT INTO updates (bot_id, update_json) VALUES (?1, ?2)",
        params![bot.id, serde_json::to_string(&update_stub).map_err(ApiError::internal)?],
    )
    .map_err(ApiError::internal)?;

    let update_id = conn.last_insert_rowid();
    let mut update_value = serde_json::to_value(update_stub).map_err(ApiError::internal)?;
    update_value["update_id"] = json!(update_id);
    if is_channel_post {
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
        channels::forward_channel_post_to_linked_discussion_best_effort(
            state,
            &mut conn,
            token,
            &bot,
            &chat_key,
            &message_value,
        );
    }

    Ok(message_value)
}

pub fn handle_reply_markup_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    reply_markup: Option<&Value>,
) -> Result<Option<Value>, ApiError> {
    let Some(markup_value) = reply_markup else {
        return Ok(None);
    };

    if markup_value.get("keyboard").is_some() {
        let normalized_markup_value = normalize_legacy_reply_keyboard_markup(markup_value);
        let parsed: ReplyKeyboardMarkup = serde_json::from_value(normalized_markup_value)
            .map_err(|_| ApiError::bad_request("reply_markup keyboard is invalid"))?;

        if parsed.keyboard.is_empty() {
            return Err(ApiError::bad_request("reply_markup keyboard must have at least one row"));
        }

        if parsed
            .keyboard
            .iter()
            .any(|row| row.is_empty() || row.iter().any(|button| button.text.trim().is_empty()))
        {
            return Err(ApiError::bad_request("keyboard rows/buttons must not be empty"));
        }

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        let now = Utc::now().timestamp();
        conn.execute(
            "INSERT INTO chat_reply_keyboards (bot_id, chat_key, markup_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(bot_id, chat_key)
             DO UPDATE SET markup_json = excluded.markup_json, updated_at = excluded.updated_at",
            params![bot_id, chat_key, normalized.to_string(), now],
        )
        .map_err(ApiError::internal)?;

        return Ok(Some(normalized));
    }

    if markup_value.get("remove_keyboard").is_some() {
        let parsed: ReplyKeyboardRemove = serde_json::from_value(markup_value.clone())
            .map_err(|_| ApiError::bad_request("reply_markup remove_keyboard is invalid"))?;

        if !parsed.remove_keyboard {
            return Err(ApiError::bad_request("remove_keyboard must be true"));
        }

        conn.execute(
            "DELETE FROM chat_reply_keyboards WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot_id, chat_key],
        )
        .map_err(ApiError::internal)?;

        let normalized = serde_json::to_value(parsed).map_err(ApiError::internal)?;
        return Ok(Some(normalized));
    }

    Ok(Some(markup_value.clone()))
}

pub fn normalize_legacy_reply_keyboard_markup(markup_value: &Value) -> Value {
    let mut normalized = markup_value.clone();
    let Some(rows) = normalized
        .get_mut("keyboard")
        .and_then(Value::as_array_mut)
    else {
        return normalized;
    };

    for row in rows {
        let Some(buttons) = row.as_array_mut() else {
            continue;
        };

        for button in buttons {
            let Some(button_obj) = button.as_object_mut() else {
                continue;
            };

            if button_obj.contains_key("request_users") {
                continue;
            }

            let Some(legacy_request_user) = button_obj.get("request_user") else {
                continue;
            };

            if let Some(request_users) = normalize_legacy_request_user_payload(legacy_request_user)
            {
                button_obj.insert("request_users".to_string(), request_users);
            }
        }
    }

    normalized
}

pub fn normalize_legacy_request_user_payload(legacy_request_user: &Value) -> Option<Value> {
    let legacy = legacy_request_user.as_object()?;
    let request_id = legacy.get("request_id").and_then(|raw| {
        raw.as_i64()
            .or_else(|| raw.as_str().and_then(|v| v.trim().parse::<i64>().ok()))
    })?;

    let mut normalized = Map::new();
    normalized.insert("request_id".to_string(), Value::from(request_id));
    normalized.insert("max_quantity".to_string(), Value::from(10));

    let mappings = [
        ("user_is_bot", "user_is_bot"),
        ("user_is_premium", "user_is_premium"),
        ("request_name", "request_name"),
        ("request_username", "request_username"),
        ("request_photo", "request_photo"),
    ];

    for (legacy_key, target_key) in mappings {
        if let Some(value) = legacy
            .get(legacy_key)
            .and_then(value_to_optional_bool_loose)
        {
            normalized.insert(target_key.to_string(), Value::Bool(value));
        }
    }

    Some(Value::Object(normalized))
}

pub fn load_reply_message_for_chat(
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    chat_key: &str,
    reply_message_id: i64,
) -> Result<Value, ApiError> {
    let reply_chat_key: Option<String> = conn
        .query_row(
            "SELECT chat_key FROM messages WHERE bot_id = ?1 AND message_id = ?2",
            params![bot.id, reply_message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(reply_chat_key) = reply_chat_key else {
        return Err(ApiError::not_found("reply message not found"));
    };

    if reply_chat_key != chat_key {
        return Err(ApiError::bad_request("reply message not found in this chat"));
    }

    load_message_value(conn, bot, reply_message_id)
}

pub fn message_has_media(message: &Value) -> bool {
    ["photo", "video", "audio", "voice", "document", "animation", "video_note"]
        .iter()
        .any(|key| message.get(*key).is_some())
}

pub fn persist_transported_message(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot: &crate::database::BotInfoRecord,
    destination_chat_key: &str,
    destination_chat: &Chat,
    sender_user: &User,
    mut message_value: Value,
    linked_discussion_context: Option<&LinkedDiscussionTransportContext>,
) -> Result<Value, ApiError> {
    let now = Utc::now().timestamp();
    let persisted_text = message_value
        .get("text")
        .and_then(Value::as_str)
        .or_else(|| message_value.get("caption").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, destination_chat_key, sender_user.id, persisted_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let destination_chat_value = serde_json::to_value(destination_chat).map_err(ApiError::internal)?;
    let sender_user_value = serde_json::to_value(sender_user).map_err(ApiError::internal)?;

    let object = message_value
        .as_object_mut()
        .ok_or_else(|| ApiError::internal("transported message payload is invalid"))?;
    object.insert("message_id".to_string(), Value::from(message_id));
    object.insert("date".to_string(), Value::from(now));
    object.insert("chat".to_string(), destination_chat_value);
    object.insert("from".to_string(), sender_user_value);
    object.remove("edit_date");
    object.remove("views");
    object.remove("author_signature");

    if let Some(context) = linked_discussion_context {
        let discussion_root_message_id = context
            .discussion_root_message_id
            .unwrap_or(message_id);
        object.insert(
            "linked_channel_chat_id".to_string(),
            Value::String(context.channel_chat_key.clone()),
        );
        object.insert(
            "linked_channel_message_id".to_string(),
            Value::from(context.channel_message_id),
        );
        object.insert(
            "linked_discussion_root_message_id".to_string(),
            Value::from(discussion_root_message_id),
        );
    }

    let update_value = if destination_chat.r#type == "channel" {
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

    webhook::persist_and_dispatch_update(state, conn, token, bot.id, update_value)?;
    Ok(message_value)
}

pub fn load_message_value(
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
        .unwrap_or_else(|_| chats::fallback_chat_id(&chat_key));
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
            "from": {
                "id": from_user_id,
                "is_bot": is_bot,
                "first_name": first_name,
                "username": username
            }
        })
    });

    let chat = if let Some(sim_chat) = chats::load_sim_chat_record(conn, bot.id, &chat_key).ok().flatten() {
        let sender = SimUserRecord {
            id: from_user_id,
            first_name: first_name.clone(),
            username: username.clone(),
            last_name: None,
            is_premium: false,
        };
        chats::chat_from_sim_record(&sim_chat, &sender)
    } else {
        Chat {
            id: chat_id,
            r#type: "private".to_string(),
            title: None,
            username: username.clone(),
            first_name: Some(first_name.clone()),
            last_name: None,
            is_forum: None,
            is_direct_messages: None,
        }
    };

    message["message_id"] = Value::from(message_id);
    message["date"] = Value::from(date);
    message.as_object_mut().map(|obj| obj.remove("edit_date"));
    message["chat"] = serde_json::to_value(chat).map_err(ApiError::internal)?;
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

pub fn find_message_snapshot(
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
            .get("edited_channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

        if let Some(msg) = update_value
            .get("channel_post")
            .and_then(Value::as_object)
            .filter(|m| m.get("message_id").and_then(Value::as_i64) == Some(message_id))
        {
            return Some(Value::Object(msg.clone()));
        }

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

pub fn update_targets_deleted_message(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update: &Value,
) -> Result<bool, ApiError> {
    const MESSAGE_FIELDS: [&str; 6] = [
        "message",
        "edited_message",
        "channel_post",
        "edited_channel_post",
        "business_message",
        "edited_business_message",
    ];

    for field in MESSAGE_FIELDS {
        let Some(message_value) = update.get(field) else {
            continue;
        };

        let Some(message_obj) = message_value.as_object() else {
            continue;
        };

        let Some(chat_value) = message_obj
            .get("chat")
            .and_then(Value::as_object)
            .and_then(|chat_obj| chat_obj.get("id"))
        else {
            continue;
        };

        let Some(message_id) = message_obj.get("message_id").and_then(Value::as_i64) else {
            continue;
        };

        let chat_key = value_to_chat_key(chat_value)?;
        let exists: Option<i64> = conn
            .query_row(
                "SELECT 1 FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot_id, chat_key, message_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if exists.is_none() {
            return Ok(true);
        }
    }

    Ok(false)
}

pub fn resolve_edit_target(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_id: Option<Value>,
    message_id: Option<i64>,
    inline_message_id: Option<String>,
    method_name: &str,
) -> Result<(String, i64, bool), ApiError> {
    if let Some(inline_id) = inline_message_id {
        let trimmed = inline_id.trim();
        if trimmed.is_empty() {
            return Err(ApiError::bad_request("inline_message_id is empty"));
        }

        let row: Option<(String, i64)> = conn
            .query_row(
                "SELECT chat_key, message_id FROM inline_messages WHERE inline_message_id = ?1 AND bot_id = ?2",
                params![trimmed, bot_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some((chat_key, resolved_message_id)) = row {
            return Ok((chat_key, resolved_message_id, true));
        }

        return Err(ApiError::not_found(format!(
            "{} inline_message_id not found",
            method_name
        )));
    }

    let Some(chat) = chat_id else {
        return Err(ApiError::bad_request("chat_id is required"));
    };
    let Some(msg_id) = message_id else {
        return Err(ApiError::bad_request("message_id is required"));
    };

    let (chat_key, _) = chats::resolve_chat_key_and_id(conn, bot_id, &chat)?;
    Ok((chat_key, msg_id, false))
}

pub fn publish_edited_message_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    edited_message: &Value,
) -> Result<(), ApiError> {
    let mut edited_with_timestamp = edited_message.clone();
    edited_with_timestamp["edit_date"] = Value::from(Utc::now().timestamp());
    let is_channel_post = edited_with_timestamp
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");
    let is_business_message = edited_with_timestamp
        .get("business_connection_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .is_some();

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: if is_channel_post || is_business_message {
            None
        } else {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        },
        channel_post: None,
        edited_channel_post: if is_channel_post {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
        business_connection: None,
        business_message: None,
        edited_business_message: if is_business_message {
            Some(serde_json::from_value(edited_with_timestamp.clone()).map_err(ApiError::internal)?)
        } else {
            None
        },
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

    webhook::persist_and_dispatch_update(state, conn, token, bot_id, update_value)
}

pub fn ensure_message_can_be_edited_by_bot(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
    via_inline_message: bool,
) -> Result<(), ApiError> {
    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to edit was not found"));
    };

    if !via_inline_message {
        let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
        if owner_user_id != actor_user_id {
            return Err(ApiError::bad_request("message can't be edited"));
        }

        if actor_user_id != bot_id {
            let chat_type = chats::load_sim_chat_record(conn, bot_id, chat_key)?
                .map(|chat| chat.chat_type)
                .unwrap_or_else(|| "private".to_string());

            if chat_type != "private" {
                let actor_status = chats::load_chat_member_status(conn, bot_id, chat_key, actor_user_id)?;
                if !actor_status
                    .as_deref()
                    .map(groups::is_active_chat_member_status)
                    .unwrap_or(false)
                {
                    return Err(ApiError::bad_request("message can't be edited"));
                }
            }
        }
    }

    Ok(())
}

pub fn ensure_message_can_be_deleted_by_actor(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    message_id: i64,
) -> Result<(), ApiError> {
    let owner_user_id: Option<i64> = conn
        .query_row(
            "SELECT from_user_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot_id, chat_key, message_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(owner_user_id) = owner_user_id else {
        return Err(ApiError::not_found("message to delete was not found"));
    };

    let actor_user_id = current_request_actor_user_id().unwrap_or(bot_id);
    if actor_user_id == bot_id || owner_user_id == actor_user_id {
        return Ok(());
    }

    let Some(sim_chat) = chats::load_sim_chat_record(conn, bot_id, chat_key)? else {
        return Err(ApiError::bad_request("message can't be deleted"));
    };

    if sim_chat.chat_type == "private" {
        return Ok(());
    }

    if channels::is_direct_messages_chat(&sim_chat) {
        return Ok(());
    }

    if sim_chat.chat_type == "group" || sim_chat.chat_type == "supergroup" {
        let actor_status = chats::load_chat_member_status(conn, bot_id, chat_key, actor_user_id)?;
        if actor_status
            .as_deref()
            .map(groups::is_group_admin_or_owner_status)
            .unwrap_or(false)
        {
            return Ok(());
        }

        return Err(ApiError::bad_request("message can't be deleted"));
    }

    if sim_chat.chat_type == "channel" {
        let Some(actor_record) = chats::load_chat_member_record(conn, bot_id, chat_key, actor_user_id)? else {
            return Err(ApiError::bad_request("message can't be deleted"));
        };

        if actor_record.status == "owner" {
            return Ok(());
        }

        if actor_record.status == "admin" {
            let rights = channels::parse_channel_admin_rights_json(actor_record.admin_rights_json.as_deref());
            if rights.can_manage_chat
                || rights.can_delete_messages
                || rights.can_post_messages
                || rights.can_edit_messages
            {
                return Ok(());
            }
        }

        return Err(ApiError::bad_request("message can't be deleted"));
    }

    Err(ApiError::bad_request("message can't be deleted"))
}

pub fn delete_messages_with_dependencies(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    chat_id: i64,
    message_ids: &[i64],
) -> Result<usize, ApiError> {
    if message_ids.is_empty() {
        return Ok(0);
    }

    let placeholders = std::iter::repeat("?")
        .take(message_ids.len())
        .collect::<Vec<_>>()
        .join(",");

    let drafts_sql = format!(
        "DELETE FROM sim_message_drafts WHERE bot_id = ? AND message_id IN ({})",
        placeholders,
    );
    let mut drafts_bind_values = Vec::with_capacity(1 + message_ids.len());
    drafts_bind_values.push(Value::from(bot_id));
    for id in message_ids {
        drafts_bind_values.push(Value::from(*id));
    }
    let mut drafts_stmt = conn.prepare(&drafts_sql).map_err(ApiError::internal)?;
    drafts_stmt
        .execute(rusqlite::params_from_iter(
            drafts_bind_values.iter().map(sql_value_to_rusqlite),
        ))
        .map_err(ApiError::internal)?;
    drop(drafts_stmt);

    let messages_sql = format!(
        "DELETE FROM messages WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders,
    );
    let mut messages_bind_values = Vec::with_capacity(2 + message_ids.len());
    messages_bind_values.push(Value::from(bot_id));
    messages_bind_values.push(Value::from(chat_key.to_string()));
    for id in message_ids {
        messages_bind_values.push(Value::from(*id));
    }
    let mut messages_stmt = conn.prepare(&messages_sql).map_err(ApiError::internal)?;
    let deleted = messages_stmt
        .execute(rusqlite::params_from_iter(
            messages_bind_values.iter().map(sql_value_to_rusqlite),
        ))
        .map_err(ApiError::internal)?;
    drop(messages_stmt);

    if deleted > 0 {
        let chat_id_fragment = format!("\"chat\":{{\"id\":{}", chat_id);
        for message_id in message_ids {
            let message_id_fragment = format!("\"message_id\":{}", message_id);
            conn.execute(
                "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2 AND update_json LIKE ?3",
                params![
                    bot_id,
                    format!("%{}%", chat_id_fragment),
                    format!("%{}%", message_id_fragment),
                ],
            )
            .map_err(ApiError::internal)?;
        }

        let invoices_sql = format!(
            "DELETE FROM invoices WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
            placeholders,
        );
        let mut invoice_bind_values = Vec::with_capacity(2 + message_ids.len());
        invoice_bind_values.push(Value::from(bot_id));
        invoice_bind_values.push(Value::from(chat_key.to_string()));
        for id in message_ids {
            invoice_bind_values.push(Value::from(*id));
        }
        let mut invoice_stmt = conn.prepare(&invoices_sql).map_err(ApiError::internal)?;
        invoice_stmt
            .execute(rusqlite::params_from_iter(
                invoice_bind_values.iter().map(sql_value_to_rusqlite),
            ))
            .map_err(ApiError::internal)?;
        drop(invoice_stmt);

        channels::ensure_sim_suggested_posts_storage(conn)?;
        let suggested_sql = format!(
            "DELETE FROM sim_suggested_posts WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
            placeholders,
        );
        let mut suggested_bind_values = Vec::with_capacity(2 + message_ids.len());
        suggested_bind_values.push(Value::from(bot_id));
        suggested_bind_values.push(Value::from(chat_key.to_string()));
        for id in message_ids {
            suggested_bind_values.push(Value::from(*id));
        }
        let mut suggested_stmt = conn.prepare(&suggested_sql).map_err(ApiError::internal)?;
        suggested_stmt
            .execute(rusqlite::params_from_iter(
                suggested_bind_values.iter().map(sql_value_to_rusqlite),
            ))
            .map_err(ApiError::internal)?;
        drop(suggested_stmt);
    }

    Ok(deleted)
}

pub fn normalize_sim_parse_mode(value: Option<&str>) -> Option<String> {
    let normalized = value?.trim();
    if normalized.is_empty() {
        return None;
    }

    if normalized.eq_ignore_ascii_case("markdownv2") {
        return Some("MarkdownV2".to_string());
    }
    if normalized.eq_ignore_ascii_case("markdown") {
        return Some("Markdown".to_string());
    }
    if normalized.eq_ignore_ascii_case("html") {
        return Some("HTML".to_string());
    }

    None
}

pub fn text_matches_privacy_command_or_mention(text: &str, bot_username: &str) -> bool {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return false;
    }

    let bot_username_lower = bot_username.to_ascii_lowercase();
    let mention = format!("@{}", bot_username_lower);
    let lowered = trimmed.to_ascii_lowercase();

    if lowered.contains(&mention) {
        return true;
    }

    let Some(first_token) = lowered.split_whitespace().next() else {
        return false;
    };
    if !first_token.starts_with('/') {
        return false;
    }

    if let Some((_, command_scope)) = first_token.split_once('@') {
        return command_scope == bot_username_lower;
    }

    true
}

pub fn message_targets_bot_via_entities(
    text: Option<&str>,
    entities: Option<&Vec<MessageEntity>>,
    bot_username: &str,
) -> bool {
    let Some(text) = text else {
        return false;
    };
    let Some(entities) = entities else {
        return false;
    };

    let bot_username_lower = bot_username.to_ascii_lowercase();
    let bot_mention = format!("@{}", bot_username_lower);
    let first_non_ws_byte = text
        .char_indices()
        .find_map(|(idx, ch)| if ch.is_whitespace() { None } else { Some(idx) })
        .unwrap_or(text.len());
    let first_non_ws_offset = utf16_len(&text[..first_non_ws_byte]);

    for entity in entities {
        if entity.offset < 0 || entity.length <= 0 {
            continue;
        }

        let Some(fragment) = entity_text_by_utf16_span(text, entity.offset as usize, entity.length as usize) else {
            continue;
        };

        if entity.r#type == "mention" {
            if fragment.to_ascii_lowercase() == bot_mention {
                return true;
            }
            continue;
        }

        if entity.r#type == "bot_command" {
            if entity.offset as usize != first_non_ws_offset {
                continue;
            }

            let normalized = fragment.to_ascii_lowercase();
            if let Some((_, command_scope)) = normalized.split_once('@') {
                if command_scope == bot_username_lower.as_str() {
                    return true;
                }
            } else {
                return true;
            }
        }
    }

    false
}

pub fn send_kind_from_media_field(media_field: &str) -> ChatSendKind {
    match media_field {
        "photo" => ChatSendKind::Photo,
        "video" => ChatSendKind::Video,
        "audio" => ChatSendKind::Audio,
        "voice" => ChatSendKind::Voice,
        "document" => ChatSendKind::Document,
        "video_note" => ChatSendKind::VideoNote,
        _ => ChatSendKind::Other,
    }
}

pub fn send_kind_from_payload_field(payload_field: &str) -> ChatSendKind {
    match payload_field {
        "poll" => ChatSendKind::Poll,
        "invoice" => ChatSendKind::Invoice,
        _ => ChatSendKind::Other,
    }
}

pub fn send_kind_from_sim_user_media_kind(media_kind: &str) -> ChatSendKind {
    match media_kind {
        "photo" => ChatSendKind::Photo,
        "video" => ChatSendKind::Video,
        "audio" => ChatSendKind::Audio,
        "voice" => ChatSendKind::Voice,
        "document" => ChatSendKind::Document,
        "video_note" => ChatSendKind::VideoNote,
        _ => ChatSendKind::Other,
    }
}

// --- Service Message ---

pub fn display_name_for_service_user(user: &User) -> String {
    if !user.first_name.trim().is_empty() {
        return user.first_name.clone();
    }

    if let Some(username) = user.username.as_ref() {
        if !username.trim().is_empty() {
            return format!("@{}", username);
        }
    }

    format!("user_{}", user.id)
}

pub fn service_text_new_chat_members(actor: &User, members: &[User]) -> String {
    let actor_name = display_name_for_service_user(actor);
    let member_names: Vec<String> = members
        .iter()
        .map(display_name_for_service_user)
        .collect();

    if members.len() == 1 && members[0].id == actor.id {
        format!("{} joined the group", actor_name)
    } else {
        format!("{} added {}", actor_name, member_names.join(", "))
    }
}

pub fn service_text_left_chat_member(actor: &User, left_member: &User) -> String {
    let actor_name = display_name_for_service_user(actor);
    let left_name = display_name_for_service_user(left_member);

    if actor.id == left_member.id {
        format!("{} left the group", left_name)
    } else {
        format!("{} removed {}", actor_name, left_name)
    }
}

pub fn service_text_group_title_changed(actor: &User, new_title: &str) -> String {
    format!(
        "{} changed the group name to \"{}\"",
        display_name_for_service_user(actor),
        new_title,
    )
}

pub fn service_text_chat_created(actor: &User, chat_type: &str) -> String {
    let actor_name = display_name_for_service_user(actor);
    match chat_type {
        "supergroup" => format!("{} created the supergroup", actor_name),
        "channel" => format!("{} created the channel", actor_name),
        _ => format!("{} created the group", actor_name),
    }
}

pub fn emit_service_message_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    chat_key: &str,
    chat: &Chat,
    from: &User,
    date: i64,
    text: String,
    service_fields: Map<String, Value>,
) -> Result<(), ApiError> {
    let persisted_text = text;
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot_id, chat_key, from.id, &persisted_text, date],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_json = json!({
        "message_id": message_id,
        "date": date,
        "chat": chat,
        "from": from,
        "text": persisted_text
    });
    for (key, value) in service_fields {
        message_json[key] = value;
    }

    let is_channel_post = chat.r#type == "channel";
    let update_value = if is_channel_post {
        json!({
            "update_id": 0,
            "channel_post": message_json,
        })
    } else {
        json!({
            "update_id": 0,
            "message": message_json,
        })
    };

    webhook::persist_and_dispatch_update(state, conn, token, bot_id, update_value)
}

pub fn handle_sim_clear_history(
    state: &Data<AppState>,
    token: &str,
    body: SimClearHistoryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let (chat_key, chat_id) =
        chats::resolve_chat_key_and_id(&mut conn, bot.id, &Value::from(body.chat_id))?;

    let sim_chat = chats::load_sim_chat_record(&mut conn, bot.id, &chat_key)?;
    let scoped_thread_id = body.message_thread_id;

    let message_ids: Vec<i64> = if let Some(message_thread_id) = scoped_thread_id {
        if message_thread_id <= 0 {
            return Err(ApiError::bad_request("message_thread_id is invalid"));
        }

        let sim_chat = sim_chat.as_ref().ok_or_else(|| {
            ApiError::bad_request(
                "message_thread_id is available only in forum supergroups and channel direct messages chats",
            )
        })?;

        if channels::is_direct_messages_chat(sim_chat) {
            let topic_exists = channels::load_direct_messages_topic_record(
                &mut conn,
                bot.id,
                &chat_key,
                message_thread_id,
            )?
            .is_some();
            if !topic_exists {
                return Err(ApiError::not_found("direct messages topic not found"));
            }
        } else if sim_chat.chat_type == "supergroup" && sim_chat.is_forum {
            if message_thread_id == 1 {
                let _ = groups::ensure_general_forum_topic_state(&mut conn, bot.id, &chat_key)?;
            } else if groups::load_forum_topic(&mut conn, bot.id, &chat_key, message_thread_id)?.is_none() {
                return Err(ApiError::not_found("forum topic not found"));
            }
        } else {
            return Err(ApiError::bad_request(
                "message_thread_id is available only in forum supergroups and channel direct messages chats",
            ));
        }

        groups::collect_message_ids_for_thread(&mut conn, bot.id, &chat_key, message_thread_id)?
    } else {
        let mut message_stmt = conn
            .prepare(
                "SELECT message_id
                 FROM messages
                 WHERE bot_id = ?1 AND chat_key = ?2
                 ORDER BY message_id ASC",
            )
            .map_err(ApiError::internal)?;
        let message_rows = message_stmt
            .query_map(params![bot.id, &chat_key], |row| row.get::<_, i64>(0))
            .map_err(ApiError::internal)?;
        let message_ids = message_rows
            .collect::<Result<Vec<_>, _>>()
            .map_err(ApiError::internal)?;
        drop(message_stmt);
        message_ids
    };

    let mut deleted = 0usize;
    for chunk in message_ids.chunks(300) {
        deleted += delete_messages_with_dependencies(
            &mut conn,
            bot.id,
            &chat_key,
            chat_id,
            chunk,
        )?;
    }

    if let Some(message_thread_id) = scoped_thread_id {
        conn.execute(
            "DELETE FROM sim_message_drafts
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_thread_id = ?3",
            params![bot.id, &chat_key, message_thread_id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "DELETE FROM sim_message_drafts WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
        )
        .map_err(ApiError::internal)?;

        let chat_fragment = format!("\"chat\":{{\"id\":{}", chat_id);
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2",
            params![bot.id, format!("%{}%", chat_fragment)],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2",
            params![bot.id, &chat_key],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!({"deleted_count": deleted}))
}

// --- Reactions ---

pub fn handle_sim_set_user_reaction(
    state: &Data<AppState>,
    token: &str,
    body: SimSetUserReactionRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = users::ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

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
        can_manage_bots: None,
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

pub fn normalize_reaction_values(raw: Option<Vec<Value>>) -> Result<Vec<Value>, ApiError> {
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

        if reaction_type == "paid" {
            normalized.push(json!({
                "type": "paid",
            }));
            continue;
        }

        let value = if reaction_type == "emoji" {
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

            json!({
                "type": "emoji",
                "emoji": emoji,
            })
        } else {
            return Err(ApiError::bad_request(
                "only emoji and paid reactions are supported in simulator",
            ));
        };

        if !normalized.iter().any(|existing| existing == &value) {
            normalized.push(value);
        }
    }

    Ok(normalized)
}

pub fn is_allowed_telegram_reaction_emoji(emoji: &str) -> bool {
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

pub fn apply_message_reaction_change(
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

    let has_paid_reaction = new_reaction.iter().any(|reaction| {
        reaction
            .get("type")
            .and_then(Value::as_str)
            .map(|kind| kind.eq_ignore_ascii_case("paid"))
            .unwrap_or(false)
    });

    if has_paid_reaction {
        let Some(sim_chat) = chats::load_sim_chat_record(conn, bot.id, chat_key)? else {
            return Err(ApiError::bad_request(
                "paid reactions are available only in channels",
            ));
        };

        if sim_chat.chat_type != "channel" {
            return Err(ApiError::bad_request(
                "paid reactions are available only in channels",
            ));
        }

        if !sim_chat.channel_paid_reactions_enabled {
            return Err(ApiError::bad_request(
                "paid star reactions are disabled for this channel",
            ));
        }
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
        let mut counts: HashMap<String, (ReactionType, i64)> = HashMap::new();
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
                    let reaction_type: ReactionType =
                        serde_json::from_value(reaction).map_err(ApiError::internal)?;
                    let entry = counts.entry(key).or_insert((reaction_type, 0));
                    entry.1 += 1;
                }
            }
        }

        let mut payload = Vec::<ReactionCount>::new();
        for (_, (reaction_type, total_count)) in counts {
            payload.push(ReactionCount {
                r#type: reaction_type,
                total_count,
            });
        }
        payload
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
    let old_reaction_types: Vec<ReactionType> = old_reaction
        .into_iter()
        .map(|value| serde_json::from_value(value).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;
    let new_reaction_types: Vec<ReactionType> = new_reaction
        .into_iter()
        .map(|value| serde_json::from_value(value).map_err(ApiError::internal))
        .collect::<Result<Vec<_>, _>>()?;

    let linked_context = channels::load_linked_discussion_mapping_for_message(
        conn,
        bot.id,
        chat_key,
        message_id,
    )?;

    let mut reaction_update = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: None,
        channel_post: None,
        edited_channel_post: None,
        business_connection: None,
        business_message: None,
        edited_business_message: None,
        deleted_business_messages: None,
        message_reaction: Some(MessageReactionUpdated {
            chat: chat.clone(),
            message_id,
            user: Some(actor),
            actor_chat: None,
            date: now,
            old_reaction: old_reaction_types,
            new_reaction: new_reaction_types,
        }),
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

    if let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = linked_context.as_ref() {
        if let Some(obj) = reaction_update
            .get_mut("message_reaction")
            .and_then(Value::as_object_mut)
        {
            obj.insert(
                "linked_channel_message_id".to_string(),
                Value::from(*channel_message_id),
            );
            obj.insert(
                "linked_discussion_root_message_id".to_string(),
                Value::from(*discussion_root_message_id),
            );
            obj.insert(
                "linked_channel_chat_id".to_string(),
                Value::String(channel_chat_key.clone()),
            );
        }
    }

    webhook::persist_and_dispatch_update(state, conn, token, bot.id, reaction_update)?;

    let mut reaction_count_update = serde_json::to_value(Update {
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
        message_reaction_count: Some(MessageReactionCountUpdated {
            chat,
            message_id,
            date: now,
            reactions: count_payload,
        }),
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

    if let Some((channel_chat_key, channel_message_id, discussion_root_message_id)) = linked_context.as_ref() {
        if let Some(obj) = reaction_count_update
            .get_mut("message_reaction_count")
            .and_then(Value::as_object_mut)
        {
            obj.insert(
                "linked_channel_message_id".to_string(),
                Value::from(*channel_message_id),
            );
            obj.insert(
                "linked_discussion_root_message_id".to_string(),
                Value::from(*discussion_root_message_id),
            );
            obj.insert(
                "linked_channel_chat_id".to_string(),
                Value::String(channel_chat_key.clone()),
            );
        }
    }

    webhook::persist_and_dispatch_update(state, conn, token, bot.id, reaction_count_update)?;

    Ok(json!(true))
}

// --- CheckList---

pub fn normalize_input_checklist_task(task: &InputChecklistTask) -> Result<ChecklistTask, ApiError> {
    if task.id <= 0 {
        return Err(ApiError::bad_request("checklist task id must be greater than zero"));
    }

    let explicit_entities = task
        .text_entities
        .as_ref()
        .and_then(|value| serde_json::to_value(value).ok());
    let (text, text_entities_value) = parse_formatted_text(
        &task.text,
        task.parse_mode.as_deref(),
        explicit_entities,
    );

    if text.trim().is_empty() {
        return Err(ApiError::bad_request("checklist task text is empty"));
    }
    if text.chars().count() > 300 {
        return Err(ApiError::bad_request("checklist task text is too long"));
    }

    let text_entities = text_entities_value
        .and_then(|value| serde_json::from_value::<Vec<MessageEntity>>(value).ok());

    Ok(ChecklistTask {
        id: task.id,
        text,
        text_entities,
        completed_by_user: None,
        completed_by_chat: None,
        completion_date: None,
    })
}

pub fn normalize_input_checklist(input: &InputChecklist) -> Result<Checklist, ApiError> {
    let explicit_title_entities = input
        .title_entities
        .as_ref()
        .and_then(|value| serde_json::to_value(value).ok());
    let (title, title_entities_value) = parse_formatted_text(
        &input.title,
        input.parse_mode.as_deref(),
        explicit_title_entities,
    );

    if title.trim().is_empty() {
        return Err(ApiError::bad_request("checklist title is empty"));
    }
    if title.chars().count() > 255 {
        return Err(ApiError::bad_request("checklist title is too long"));
    }
    if input.tasks.is_empty() {
        return Err(ApiError::bad_request("checklist must include at least one task"));
    }
    if input.tasks.len() > 30 {
        return Err(ApiError::bad_request("checklist can include at most 30 tasks"));
    }

    let mut task_ids = HashSet::<i64>::new();
    let mut tasks = Vec::<ChecklistTask>::with_capacity(input.tasks.len());
    for task in &input.tasks {
        if !task_ids.insert(task.id) {
            return Err(ApiError::bad_request("checklist task ids must be unique"));
        }
        tasks.push(normalize_input_checklist_task(task)?);
    }

    let title_entities = title_entities_value
        .and_then(|value| serde_json::from_value::<Vec<MessageEntity>>(value).ok());

    Ok(Checklist {
        title,
        title_entities,
        tasks,
        others_can_add_tasks: input.others_can_add_tasks,
        others_can_mark_tasks_as_done: input.others_can_mark_tasks_as_done,
    })
}

pub fn parse_request_with_legacy_checklist<T: DeserializeOwned>(
    params: &HashMap<String, Value>,
) -> Result<T, ApiError> {
    let object = Map::from_iter(params.iter().map(|(k, v)| (k.clone(), v.clone())));
    let normalized = normalize_legacy_checklist_request_payload(Value::Object(object));
    decode_request_value(normalized)
}

pub fn normalize_legacy_checklist_request_payload(payload: Value) -> Value {
    match payload {
        Value::Object(mut root) => {
            if let Some(checklist_value) = root.get_mut("checklist") {
                normalize_legacy_checklist_value(checklist_value);
            }

            if !root.contains_key("checklist") {
                if let Some(items_value) = root.remove("items") {
                    let mut checklist = Map::new();
                    checklist.insert(
                        "title".to_string(),
                        root.remove("title")
                            .unwrap_or_else(|| Value::String("Checklist".to_string())),
                    );
                    checklist.insert("tasks".to_string(), normalize_legacy_checklist_tasks(items_value));

                    if let Some(value) = root.remove("others_can_add_tasks") {
                        checklist.insert("others_can_add_tasks".to_string(), value);
                    }
                    if let Some(value) = root.remove("others_can_mark_tasks_as_done") {
                        checklist.insert("others_can_mark_tasks_as_done".to_string(), value);
                    }

                    root.insert("checklist".to_string(), Value::Object(checklist));
                }
            }

            Value::Object(root)
        }
        other => other,
    }
}

pub fn normalize_legacy_checklist_value(value: &mut Value) {
    if let Value::Object(checklist) = value {
        if checklist.get("tasks").is_none() {
            if let Some(items_value) = checklist.remove("items") {
                checklist.insert(
                    "tasks".to_string(),
                    normalize_legacy_checklist_tasks(items_value),
                );
            }
        }
    }
}

pub fn normalize_legacy_checklist_tasks(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .enumerate()
                .map(|(index, item)| normalize_legacy_checklist_task(item, index + 1))
                .collect(),
        ),
        other => other,
    }
}

pub fn normalize_legacy_checklist_task(value: Value, fallback_id: usize) -> Value {
    match value {
        Value::Object(mut task) => {
            if task.get("text").is_none() {
                if let Some(title) = task.remove("title") {
                    task.insert("text".to_string(), title);
                } else if let Some(label) = task.remove("label") {
                    task.insert("text".to_string(), label);
                }
            }

            if task.get("id").is_none() {
                task.insert("id".to_string(), Value::from(fallback_id as i64));
            }

            if task.get("is_done").is_none() {
                if let Some(checked) = task.remove("is_checked") {
                    task.insert("is_done".to_string(), checked);
                } else if let Some(checked) = task.remove("checked") {
                    task.insert("is_done".to_string(), checked);
                }
            }

            Value::Object(task)
        }
        Value::String(text) => json!({
            "id": fallback_id as i64,
            "text": text,
        }),
        other => json!({
            "id": fallback_id as i64,
            "text": other.to_string(),
        }),
    }
}

pub fn normalize_sticker_format(value: &str) -> Result<&'static str, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "static" => Ok("static"),
        "animated" => Ok("animated"),
        "video" => Ok("video"),
        _ => Err(ApiError::bad_request("sticker format must be static, animated, or video")),
    }
}

pub fn normalize_sticker_type(value: &str) -> Result<&'static str, ApiError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "regular" => Ok("regular"),
        "mask" => Ok("mask"),
        "custom_emoji" => Ok("custom_emoji"),
        _ => Err(ApiError::bad_request("sticker_type must be regular, mask, or custom_emoji")),
    }
}

pub fn sticker_format_flags(format: &str) -> (bool, bool) {
    match format {
        "animated" => (true, false),
        "video" => (false, true),
        _ => (false, false),
    }
}

pub fn derive_custom_emoji_id(bot_id: i64, file_unique_id: &str) -> String {
    let mut hash: u64 = 1469598103934665603;
    for byte in file_unique_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash ^= bot_id as u64;
    hash = hash.wrapping_mul(1099511628211);
    hash.to_string()
}

pub fn infer_sticker_format_from_file(file: &StoredFile) -> Option<&'static str> {
    let mime = file
        .mime_type
        .as_deref()
        .unwrap_or_default()
        .to_ascii_lowercase();
    let path = file.file_path.to_ascii_lowercase();

    if mime.contains("webm") || path.ends_with(".webm") {
        return Some("video");
    }
    if mime.contains("x-tgsticker") || path.ends_with(".tgs") {
        return Some("animated");
    }
    if mime.contains("webp") || path.ends_with(".webp") {
        return Some("static");
    }

    None
}

pub fn load_sticker_meta(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    file_id: &str,
) -> Result<Option<StickerMeta>, ApiError> {
    conn.query_row(
        "SELECT set_name, sticker_type, format, emoji, mask_position_json, custom_emoji_id, COALESCE(needs_repainting, 0)
         FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
        params![bot_id, file_id],
        |r| {
            Ok(StickerMeta {
                set_name: r.get(0)?,
                sticker_type: r.get(1)?,
                format: r.get(2)?,
                emoji: r.get(3)?,
                mask_position_json: r.get(4)?,
                custom_emoji_id: r.get(5)?,
                needs_repainting: r.get::<_, i64>(6)? == 1,
            })
        },
    )
    .optional()
    .map_err(ApiError::internal)
}

pub fn sticker_from_row(
    file_id: String,
    file_unique_id: String,
    set_name: Option<String>,
    sticker_type: String,
    format: String,
    emoji: Option<String>,
    mask_position_json: Option<String>,
    custom_emoji_id: Option<String>,
    needs_repainting: bool,
    file_size: Option<i64>,
) -> Sticker {
    let (is_animated, is_video) = sticker_format_flags(&format);
    Sticker {
        file_id,
        file_unique_id,
        r#type: sticker_type,
        width: 512,
        height: 512,
        is_animated,
        is_video,
        thumbnail: None,
        emoji,
        set_name,
        premium_animation: None,
        mask_position: mask_position_json.and_then(|raw| serde_json::from_str::<MaskPosition>(&raw).ok()),
        custom_emoji_id,
        needs_repainting: Some(needs_repainting),
        file_size,
    }
}

pub fn load_set_stickers(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    set_name: &str,
) -> Result<Vec<Sticker>, ApiError> {
    let mut stmt = conn
        .prepare(
            "SELECT s.file_id, s.file_unique_id, s.set_name, s.sticker_type, s.format, s.emoji, s.mask_position_json,
                    s.custom_emoji_id, COALESCE(s.needs_repainting, 0), f.file_size
             FROM stickers s
             LEFT JOIN files f ON f.bot_id = s.bot_id AND f.file_id = s.file_id
             WHERE s.bot_id = ?1 AND s.set_name = ?2
             ORDER BY s.position ASC, s.created_at ASC",
        )
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot_id, set_name], |r| {
            Ok(sticker_from_row(
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get::<_, i64>(8)? == 1,
                r.get(9)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut stickers = Vec::new();
    for row in rows {
        stickers.push(row.map_err(ApiError::internal)?);
    }
    Ok(stickers)
}

pub fn upsert_set_sticker(
    _state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot: &crate::database::BotInfoRecord,
    set_name: &str,
    sticker_type: &str,
    needs_repainting: bool,
    input: &InputSticker,
    position: i64,
) -> Result<(), ApiError> {
    if input.emoji_list.is_empty() {
        return Err(ApiError::bad_request("input sticker must include at least one emoji"));
    }

    let requested_format = normalize_sticker_format(&input.format)?;
    let file = resolve_media_file_with_conn(conn, bot.id, &Value::String(input.sticker.clone()), "sticker")?;
    let format = infer_sticker_format_from_file(&file).unwrap_or(requested_format);
    let now = Utc::now().timestamp();
    let (is_animated, is_video) = sticker_format_flags(format);

    let existing_custom_emoji_id: Option<String> = conn
        .query_row(
            "SELECT custom_emoji_id FROM stickers WHERE bot_id = ?1 AND file_id = ?2",
            params![bot.id, file.file_id],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?
        .flatten();
    let custom_emoji_id = if sticker_type == "custom_emoji" {
        existing_custom_emoji_id
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .or_else(|| Some(derive_custom_emoji_id(bot.id, &file.file_unique_id)))
    } else {
        None
    };

    let mask_json = input
        .mask_position
        .as_ref()
        .map(|m| serde_json::to_string(m).map_err(ApiError::internal))
        .transpose()?;
    let keywords_json = input
        .keywords
        .as_ref()
        .map(|k| serde_json::to_string(k).map_err(ApiError::internal))
        .transpose()?;
    let emoji_json = serde_json::to_string(&input.emoji_list).map_err(ApiError::internal)?;

    conn.execute(
        "INSERT INTO stickers
         (bot_id, file_id, file_unique_id, set_name, sticker_type, format, width, height, is_animated, is_video,
          emoji, emoji_list_json, keywords_json, mask_position_json, custom_emoji_id, needs_repainting, position, created_at, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 512, 512, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?16)
         ON CONFLICT(bot_id, file_id) DO UPDATE SET
             set_name = excluded.set_name,
             sticker_type = excluded.sticker_type,
             format = excluded.format,
             is_animated = excluded.is_animated,
             is_video = excluded.is_video,
             emoji = excluded.emoji,
             emoji_list_json = excluded.emoji_list_json,
             keywords_json = excluded.keywords_json,
             mask_position_json = excluded.mask_position_json,
             custom_emoji_id = excluded.custom_emoji_id,
             needs_repainting = excluded.needs_repainting,
             position = excluded.position,
             updated_at = excluded.updated_at",
        params![
            bot.id,
            file.file_id,
            file.file_unique_id,
            set_name,
            sticker_type,
            format,
            if is_animated { 1 } else { 0 },
            if is_video { 1 } else { 0 },
            input.emoji_list[0].clone(),
            emoji_json,
            keywords_json,
            mask_json,
            custom_emoji_id,
            if needs_repainting { 1 } else { 0 },
            position,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(())
}

pub fn compact_sticker_positions(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    set_name: &str,
) -> Result<(), ApiError> {
    let mut stmt = conn
        .prepare("SELECT file_id FROM stickers WHERE bot_id = ?1 AND set_name = ?2 ORDER BY position ASC, created_at ASC")
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map(params![bot_id, set_name], |r| r.get::<_, String>(0))
        .map_err(ApiError::internal)?;

    let mut ids = Vec::new();
    for row in rows {
        ids.push(row.map_err(ApiError::internal)?);
    }

    let now = Utc::now().timestamp();
    for (idx, file_id) in ids.iter().enumerate() {
        conn.execute(
            "UPDATE stickers SET position = ?1, updated_at = ?2 WHERE bot_id = ?3 AND file_id = ?4",
            params![idx as i64, now, bot_id, file_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(())
}

pub fn resolve_media_file(
    state: &Data<AppState>,
    token: &str,
    input: &Value,
    media_kind: &str,
) -> Result<StoredFile, ApiError> {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    resolve_media_file_with_conn(&mut conn, bot.id, input, media_kind)
}

pub fn parse_input_file_value(input: &Value, field: &str) -> Result<Value, ApiError> {
    match input {
        Value::String(_) => Ok(input.clone()),
        Value::Object(obj) => {
            if let Some(extra) = obj.get("extra") {
                return match extra {
                    Value::String(_) => Ok(extra.clone()),
                    _ => Err(ApiError::bad_request(format!("{field} extra must be string"))),
                };
            }

            if let Some(media) = obj.get("media") {
                return match media {
                    Value::String(_) => Ok(media.clone()),
                    _ => Err(ApiError::bad_request(format!("{field} media must be string"))),
                };
            }

            Err(ApiError::bad_request(format!("{field} must be a string or InputFile object")))
        }
        _ => Err(ApiError::bad_request(format!("{field} must be a string or InputFile object"))),
    }
}

pub fn resolve_media_file_with_conn(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    input: &Value,
    media_kind: &str,
) -> Result<StoredFile, ApiError> {
    let input_text = input
        .as_str()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("{} is invalid", media_kind)))?;

    if input_text.starts_with("http://") || input_text.starts_with("https://") {
        let (bytes, mime) = download_remote_file(&input_text)?;
        return store_binary_file(conn, bot_id, &bytes, mime.as_deref(), Some(input_text));
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
            conn,
            bot_id,
            &bytes,
            None,
            Some(local_candidate),
        );
    }

    let existing: Option<(String, String, String, Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT file_id, file_unique_id, file_path, mime_type, file_size
             FROM files WHERE bot_id = ?1 AND file_id = ?2",
            params![bot_id, input_text],
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
    let file_path = format!("virtual/{}/{}", bot_id, file_id.replace('/', "_"));

    conn.execute(
        "INSERT INTO files (bot_id, file_id, file_unique_id, file_path, local_path, mime_type, file_size, source, created_at)
         VALUES (?1, ?2, ?3, ?4, NULL, NULL, NULL, ?5, ?6)",
        params![bot_id, file_id, file_unique_id, file_path, input_text, now],
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