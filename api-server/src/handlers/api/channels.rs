use actix_web::web::Data;
use chrono::Utc;
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::{
    ApproveSuggestedPostRequest, DeclineSuggestedPostRequest,
};

use crate::handlers::client::{bot, channels, groups, messages, users};

use crate::handlers::parse_request;

use crate::handlers::utils::updates::current_request_actor_user_id;

const SUGGESTED_POST_MAX_FUTURE_SECONDS: i64 = 2_678_400;
const SUGGESTED_POST_DECLINE_COMMENT_MAX_LEN: usize = 128;

pub fn handle_approve_suggested_post(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: ApproveSuggestedPostRequest = parse_request(params)?;
    if request.chat_id == 0 {
        return Err(ApiError::bad_request("chat_id is invalid"));
    }
    if request.message_id <= 0 {
        return Err(ApiError::bad_request("message_id is invalid"));
    }

    let now = Utc::now().timestamp();
    if let Some(send_date) = request.send_date {
        if send_date <= 0 {
            return Err(ApiError::bad_request("send_date is invalid"));
        }
        if send_date - now > SUGGESTED_POST_MAX_FUTURE_SECONDS {
            return Err(ApiError::bad_request(
                "send_date must be at most 30 days in the future",
            ));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let direct_messages_chat = channels::load_direct_messages_chat_for_request(
        &mut conn,
        bot.id,
        request.chat_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    channels::ensure_channel_member_can_approve_suggested_posts(
        &mut conn,
        bot.id,
        &parent_channel_chat_key,
        actor_user_id,
    )?;

    let suggested_message = channels::load_suggested_post_message_for_service(
        &mut conn,
        &bot,
        direct_messages_chat.chat_id,
        request.message_id,
    )?;

    channels::ensure_sim_suggested_posts_storage(&mut conn)?;
    let existing = channels::load_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
    )?;

    if let Some((current_state, existing_send_date)) = existing.as_ref() {
        if current_state == "declined" {
            return Err(ApiError::bad_request(
                "suggested post is already declined",
            ));
        }
        if current_state == "paid" {
            return Ok(json!(true));
        }
        if current_state == "refunded" {
            return Err(ApiError::bad_request(
                "suggested post is already refunded",
            ));
        }
        if current_state == "approved" && request.send_date.is_none() {
            let effective_send_date = existing_send_date.unwrap_or(now);
            if effective_send_date <= now {
                let _ = channels::finalize_due_suggested_post_if_ready(
                    state,
                    &mut conn,
                    token,
                    &bot,
                    &direct_messages_chat,
                    request.message_id,
                    actor_user_id,
                )?;
            }
            return Ok(json!(true));
        }
    }

    let resolved_send_date = request
        .send_date
        .or_else(|| existing.as_ref().and_then(|(_, send_date)| *send_date))
        .or_else(|| channels::extract_suggested_post_send_date_from_message(&suggested_message));
    if let Some(send_date) = resolved_send_date {
        if send_date <= 0 {
            return Err(ApiError::bad_request("send_date is invalid"));
        }
        if send_date - now > SUGGESTED_POST_MAX_FUTURE_SECONDS {
            return Err(ApiError::bad_request(
                "send_date must be at most 30 days in the future",
            ));
        }

        if send_date < now {
            let Some(price) = channels::extract_suggested_post_price_from_message(&suggested_message) else {
                return Err(ApiError::bad_request(
                    "suggested post send_date is in the past",
                ));
            };

            channels::upsert_suggested_post_state(
                &mut conn,
                bot.id,
                &direct_messages_chat.chat_key,
                request.message_id,
                "approval_failed",
                Some(send_date),
                Some("send_date_in_past"),
                now,
            )?;

            let actor = if actor_user_id == bot.id {
                bot::build_bot_user(&bot)
            } else {
                let actor_record = users::ensure_user(&mut conn, Some(actor_user_id), None, None)?;
                users::build_user_from_sim_record(&actor_record, false)
            };
            let mut approval_failed_payload = Map::<String, Value>::new();
            approval_failed_payload.insert(
                "suggested_post_message".to_string(),
                suggested_message.clone(),
            );
            approval_failed_payload.insert("price".to_string(), price);

            let mut service_fields = Map::<String, Value>::new();
            service_fields.insert(
                "suggested_post_approval_failed".to_string(),
                Value::Object(approval_failed_payload),
            );
            let direct_messages_chat_obj = groups::build_chat_from_group_record(&direct_messages_chat);
            messages::emit_service_message_update(
                state,
                &mut conn,
                token,
                bot.id,
                &direct_messages_chat.chat_key,
                &direct_messages_chat_obj,
                &actor,
                now,
                format!(
                    "{} failed to approve a suggested post",
                    messages::display_name_for_service_user(&actor)
                ),
                service_fields,
            )?;

            return Ok(json!(true));
        }
    }
    let approved_send_date = resolved_send_date.unwrap_or(now);

    channels::upsert_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
        "approved",
        Some(approved_send_date),
        None,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        bot::build_bot_user(&bot)
    } else {
        let actor_record = users::ensure_user(&mut conn, Some(actor_user_id), None, None)?;
        users::build_user_from_sim_record(&actor_record, false)
    };
    let mut approved_payload = Map::<String, Value>::new();
    approved_payload.insert(
        "suggested_post_message".to_string(),
        suggested_message.clone(),
    );
    approved_payload.insert("send_date".to_string(), Value::from(approved_send_date));
    if let Some(price) = channels::extract_suggested_post_price_from_message(
        approved_payload
            .get("suggested_post_message")
            .unwrap_or(&Value::Null),
    ) {
        approved_payload.insert("price".to_string(), price);
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert(
        "suggested_post_approved".to_string(),
        Value::Object(approved_payload),
    );
    let direct_messages_chat_obj = groups::build_chat_from_group_record(&direct_messages_chat);
    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} approved a suggested post",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    if approved_send_date <= now {
        let _ = channels::finalize_due_suggested_post_if_ready(
            state,
            &mut conn,
            token,
            &bot,
            &direct_messages_chat,
            request.message_id,
            actor_user_id,
        )?;
    }

    Ok(json!(true))
}

pub fn handle_decline_suggested_post(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: DeclineSuggestedPostRequest = parse_request(params)?;
    if request.chat_id == 0 {
        return Err(ApiError::bad_request("chat_id is invalid"));
    }
    if request.message_id <= 0 {
        return Err(ApiError::bad_request("message_id is invalid"));
    }

    let normalized_comment = request
        .comment
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());

    if let Some(comment) = normalized_comment {
        if comment.chars().count() > SUGGESTED_POST_DECLINE_COMMENT_MAX_LEN {
            return Err(ApiError::bad_request("comment is too long"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let direct_messages_chat = channels::load_direct_messages_chat_for_request(
        &mut conn,
        bot.id,
        request.chat_id,
    )?;

    let parent_channel_chat_key = direct_messages_chat
        .parent_channel_chat_id
        .ok_or_else(|| ApiError::bad_request("direct messages chat parent channel is missing"))?
        .to_string();
    let actor_user_id = current_request_actor_user_id().unwrap_or(bot.id);
    channels::ensure_channel_member_can_manage_direct_messages(
        &mut conn,
        bot.id,
        &parent_channel_chat_key,
        actor_user_id,
    )?;

    let suggested_message = channels::load_suggested_post_message_for_service(
        &mut conn,
        &bot,
        direct_messages_chat.chat_id,
        request.message_id,
    )?;

    channels::ensure_sim_suggested_posts_storage(&mut conn)?;
    let existing = channels::load_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
    )?;

    if let Some((state, _)) = existing.as_ref() {
        if state == "approved" {
            return Err(ApiError::bad_request(
                "suggested post is already approved",
            ));
        }
        if state == "paid" {
            return Err(ApiError::bad_request(
                "suggested post is already paid",
            ));
        }
        if state == "refunded" {
            return Ok(json!(true));
        }
        if state == "declined" {
            return Ok(json!(true));
        }
    }

    let now = Utc::now().timestamp();
    channels::upsert_suggested_post_state(
        &mut conn,
        bot.id,
        &direct_messages_chat.chat_key,
        request.message_id,
        "declined",
        None,
        normalized_comment,
        now,
    )?;

    let actor = if actor_user_id == bot.id {
        bot::build_bot_user(&bot)
    } else {
        let actor_record = users::ensure_user(&mut conn, Some(actor_user_id), None, None)?;
        users::build_user_from_sim_record(&actor_record, false)
    };
    let mut declined_payload = Map::<String, Value>::new();
    declined_payload.insert("suggested_post_message".to_string(), suggested_message);
    if let Some(comment) = normalized_comment {
        declined_payload.insert("comment".to_string(), Value::String(comment.to_string()));
    }

    let mut service_fields = Map::<String, Value>::new();
    service_fields.insert(
        "suggested_post_declined".to_string(),
        Value::Object(declined_payload),
    );
    let direct_messages_chat_obj = groups::build_chat_from_group_record(&direct_messages_chat);
    messages::emit_service_message_update(
        state,
        &mut conn,
        token,
        bot.id,
        &direct_messages_chat.chat_key,
        &direct_messages_chat_obj,
        &actor,
        now,
        format!(
            "{} declined a suggested post",
            messages::display_name_for_service_user(&actor)
        ),
        service_fields,
    )?;

    Ok(json!(true))
}