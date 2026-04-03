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
    AnswerCallbackQueryRequest, AnswerInlineQueryRequest, AnswerPreCheckoutQueryRequest, AnswerShippingQueryRequest, DeleteMessageRequest, DeleteMessagesRequest, DeleteWebhookRequest, EditMessageCaptionRequest,
    CreateInvoiceLinkRequest, EditMessageMediaRequest, EditMessageReplyMarkupRequest, EditMessageTextRequest, EditUserStarSubscriptionRequest, GetFileRequest, GetMeRequest, GetMyStarBalanceRequest, GetStarTransactionsRequest,
    GetUpdatesRequest, SendAudioRequest, SendDocumentRequest, SendMediaGroupRequest,
    RefundStarPaymentRequest, SendInvoiceRequest, SendMessageRequest, SendPhotoRequest, SendPollRequest, SendVideoRequest, SendVoiceRequest, SetMessageReactionRequest, SetWebhookRequest, StopPollRequest,
};
use crate::generated::types::{CallbackQuery, Chat, ChosenInlineResult, InlineKeyboardMarkup, InlineQuery, Invoice, MaybeInaccessibleMessage, Message, MessageReactionCountUpdated, MessageReactionUpdated, OrderInfo, Poll, PollAnswer, PollOption, PreCheckoutQuery, ReactionCount, ReactionType, ReplyKeyboardMarkup, ReplyKeyboardRemove, ShippingAddress, ShippingQuery, SuccessfulPayment, Update, User};
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

#[derive(Deserialize)]
pub struct SimPressInlineButtonRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub callback_data: String,
}

#[derive(Deserialize)]
pub struct SimSendInlineQueryRequest {
    pub chat_id: Option<i64>,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub query: String,
    pub offset: Option<String>,
}

#[derive(Deserialize)]
pub struct SimChooseInlineResultRequest {
    pub inline_query_id: String,
    pub result_id: String,
}

#[derive(Deserialize)]
pub struct SimVotePollRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub option_ids: Vec<i64>,
}

#[derive(Deserialize)]
pub struct SimPayInvoiceRequest {
    pub chat_id: i64,
    pub message_id: i64,
    pub user_id: Option<i64>,
    pub first_name: Option<String>,
    pub username: Option<String>,
    pub payment_method: Option<String>,
    pub outcome: Option<String>,
    pub tip_amount: Option<i64>,
}

pub fn handle_sim_get_poll_voters(
    state: &Data<AppState>,
    token: &str,
    chat_id: i64,
    message_id: i64,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = chat_id.to_string();

    let row: Option<(String, i64)> = conn
        .query_row(
            "SELECT id, is_anonymous FROM polls WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, message_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, is_anonymous)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    if is_anonymous == 1 {
        return Ok(json!({
            "poll_id": poll_id,
            "anonymous": true,
            "voters": [],
        }));
    }

    let mut stmt = conn
        .prepare(
            "SELECT v.voter_user_id, u.first_name, u.username, v.option_ids_json
             FROM poll_votes v
             LEFT JOIN users u ON u.id = v.voter_user_id
             WHERE v.poll_id = ?1
             ORDER BY v.updated_at ASC",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![poll_id.clone()], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?,
                r.get::<_, Option<String>>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut voters = Vec::new();
    for row in rows {
        let (user_id, first_name, username, option_ids_json) = row.map_err(ApiError::internal)?;
        let option_ids: Vec<i64> = serde_json::from_str(&option_ids_json).unwrap_or_default();
        voters.push(json!({
            "user_id": user_id,
            "first_name": first_name.unwrap_or_else(|| "User".to_string()),
            "username": username,
            "option_ids": option_ids,
        }));
    }

    Ok(json!({
        "poll_id": poll_id,
        "anonymous": false,
        "voters": voters,
    }))
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
        "sendpoll" => handle_send_poll(state, token, &params),
        "sendinvoice" => handle_send_invoice(state, token, &params),
        "sendmediagroup" => handle_send_media_group(state, token, &params),
        "editmessagetext" => handle_edit_message_text(state, token, &params),
        "editmessagecaption" => handle_edit_message_caption(state, token, &params),
        "editmessagemedia" => handle_edit_message_media(state, token, &params),
        "editmessagereplymarkup" => handle_edit_message_reply_markup(state, token, &params),
        "deletemessage" => handle_delete_message(state, token, &params),
        "deletemessages" => handle_delete_messages(state, token, &params),
        "getfile" => handle_get_file(state, token, &params),
        "getupdates" => handle_get_updates(state, token, &params),
        "setwebhook" => handle_set_webhook(state, token, &params),
        "deletewebhook" => handle_delete_webhook(state, token, &params),
        "setmessagereaction" => handle_set_message_reaction(state, token, &params),
        "stoppoll" => handle_stop_poll(state, token, &params),
        "answercallbackquery" => handle_answer_callback_query(state, token, &params),
        "answerinlinequery" => handle_answer_inline_query(state, token, &params),
        "answershippingquery" => handle_answer_shipping_query(state, token, &params),
        "answerprecheckoutquery" => handle_answer_pre_checkout_query(state, token, &params),
        "createinvoicelink" => handle_create_invoice_link(state, token, &params),
        "getmystarbalance" => handle_get_my_star_balance(state, token, &params),
        "getstartransactions" => handle_get_star_transactions(state, token, &params),
        "refundstarpayment" => handle_refund_star_payment(state, token, &params),
        "edituserstarsubscription" => handle_edit_user_star_subscription(state, token, &params),
        _ => Err(ApiError::not_found(format!("method {} not found", method))),
    }
}

fn handle_create_invoice_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateInvoiceLinkRequest = parse_request(params)?;
    let max_tip_amount = request.max_tip_amount.unwrap_or(0);
    let suggested_tip_amounts = request.suggested_tip_amounts.clone().unwrap_or_default();

    let normalized_currency = request.currency.trim().to_ascii_uppercase();
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

    for price in &request.prices {
        if price.label.trim().is_empty() {
            return Err(ApiError::bad_request("price label is empty"));
        }
        if price.amount <= 0 {
            return Err(ApiError::bad_request("price amount must be greater than zero"));
        }
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let slug = request
        .payload
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    Ok(json!(format!("https://laragram.local/invoice/{}/{}", bot.id, slug)))
}

fn handle_get_my_star_balance(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let _request: GetMyStarBalanceRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let balance: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(amount), 0) FROM star_transactions_ledger WHERE bot_id = ?1",
            params![bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    Ok(json!({
        "amount": balance,
    }))
}

fn handle_get_star_transactions(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetStarTransactionsRequest = parse_request(params)?;
    let offset = request.offset.unwrap_or(0).max(0);
    let limit = request.limit.unwrap_or(20).clamp(1, 100);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut stmt = conn
        .prepare(
            "SELECT id, amount, date
             FROM star_transactions_ledger
             WHERE bot_id = ?1
             ORDER BY date DESC
             LIMIT ?2 OFFSET ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, limit, offset], |r| {
            Ok(json!({
                "id": r.get::<_, String>(0)?,
                "amount": r.get::<_, i64>(1)?,
                "date": r.get::<_, i64>(2)?,
            }))
        })
        .map_err(ApiError::internal)?;

    let mut transactions = Vec::new();
    for row in rows {
        transactions.push(row.map_err(ApiError::internal)?);
    }

    Ok(json!({ "transactions": transactions }))
}

fn handle_refund_star_payment(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RefundStarPaymentRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let original_amount: Option<i64> = conn
        .query_row(
            "SELECT amount FROM star_transactions_ledger
             WHERE bot_id = ?1 AND user_id = ?2 AND telegram_payment_charge_id = ?3 AND kind = 'payment'",
            params![bot.id, request.user_id, request.telegram_payment_charge_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some(amount) = original_amount else {
        return Err(ApiError::bad_request("star payment not found for refund"));
    };

    let already_refunded: Option<String> = conn
        .query_row(
            "SELECT id FROM star_transactions_ledger
             WHERE bot_id = ?1 AND user_id = ?2 AND telegram_payment_charge_id = ?3 AND kind = 'refund'",
            params![bot.id, request.user_id, request.telegram_payment_charge_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if already_refunded.is_some() {
        return Err(ApiError::bad_request("star payment already refunded"));
    }

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO star_transactions_ledger
         (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'refund')",
        params![
            format!("refund_{}", generate_telegram_numeric_id()),
            bot.id,
            request.user_id,
            request.telegram_payment_charge_id,
            -amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_edit_user_star_subscription(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditUserStarSubscriptionRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let now = Utc::now().timestamp();

    conn.execute(
        "INSERT INTO star_subscriptions
         (bot_id, user_id, telegram_payment_charge_id, is_canceled, updated_at)
         VALUES (?1, ?2, ?3, ?4, ?5)
         ON CONFLICT(bot_id, user_id, telegram_payment_charge_id)
         DO UPDATE SET is_canceled = excluded.is_canceled, updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            request.telegram_payment_charge_id,
            if request.is_canceled { 1 } else { 0 },
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_callback_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let mut normalized = params.clone();
    if let Some(raw) = params.get("show_alert") {
        if let Some(loose) = value_to_optional_bool_loose(raw) {
            normalized.insert("show_alert".to_string(), Value::Bool(loose));
        }
    }

    let request: AnswerCallbackQueryRequest = parse_request(&normalized)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let callback_row: Option<String> = conn
        .query_row(
            "SELECT id FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.callback_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if callback_row.is_none() {
        return Err(ApiError::not_found("callback query not found"));
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE callback_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.callback_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_inline_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerInlineQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT query, from_user_id FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((query_text, from_user_id)) = query_row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE inline_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.inline_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    let cache_time = request.cache_time.unwrap_or(300).max(0);
    let is_personal = request.is_personal.unwrap_or(false);
    let query_offset: String = conn
        .query_row(
            "SELECT offset FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.inline_query_id, bot.id],
            |r| r.get(0),
        )
        .map_err(ApiError::internal)?;

    if cache_time > 0 {
        let expires_at = now + cache_time;
        let cache_user_id = if is_personal { from_user_id } else { -1 };
        conn.execute(
            "INSERT INTO inline_query_cache
             (bot_id, query, offset, from_user_id, answer_json, cache_time, expires_at, is_personal, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT(bot_id, query, offset, from_user_id)
             DO UPDATE SET answer_json = excluded.answer_json,
                           cache_time = excluded.cache_time,
                           expires_at = excluded.expires_at,
                           is_personal = excluded.is_personal,
                           created_at = excluded.created_at",
            params![
                bot.id,
                query_text,
                query_offset,
                cache_user_id,
                answer_payload.to_string(),
                cache_time,
                expires_at,
                if is_personal { 1 } else { 0 },
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(json!(true))
}

fn handle_answer_shipping_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerShippingQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<String> = conn
        .query_row(
            "SELECT id FROM shipping_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.shipping_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if query_row.is_none() {
        return Err(ApiError::not_found("shipping query not found"));
    }

    if !request.ok {
        let has_error_message = request
            .error_message
            .as_ref()
            .map(|text| !text.trim().is_empty())
            .unwrap_or(false);
        if !has_error_message {
            return Err(ApiError::bad_request("error_message is required when ok is false"));
        }
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE shipping_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.shipping_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}

fn handle_answer_pre_checkout_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerPreCheckoutQueryRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let query_row: Option<String> = conn
        .query_row(
            "SELECT id FROM pre_checkout_queries WHERE id = ?1 AND bot_id = ?2",
            params![request.pre_checkout_query_id, bot.id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if query_row.is_none() {
        return Err(ApiError::not_found("pre checkout query not found"));
    }

    if !request.ok {
        let has_error_message = request
            .error_message
            .as_ref()
            .map(|text| !text.trim().is_empty())
            .unwrap_or(false);
        if !has_error_message {
            return Err(ApiError::bad_request("error_message is required when ok is false"));
        }
    }

    let now = Utc::now().timestamp();
    let answer_payload = serde_json::to_value(&request).map_err(ApiError::internal)?;

    conn.execute(
        "UPDATE pre_checkout_queries SET answered_at = ?1, answer_json = ?2 WHERE id = ?3 AND bot_id = ?4",
        params![now, answer_payload.to_string(), request.pre_checkout_query_id, bot.id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
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

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

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

    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }
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
        request.reply_markup,
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
        request.reply_markup,
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
        request.reply_markup,
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
        request.reply_markup,
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
        request.reply_markup,
        "voice",
        voice,
    )
}

fn handle_send_invoice(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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

    let chat_key = value_to_chat_key(&request.chat_id)?;
    ensure_chat(&mut conn, &chat_key)?;

    let reply_markup_value = request
        .reply_markup
        .as_ref()
        .and_then(|markup| serde_json::to_value(markup).ok());

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup_value.as_ref(),
    )?;

    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, bot.id, request.description, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let mut message_value = load_message_value(&mut conn, &bot, message_id)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));

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
    message_value["invoice_meta"] = json!({
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
    });

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

        if let Ok(reply_value) = load_message_value(&mut conn, &bot, reply_parameters.message_id) {
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

    let update_value = json!({
        "update_id": 0,
        "message": message_value.clone(),
    });

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
}

fn handle_send_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
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
        if !(5..=600).contains(&open_period) {
            return Err(ApiError::bad_request("open_period must be between 5 and 600"));
        }
    }

    let now = Utc::now().timestamp();
    if let Some(close_date) = request.close_date {
        let delta = close_date - now;
        if !(5..=600).contains(&delta) {
            return Err(ApiError::bad_request("close_date must be 5-600 seconds in the future"));
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
    if poll_type == "quiz" && allows_multiple_answers {
        return Err(ApiError::bad_request("quiz poll cannot allow multiple answers"));
    }

    let correct_option_id = request.correct_option_id;
    if poll_type == "quiz" {
        let Some(correct_idx) = correct_option_id else {
            return Err(ApiError::bad_request("quiz poll requires correct_option_id"));
        };
        if correct_idx < 0 || correct_idx >= request.options.len() as i64 {
            return Err(ApiError::bad_request("correct_option_id out of range"));
        }
    } else if correct_option_id.is_some() {
        return Err(ApiError::bad_request("correct_option_id is allowed only for quiz polls"));
    }

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
            text: option_text,
            text_entities,
            voter_count: 0,
        });
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;
    ensure_chat(&mut conn, &chat_key)?;

    let reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        request.reply_markup.as_ref(),
    )?;

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, bot.id, question, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();

    let poll_id = generate_telegram_numeric_id();
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
        correct_option_id,
        explanation,
        explanation_entities: explanation_entities
            .clone()
            .and_then(|value| serde_json::from_value(value).ok()),
        open_period: request.open_period,
        close_date: request.close_date,
    };

    conn.execute(
        "INSERT INTO polls (id, bot_id, chat_key, message_id, question, options_json, total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, correct_option_id, explanation, open_period, close_date, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16)",
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
            poll.correct_option_id,
            poll.explanation,
            poll.open_period,
            poll.close_date,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "INSERT OR REPLACE INTO poll_metadata (poll_id, question_entities_json, explanation_entities_json)
         VALUES (?1, ?2, ?3)",
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
        ],
    )
    .map_err(ApiError::internal)?;

    let mut message_value = load_message_value(&mut conn, &bot, message_id)?;
    message_value["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    message_value.as_object_mut().map(|obj| obj.remove("text"));
    message_value.as_object_mut().map(|obj| obj.remove("edit_date"));

    if let Some(markup) = reply_markup {
        message_value["reply_markup"] = markup;
    }

    if let Some(reply_parameters) = request.reply_parameters {
        let reply_chat_key = match reply_parameters.chat_id {
            Some(ref value) => value_to_chat_key(value).unwrap_or_else(|_| chat_key.clone()),
            None => chat_key.clone(),
        };

        if let Ok(reply_value) = load_message_value(&mut conn, &bot, reply_parameters.message_id) {
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
        message: Some(serde_json::from_value(message_value.clone()).map_err(ApiError::internal)?),
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
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;
    Ok(message_value)
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

fn handle_stop_poll(state: &Data<AppState>, token: &str, params: &HashMap<String, Value>) -> ApiResult {
    let request: StopPollRequest = parse_request(params)?;

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let row: Option<(String, String, String, i64, i64, i64, String, i64, Option<i64>, Option<String>, Option<i64>, Option<i64>, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type, p.allows_multiple_answers, p.correct_option_id, p.explanation, p.open_period, p.close_date,
                    m.question_entities_json, m.explanation_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, request.message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?, r.get(10)?, r.get(11)?, r.get(12)?, r.get(13)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, total_voter_count, _is_closed, is_anonymous, poll_type, allows_multiple_answers, correct_option_id, explanation, open_period, close_date, question_entities_json, explanation_entities_json)) = row else {
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
        correct_option_id,
        explanation,
        explanation_entities,
        open_period,
        close_date,
    };

    let mut edited_message = load_message_value(&mut conn, &bot, request.message_id)?;
    edited_message["poll"] = serde_json::to_value(&poll).map_err(ApiError::internal)?;
    edited_message.as_object_mut().map(|obj| obj.remove("text"));
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;
    Ok(edited_message)
}

pub fn handle_auto_close_due_polls(state: &Data<AppState>) -> Result<(), ApiError> {
    let now = Utc::now().timestamp();
    let mut conn = lock_db(state)?;

    let due_rows: Vec<(i64, String, String, String, String, i64, i64, String, i64, Option<i64>, Option<String>, Option<i64>, Option<i64>, Option<String>, Option<String>)> = {
        let mut stmt = conn
            .prepare(
                "SELECT p.bot_id, b.token, p.id, p.question, p.options_json, p.total_voter_count, p.is_anonymous, p.poll_type,
                        p.allows_multiple_answers, p.correct_option_id, p.explanation, p.open_period, p.close_date,
                        m.question_entities_json, m.explanation_entities_json
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
                    r.get::<_, Option<i64>>(9)?,
                    r.get::<_, Option<String>>(10)?,
                    r.get::<_, Option<i64>>(11)?,
                    r.get::<_, Option<i64>>(12)?,
                    r.get::<_, Option<String>>(13)?,
                    r.get::<_, Option<String>>(14)?,
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
        correct_option_id,
        explanation,
        open_period,
        close_date,
        question_entities_json,
        explanation_entities_json,
    ) in due_rows
    {
        conn.execute(
            "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2 AND is_closed = 0",
            params![poll_id, bot_id],
        )
        .map_err(ApiError::internal)?;

        let options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
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
            correct_option_id,
            explanation,
            explanation_entities: explanation_entities_json
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok()),
            open_period,
            close_date,
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
        })
        .map_err(ApiError::internal)?;

        persist_and_dispatch_update(state, &mut conn, &token, bot_id, update_value)?;
    }

    Ok(())
}

pub fn handle_sim_vote_poll(
    state: &Data<AppState>,
    token: &str,
    body: SimVotePollRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let row: Option<(String, String, String, i64, i64, i64, String, i64, Option<i64>, Option<String>, Option<i64>, Option<i64>, i64, Option<String>, Option<String>)> = conn
        .query_row(
            "SELECT p.id, p.question, p.options_json, p.total_voter_count, p.is_closed, p.is_anonymous, p.poll_type, p.allows_multiple_answers, p.correct_option_id, p.explanation, p.open_period, p.close_date, p.created_at,
                    m.question_entities_json, m.explanation_entities_json
             FROM polls p
             LEFT JOIN poll_metadata m ON m.poll_id = p.id
             WHERE p.bot_id = ?1 AND p.chat_key = ?2 AND p.message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?, r.get(10)?, r.get(11)?, r.get(12)?, r.get(13)?, r.get(14)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((poll_id, question, options_json, _total_voter_count, is_closed, is_anonymous, poll_type, allows_multiple_answers, correct_option_id, explanation, open_period, close_date, created_at, question_entities_json, explanation_entities_json)) = row else {
        return Err(ApiError::not_found("poll not found"));
    };

    let now = Utc::now().timestamp();
    let auto_closed = close_date.map(|ts| now >= ts).unwrap_or(false)
        || open_period.map(|p| now >= created_at + p).unwrap_or(false);

    if is_closed == 1 || auto_closed {
        if auto_closed && is_closed == 0 {
            conn.execute(
                "UPDATE polls SET is_closed = 1 WHERE id = ?1 AND bot_id = ?2",
                params![poll_id, bot.id],
            )
            .map_err(ApiError::internal)?;
        }
        return Err(ApiError::bad_request("poll is closed"));
    }

    if poll_type == "quiz" {
        if body.option_ids.is_empty() {
            return Err(ApiError::bad_request("quiz polls do not allow vote retraction"));
        }
        if body.option_ids.len() != 1 {
            return Err(ApiError::bad_request("quiz polls accept exactly one option"));
        }

        let existing_vote: Option<String> = conn
            .query_row(
                "SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
                params![poll_id, user.id],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if existing_vote.is_some() {
            return Err(ApiError::bad_request("quiz vote is final and cannot be changed"));
        }
    }

    let mut options: Vec<PollOption> = serde_json::from_str(&options_json).map_err(ApiError::internal)?;
    let max_index = options.len() as i64;
    if body.option_ids.iter().any(|v| *v < 0 || *v >= max_index) {
        return Err(ApiError::bad_request("option_ids contains invalid index"));
    }

    if allows_multiple_answers == 0 && body.option_ids.len() > 1 {
        return Err(ApiError::bad_request("poll accepts only one option"));
    }

    if poll_type == "quiz" && allows_multiple_answers == 1 {
        return Err(ApiError::bad_request("quiz poll cannot allow multiple answers"));
    }

    if body.option_ids.is_empty() {
        conn.execute(
            "DELETE FROM poll_votes WHERE poll_id = ?1 AND voter_user_id = ?2",
            params![poll_id, user.id],
        )
        .map_err(ApiError::internal)?;
    } else {
        conn.execute(
            "INSERT OR REPLACE INTO poll_votes (poll_id, voter_user_id, option_ids_json, updated_at)
             VALUES (?1, ?2, ?3, ?4)",
            params![
                poll_id,
                user.id,
                serde_json::to_string(&body.option_ids).map_err(ApiError::internal)?,
                Utc::now().timestamp(),
            ],
        )
        .map_err(ApiError::internal)?;
    }

    let (total_voter_count, counts) = {
        let mut total_voter_count: i64 = 0;
        let mut counts = vec![0i64; options.len()];
        let mut stmt = conn
            .prepare("SELECT option_ids_json FROM poll_votes WHERE poll_id = ?1")
            .map_err(ApiError::internal)?;
        let rows = stmt
            .query_map(params![poll_id], |r| r.get::<_, String>(0))
            .map_err(ApiError::internal)?;

        for row in rows {
            let raw = row.map_err(ApiError::internal)?;
            let ids: Vec<i64> = serde_json::from_str(&raw).unwrap_or_default();
            total_voter_count += 1;
            for id in ids {
                if let Some(slot) = counts.get_mut(id as usize) {
                    *slot += 1;
                }
            }
        }

        (total_voter_count, counts)
    };

    for (idx, option) in options.iter_mut().enumerate() {
        option.voter_count = counts[idx];
    }

    conn.execute(
        "UPDATE polls SET options_json = ?1, total_voter_count = ?2 WHERE id = ?3",
        params![serde_json::to_string(&options).map_err(ApiError::internal)?, total_voter_count, poll_id],
    )
    .map_err(ApiError::internal)?;

    let poll = Poll {
        id: poll_id.clone(),
        question,
        question_entities: question_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        options,
        total_voter_count,
        is_closed: false,
        is_anonymous: is_anonymous == 1,
        r#type: poll_type,
        allows_multiple_answers: allows_multiple_answers == 1,
        correct_option_id,
        explanation,
        explanation_entities: explanation_entities_json
            .as_deref()
            .and_then(|raw| serde_json::from_str(raw).ok()),
        open_period,
        close_date,
    };

    let poll_update = serde_json::to_value(Update {
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
        poll: Some(poll.clone()),
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_update)?;

    if is_anonymous == 1 {
        return Ok(serde_json::to_value(true).map_err(ApiError::internal)?);
    }

    let poll_answer_update = serde_json::to_value(Update {
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
        poll_answer: Some(PollAnswer {
            poll_id,
            voter_chat: None,
            user: Some(User {
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
            }),
            option_ids: body.option_ids,
        }),
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, poll_answer_update)?;

    Ok(serde_json::to_value(true).map_err(ApiError::internal)?)
}

pub fn handle_sim_pay_invoice(
    state: &Data<AppState>,
    token: &str,
    body: SimPayInvoiceRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;
    let chat_key = body.chat_id.to_string();

    let invoice_row: Option<(String, String, String, i64, i64, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "SELECT title, payload, currency, total_amount, need_shipping_address, is_flexible, max_tip_amount,
                    need_name, need_phone_number, need_email
             FROM invoices
             WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?, r.get(5)?, r.get(6)?, r.get(7)?, r.get(8)?, r.get(9)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((invoice_title, invoice_payload, currency_raw, invoice_total_amount, need_shipping_address, is_flexible, max_tip_amount, need_name, need_phone_number, need_email)) = invoice_row else {
        return Err(ApiError::not_found("invoice not found"));
    };
    let currency = currency_raw.trim().to_ascii_uppercase();
    let is_stars_invoice = currency == "XTR";

    let payment_method = body
        .payment_method
        .unwrap_or_else(|| "wallet".to_string())
        .trim()
        .to_ascii_lowercase();
    let outcome = body
        .outcome
        .unwrap_or_else(|| "success".to_string())
        .trim()
        .to_ascii_lowercase();

    if outcome != "success" && outcome != "failed" {
        return Err(ApiError::bad_request("outcome must be success or failed"));
    }

    if payment_method != "wallet" && payment_method != "card" && payment_method != "stars" {
        return Err(ApiError::bad_request("payment_method must be wallet, card, or stars"));
    }

    if is_stars_invoice && payment_method != "stars" {
        return Err(ApiError::bad_request("Telegram Stars invoice must be paid using payment_method=stars"));
    }

    if !is_stars_invoice && payment_method == "stars" {
        return Err(ApiError::bad_request("Non-Stars invoice cannot be paid using payment_method=stars"));
    }

    let tip_amount = body.tip_amount.unwrap_or(0);
    if tip_amount < 0 {
        return Err(ApiError::bad_request("tip_amount must be non-negative"));
    }
    if is_stars_invoice && tip_amount > 0 {
        return Err(ApiError::bad_request("tip_amount is not supported for Telegram Stars invoices"));
    }
    if tip_amount > max_tip_amount {
        return Err(ApiError::bad_request("tip_amount exceeds invoice max_tip_amount"));
    }
    let total_amount = invoice_total_amount
        .checked_add(tip_amount)
        .ok_or_else(|| ApiError::bad_request("total amount overflow"))?;

    let from_user = User {
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

    let now = Utc::now().timestamp();
    let mut selected_shipping_option_id: Option<String> = None;

    if need_shipping_address == 1 {
        let shipping_query_id = generate_telegram_numeric_id();
        let shipping_address = ShippingAddress {
            country_code: "US".to_string(),
            state: "CA".to_string(),
            city: "San Francisco".to_string(),
            street_line1: "Market Street".to_string(),
            street_line2: "Suite 100".to_string(),
            post_code: "94103".to_string(),
        };

        conn.execute(
            "INSERT INTO shipping_queries
             (id, bot_id, chat_key, from_user_id, payload, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                shipping_query_id,
                bot.id,
                chat_key,
                user.id,
                invoice_payload,
                now,
            ],
        )
        .map_err(ApiError::internal)?;

        let shipping_update = serde_json::to_value(Update {
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
            shipping_query: Some(ShippingQuery {
                id: shipping_query_id.clone(),
                from: from_user.clone(),
                invoice_payload: invoice_payload.clone(),
                shipping_address,
            }),
            pre_checkout_query: None,
            purchased_paid_media: None,
            poll: None,
            poll_answer: None,
            my_chat_member: None,
            chat_member: None,
            chat_join_request: None,
            chat_boost: None,
            removed_chat_boost: None,
        })
        .map_err(ApiError::internal)?;
        persist_and_dispatch_update(state, &mut conn, token, bot.id, shipping_update)?;

        let mut answer_json: Option<String> = None;
        for _ in 0..15 {
            answer_json = conn
                .query_row(
                    "SELECT COALESCE(answer_json, '') FROM shipping_queries WHERE id = ?1 AND bot_id = ?2",
                    params![shipping_query_id, bot.id],
                    |r| r.get(0),
                )
                .optional()
                .map_err(ApiError::internal)?;

            if answer_json.as_ref().map(|value| !value.trim().is_empty()).unwrap_or(false) {
                break;
            }

            std::thread::sleep(Duration::from_millis(120));
        }

        let Some(answer_raw) = answer_json.filter(|value| !value.trim().is_empty()) else {
            return Err(ApiError::bad_request("shipping_query pending; call answerShippingQuery, then retry payment"));
        };

        let shipping_answer: AnswerShippingQueryRequest = serde_json::from_str(&answer_raw)
            .map_err(|_| ApiError::bad_request("invalid answerShippingQuery payload"))?;

        if !shipping_answer.ok {
            return Err(ApiError::bad_request(
                shipping_answer
                    .error_message
                    .unwrap_or_else(|| "shipping query was rejected".to_string()),
            ));
        }

        selected_shipping_option_id = shipping_answer
            .shipping_options
            .as_ref()
            .and_then(|options| options.first())
            .map(|option| option.id.clone());

        if is_flexible == 1 && selected_shipping_option_id.is_none() {
            return Err(ApiError::bad_request("flexible shipping requires at least one shipping option in answerShippingQuery"));
        }
    }

    let pre_checkout_query_id = generate_telegram_numeric_id();

    let order_info = if need_name == 1 || need_phone_number == 1 || need_email == 1 || need_shipping_address == 1 {
        Some(OrderInfo {
            name: if need_name == 1 { Some(user.first_name.clone()) } else { None },
            phone_number: if need_phone_number == 1 { Some("+10000000000".to_string()) } else { None },
            email: if need_email == 1 {
                Some(format!("{}@laragram.local", user.username.clone().unwrap_or_else(|| format!("user{}", user.id))))
            } else {
                None
            },
            shipping_address: if need_shipping_address == 1 {
                Some(ShippingAddress {
                    country_code: "US".to_string(),
                    state: "CA".to_string(),
                    city: "San Francisco".to_string(),
                    street_line1: "Market Street".to_string(),
                    street_line2: "Suite 100".to_string(),
                    post_code: "94103".to_string(),
                })
            } else {
                None
            },
        })
    } else {
        None
    };

    conn.execute(
        "INSERT INTO pre_checkout_queries
         (id, bot_id, chat_key, from_user_id, payload, currency, total_amount, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![
            pre_checkout_query_id,
            bot.id,
            chat_key,
            user.id,
            invoice_payload,
            currency,
            total_amount,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    let pre_checkout_update = serde_json::to_value(Update {
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
        pre_checkout_query: Some(PreCheckoutQuery {
            id: pre_checkout_query_id.clone(),
            from: from_user.clone(),
            currency: currency.clone(),
            total_amount,
            invoice_payload: invoice_payload.clone(),
            shipping_option_id: selected_shipping_option_id.clone(),
            order_info: order_info.clone(),
        }),
        purchased_paid_media: None,
        poll: None,
        poll_answer: None,
        my_chat_member: None,
        chat_member: None,
        chat_join_request: None,
        chat_boost: None,
        removed_chat_boost: None,
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, pre_checkout_update)?;

    if outcome == "failed" {
        return Ok(json!({
            "status": "failed",
            "pre_checkout_query_id": pre_checkout_query_id,
            "payment_method": payment_method,
        }));
    }

    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, body.chat_id.to_string(), user.id, format!("Paid: {}", invoice_title), now],
    )
    .map_err(ApiError::internal)?;

    let paid_message_id = conn.last_insert_rowid();
    let mut paid_message = load_message_value(&mut conn, &bot, paid_message_id)?;
    paid_message.as_object_mut().map(|obj| obj.remove("text"));

    let successful_payment = SuccessfulPayment {
        currency,
        total_amount,
        invoice_payload,
        subscription_expiration_date: None,
        is_recurring: None,
        is_first_recurring: None,
        shipping_option_id: selected_shipping_option_id,
        order_info,
        telegram_payment_charge_id: format!("tg_{}_{}", payment_method, generate_telegram_numeric_id()),
        provider_payment_charge_id: format!("provider_{}_{}", payment_method, generate_telegram_numeric_id()),
    };

    if payment_method == "stars" {
        conn.execute(
            "INSERT INTO star_transactions_ledger
             (id, bot_id, user_id, telegram_payment_charge_id, amount, date, kind)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'payment')",
            params![
                format!("pay_{}", generate_telegram_numeric_id()),
                bot.id,
                user.id,
                successful_payment.telegram_payment_charge_id.clone(),
                total_amount,
                now,
            ],
        )
        .map_err(ApiError::internal)?;
    }

    paid_message["successful_payment"] = serde_json::to_value(successful_payment).map_err(ApiError::internal)?;

    let paid_update = serde_json::to_value(Update {
        update_id: 0,
        message: Some(serde_json::from_value(paid_message.clone()).map_err(ApiError::internal)?),
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
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, paid_update)?;

    conn.execute(
        "UPDATE invoices SET paid_at = ?1 WHERE bot_id = ?2 AND chat_key = ?3 AND message_id = ?4",
        params![now, bot.id, body.chat_id.to_string(), body.message_id],
    )
    .map_err(ApiError::internal)?;

    Ok(json!({
        "status": "success",
        "pre_checkout_query_id": pre_checkout_query_id,
        "message_id": paid_message_id,
        "payment_method": payment_method,
    }))
}

fn send_media_message(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
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
    )
}

fn send_media_message_with_group(
    state: &Data<AppState>,
    token: &str,
    chat_id_value: &Value,
    caption: Option<String>,
    caption_entities: Option<Value>,
    reply_markup: Option<Value>,
    media_field: &str,
    media_payload: Value,
    media_group_id: Option<&str>,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let chat_key = value_to_chat_key(chat_id_value)?;
    ensure_chat(&mut conn, &chat_key)?;

    let resolved_reply_markup = handle_reply_markup_state(
        &mut conn,
        bot.id,
        &chat_key,
        reply_markup.as_ref(),
    )?;

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
    let mut message_value = serde_json::to_value(&message).map_err(ApiError::internal)?;
    if let Some(markup) = resolved_reply_markup {
        message_value["reply_markup"] = markup;
    }

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
    let file_id = generate_telegram_file_id("file");
    let file_unique_id = generate_telegram_file_unique_id();
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

    let chat_fragment = format!("\"chat\":{{\"id\":{}", body.chat_id);
    conn.execute(
        "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2",
        params![bot.id, format!("%{}%", chat_fragment)],
    )
    .map_err(ApiError::internal)?;

    conn.execute(
        "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2",
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

pub fn handle_sim_press_inline_button(
    state: &Data<AppState>,
    token: &str,
    body: SimPressInlineButtonRequest,
) -> ApiResult {
    if body.callback_data.trim().is_empty() {
        return Err(ApiError::bad_request("callback_data is empty"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_key = body.chat_id.to_string();
    let message_value = load_message_value(&mut conn, &bot, body.message_id)?;

    let exists: Option<i64> = conn
        .query_row(
            "SELECT message_id FROM messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, body.message_id],
            |r| r.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if exists.is_none() {
        return Err(ApiError::not_found("message not found"));
    }

    let callback_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();

    let callback_from = User {
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

    let is_inline_origin = message_value
        .get("via_bot")
        .and_then(|v| v.get("id"))
        .and_then(Value::as_i64)
        == Some(bot.id);

    let inline_message_id = if is_inline_origin {
        let existing: Option<String> = conn
            .query_row(
                "SELECT inline_message_id FROM inline_messages WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
                params![bot.id, chat_key, body.message_id],
                |r| r.get(0),
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(existing_id) = existing {
            Some(existing_id)
        } else {
            let generated = generate_telegram_numeric_id();
            conn.execute(
                "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![generated, bot.id, chat_key, body.message_id, now],
            )
            .map_err(ApiError::internal)?;
            Some(generated)
        }
    } else {
        None
    };

    let callback_message: Option<MaybeInaccessibleMessage> = if inline_message_id.is_some() {
        None
    } else {
        Some(serde_json::from_value(message_value).map_err(ApiError::internal)?)
    };

    let callback_query = CallbackQuery {
        id: callback_query_id.clone(),
        from: callback_from,
        message: callback_message,
        inline_message_id,
        chat_instance: generate_telegram_numeric_id(),
        data: Some(body.callback_data.clone()),
        game_short_name: None,
    };

    conn.execute(
        "INSERT INTO callback_queries (id, bot_id, chat_key, message_id, from_user_id, data, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![callback_query_id, bot.id, chat_key, body.message_id, user.id, body.callback_data, now],
    )
    .map_err(ApiError::internal)?;

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
        callback_query: Some(callback_query),
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
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "ok": true,
        "callback_query_id": callback_query_id,
    }))
}

pub fn handle_sim_send_inline_query(
    state: &Data<AppState>,
    token: &str,
    body: SimSendInlineQueryRequest,
) -> ApiResult {
    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let user = ensure_user(&mut conn, body.user_id, body.first_name, body.username)?;

    let chat_id = body.chat_id.unwrap_or(user.id);
    let chat_key = chat_id.to_string();
    ensure_chat(&mut conn, &chat_key)?;

    let inline_query_id = generate_telegram_numeric_id();
    let now = Utc::now().timestamp();
    let query_text = body.query;
    let offset = body.offset.unwrap_or_default();

    let cached_answer_row: Option<(String, i64)> = conn
        .query_row(
            "SELECT answer_json, expires_at
             FROM inline_query_cache
             WHERE bot_id = ?1 AND query = ?2 AND offset = ?3
                             AND (from_user_id = -1 OR from_user_id = ?4)
                         ORDER BY CASE WHEN from_user_id = ?4 THEN 0 ELSE 1 END
             LIMIT 1",
            params![bot.id, query_text, offset, user.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if let Some((cached_answer_json, expires_at)) = cached_answer_row {
        if expires_at >= now {
            conn.execute(
                "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at, answered_at, answer_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
                params![
                    inline_query_id,
                    bot.id,
                    chat_key,
                    user.id,
                    query_text,
                    offset,
                    now,
                    now,
                    cached_answer_json,
                ],
            )
            .map_err(ApiError::internal)?;

            return Ok(json!({
                "inline_query_id": inline_query_id,
                "cached": true,
            }));
        }
    }

    let inline_from = User {
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

    let inline_query = InlineQuery {
        id: inline_query_id.clone(),
        from: inline_from,
        query: query_text.clone(),
        offset: offset.clone(),
        chat_type: Some("private".to_string()),
        location: None,
    };

    conn.execute(
        "INSERT INTO inline_queries (id, bot_id, chat_key, from_user_id, query, offset, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        params![
            inline_query_id,
            bot.id,
            chat_key,
            user.id,
            query_text,
            offset,
            now
        ],
    )
    .map_err(ApiError::internal)?;

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
        inline_query: Some(inline_query.clone()),
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
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, &mut conn, token, bot.id, update_value)?;

    Ok(json!({
        "inline_query_id": inline_query_id,
        "cached": false,
    }))
}

pub fn handle_sim_get_inline_query_answer(
    state: &Data<AppState>,
    token: &str,
    inline_query_id: &str,
) -> ApiResult {
    if inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "inline_query_id": inline_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

pub fn handle_sim_get_callback_query_answer(
    state: &Data<AppState>,
    token: &str,
    callback_query_id: &str,
) -> ApiResult {
    if callback_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("callback_query_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT answer_json, answered_at FROM callback_queries WHERE id = ?1 AND bot_id = ?2",
            params![callback_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((answer_json, answered_at)) = row else {
        return Err(ApiError::not_found("callback query not found"));
    };

    let parsed = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str::<Value>(raw).ok());

    Ok(json!({
        "callback_query_id": callback_query_id,
        "answered": answered_at.is_some(),
        "answered_at": answered_at,
        "answer": parsed,
    }))
}

pub fn handle_sim_choose_inline_result(
    state: &Data<AppState>,
    token: &str,
    body: SimChooseInlineResultRequest,
) -> ApiResult {
    if body.inline_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("inline_query_id is required"));
    }
    if body.result_id.trim().is_empty() {
        return Err(ApiError::bad_request("result_id is required"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;

    let row: Option<(String, i64, String, Option<String>)> = conn
        .query_row(
            "SELECT chat_key, from_user_id, query, answer_json FROM inline_queries WHERE id = ?1 AND bot_id = ?2",
            params![body.inline_query_id, bot.id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;

    let Some((chat_key, from_user_id, query_text, answer_json)) = row else {
        return Err(ApiError::not_found("inline query not found"));
    };

    let answer_value: Value = answer_json
        .as_deref()
        .and_then(|raw| serde_json::from_str(raw).ok())
        .ok_or_else(|| ApiError::bad_request("inline query has no answer yet"))?;

    let results = answer_value
        .get("results")
        .and_then(Value::as_array)
        .ok_or_else(|| ApiError::bad_request("inline query answer has no results"))?;

    let selected = results
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(body.result_id.as_str()))
        .or_else(|| results.first())
        .ok_or_else(|| ApiError::bad_request("inline query answer has empty results"))?;

    let message_text = selected
        .get("input_message_content")
        .and_then(|c| c.get("message_text"))
        .and_then(Value::as_str)
        .map(|v| v.to_string())
        .or_else(|| selected.get("title").and_then(Value::as_str).map(|v| v.to_string()))
        .or_else(|| selected.get("description").and_then(Value::as_str).map(|v| v.to_string()))
        .unwrap_or_else(|| "inline result".to_string());

    ensure_chat(&mut conn, &chat_key)?;
    let now = Utc::now().timestamp();
    conn.execute(
        "INSERT INTO messages (bot_id, chat_key, from_user_id, text, date) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![bot.id, chat_key, from_user_id, message_text, now],
    )
    .map_err(ApiError::internal)?;

    let message_id = conn.last_insert_rowid();
    let chat_id = chat_key
        .parse::<i64>()
        .unwrap_or_else(|_| fallback_chat_id(&chat_key));

    let user_info: Option<(String, Option<String>)> = conn
        .query_row(
            "SELECT first_name, username FROM users WHERE id = ?1",
            params![from_user_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()
        .map_err(ApiError::internal)?;
    let (first_name, username) = user_info.unwrap_or_else(|| ("User".to_string(), None));

    let message_payload = json!({
        "message_id": message_id,
        "date": now,
        "chat": {
            "id": chat_id,
            "type": "private"
        },
        "from": {
            "id": from_user_id,
            "is_bot": false,
            "first_name": first_name,
            "username": username
        },
        "text": message_text,
        "via_bot": {
            "id": bot.id,
            "is_bot": true,
            "first_name": bot.first_name,
            "username": bot.username
        }
    });
    let message_for_update: Message = serde_json::from_value(message_payload).map_err(ApiError::internal)?;
    let message_update = serde_json::to_value(Update {
        update_id: 0,
        message: Some(message_for_update),
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
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, message_update)?;

    let chosen_from = User {
        id: from_user_id,
        is_bot: false,
        first_name: first_name.clone(),
        last_name: None,
        username: username.clone(),
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
    let inline_message_id = generate_telegram_numeric_id();
    conn.execute(
        "INSERT OR REPLACE INTO inline_messages (inline_message_id, bot_id, chat_key, message_id, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5)",
        params![inline_message_id, bot.id, chat_key, message_id, now],
    )
    .map_err(ApiError::internal)?;

    let chosen_inline_result_update = serde_json::to_value(Update {
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
        chosen_inline_result: Some(ChosenInlineResult {
            result_id: body.result_id.clone(),
            from: chosen_from,
            location: None,
            inline_message_id: Some(inline_message_id),
            query: query_text,
        }),
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
    })
    .map_err(ApiError::internal)?;
    persist_and_dispatch_update(state, &mut conn, token, bot.id, chosen_inline_result_update)?;

    Ok(json!({
        "message_id": message_id,
        "result_id": body.result_id,
    }))
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
            "SELECT update_id, update_json FROM updates
             WHERE bot_id = ?1 AND update_id >= ?2
             ORDER BY update_id ASC
             LIMIT ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, offset.max(1), limit], |row| {
            Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?))
        })
        .map_err(ApiError::internal)?;

    let fetched_rows: Vec<(i64, String)> = rows
        .collect::<Result<Vec<_>, _>>()
        .map_err(ApiError::internal)?;
    drop(stmt);

    let mut updates = Vec::new();
    let mut stale_update_ids = Vec::new();
    for (update_id, raw) in fetched_rows {
        let parsed: Value = serde_json::from_str(&raw).map_err(ApiError::internal)?;

        if update_targets_deleted_message(&mut conn, bot.id, &parsed)? {
            stale_update_ids.push(update_id);
            continue;
        }

        updates.push(parsed);
    }

    if !stale_update_ids.is_empty() {
        let placeholders = std::iter::repeat("?")
            .take(stale_update_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "DELETE FROM updates WHERE bot_id = ? AND update_id IN ({})",
            placeholders
        );

        let mut bind_values = Vec::with_capacity(1 + stale_update_ids.len());
        bind_values.push(Value::from(bot.id));
        for id in stale_update_ids {
            bind_values.push(Value::from(id));
        }

        let mut delete_stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
        delete_stmt
            .execute(rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)))
            .map_err(ApiError::internal)?;
    }

    Ok(Value::Array(updates))
}

fn update_targets_deleted_message(
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

fn resolve_edit_target(
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

    let chat_key = value_to_chat_key(&chat)?;
    Ok((chat_key, msg_id, false))
}

fn publish_edited_message_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    edited_message: &Value,
) -> Result<(), ApiError> {
    let mut edited_with_timestamp = edited_message.clone();
    edited_with_timestamp["edit_date"] = Value::from(Utc::now().timestamp());

    let update_value = serde_json::to_value(Update {
        update_id: 0,
        message: None,
        edited_message: Some(serde_json::from_value(edited_with_timestamp).map_err(ApiError::internal)?),
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
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, conn, token, bot_id, update_value)
}

fn handle_edit_message_text(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageTextRequest = parse_request(params)?;

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
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageText",
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

    let mut edited_message = load_message_value(&mut conn, &bot, message_id)?;
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);
    if let Some(entities) = parsed_entities {
        edited_message["entities"] = entities;
    }

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_media(
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
    let parse_mode = media_obj.get("parse_mode").and_then(Value::as_str);
    let (caption, caption_entities) = parse_optional_formatted_text(
        media_obj.get("caption").and_then(Value::as_str),
        parse_mode,
        explicit_caption_entities,
    );

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
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_caption(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: EditMessageCaptionRequest = parse_request(params)?;

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
    let (chat_key, message_id, via_inline_message) = resolve_edit_target(
        &mut conn,
        bot.id,
        request.chat_id.clone(),
        request.message_id,
        request.inline_message_id.clone(),
        "editMessageCaption",
    )?;

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
    edited_message.as_object_mut().map(|obj| obj.remove("text"));

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn handle_edit_message_reply_markup(
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
    apply_inline_reply_markup(&mut edited_message, request.reply_markup);

    publish_edited_message_update(state, &mut conn, token, bot.id, &edited_message)?;

    if via_inline_message {
        Ok(json!(true))
    } else {
        Ok(edited_message)
    }
}

fn apply_inline_reply_markup(target: &mut Value, reply_markup: Option<InlineKeyboardMarkup>) {
    if let Some(markup) = reply_markup {
        if let Ok(value) = serde_json::to_value(markup) {
            target["reply_markup"] = value;
        }
    } else {
        target.as_object_mut().map(|obj| obj.remove("reply_markup"));
    }
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

    if deleted > 0 {
        let chat_id_fragment = format!("\"chat\":{{\"id\":{}", chat_key);
        let message_id_fragment = format!("\"message_id\":{}", request.message_id);
        conn.execute(
            "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2 AND update_json LIKE ?3",
            params![
                bot.id,
                format!("%{}%", chat_id_fragment),
                format!("%{}%", message_id_fragment),
            ],
        )
        .map_err(ApiError::internal)?;

        conn.execute(
            "DELETE FROM invoices WHERE bot_id = ?1 AND chat_key = ?2 AND message_id = ?3",
            params![bot.id, chat_key, request.message_id],
        )
        .map_err(ApiError::internal)?;
    }

    Ok(Value::Bool(deleted > 0))
}

fn handle_delete_messages(
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
    let chat_key = value_to_chat_key(&request.chat_id)?;

    let placeholders = std::iter::repeat("?")
        .take(message_ids.len())
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "DELETE FROM messages WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
        placeholders
    );

    let mut bind_values = Vec::with_capacity(2 + message_ids.len());
    bind_values.push(Value::from(bot.id));
    bind_values.push(Value::from(chat_key.clone()));
    for id in &message_ids {
        bind_values.push(Value::from(*id));
    }

    let mut stmt = conn.prepare(&sql).map_err(ApiError::internal)?;
    let deleted = stmt
        .execute(rusqlite::params_from_iter(bind_values.iter().map(sql_value_to_rusqlite)))
        .map_err(ApiError::internal)?;

    if deleted > 0 {
        for message_id in &message_ids {
            let chat_id_fragment = format!("\"chat\":{{\"id\":{}", chat_key);
            let message_id_fragment = format!("\"message_id\":{}", message_id);
            conn.execute(
                "DELETE FROM updates WHERE bot_id = ?1 AND update_json LIKE ?2 AND update_json LIKE ?3",
                params![
                    bot.id,
                    format!("%{}%", chat_id_fragment),
                    format!("%{}%", message_id_fragment),
                ],
            )
            .map_err(ApiError::internal)?;
        }

        let placeholders = std::iter::repeat("?")
            .take(message_ids.len())
            .collect::<Vec<_>>()
            .join(",");
        let invoices_sql = format!(
            "DELETE FROM invoices WHERE bot_id = ? AND chat_key = ? AND message_id IN ({})",
            placeholders,
        );
        let mut invoice_bind_values = Vec::with_capacity(2 + message_ids.len());
        invoice_bind_values.push(Value::from(bot.id));
        invoice_bind_values.push(Value::from(chat_key));
        for id in &message_ids {
            invoice_bind_values.push(Value::from(*id));
        }

        let mut invoice_stmt = conn.prepare(&invoices_sql).map_err(ApiError::internal)?;
        invoice_stmt
            .execute(rusqlite::params_from_iter(invoice_bind_values.iter().map(sql_value_to_rusqlite)))
            .map_err(ApiError::internal)?;
    }

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

fn value_to_optional_bool_loose(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(v) => Some(*v),
        Value::Number(n) => {
            if n.as_i64() == Some(1) {
                Some(true)
            } else if n.as_i64() == Some(0) {
                Some(false)
            } else {
                None
            }
        }
        Value::String(raw) => {
            let normalized = raw.trim().to_ascii_lowercase();
            match normalized.as_str() {
                "1" | "true" | "yes" | "on" => Some(true),
                "0" | "false" | "no" | "off" | "" => Some(false),
                _ => None,
            }
        }
        _ => None,
    }
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

    let reaction_update = serde_json::to_value(Update {
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
    })
    .map_err(ApiError::internal)?;

    persist_and_dispatch_update(state, conn, token, bot.id, reaction_update)?;

    let reaction_count_update = serde_json::to_value(Update {
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
    })
    .map_err(ApiError::internal)?;

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
    message.as_object_mut().map(|obj| obj.remove("edit_date"));
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

fn handle_reply_markup_state(
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    chat_key: &str,
    reply_markup: Option<&Value>,
) -> Result<Option<Value>, ApiError> {
    let Some(markup_value) = reply_markup else {
        return Ok(None);
    };

    if markup_value.get("keyboard").is_some() {
        let parsed: ReplyKeyboardMarkup = serde_json::from_value(markup_value.clone())
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

fn generate_telegram_numeric_id() -> String {
    let ts = Utc::now().timestamp_micros().unsigned_abs();
    let bytes = uuid::Uuid::new_v4().as_bytes().to_vec();
    let mut mix: u64 = 0;
    for b in bytes.iter().take(8) {
        mix = (mix << 8) | u64::from(*b);
    }
    format!("{}{}", ts, mix % 1_000_000)
}

fn generate_telegram_file_id(kind: &str) -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}:{}", kind, Utc::now().timestamp_nanos_opt().unwrap_or_default(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AgACAgQAAxk{}", &hexed[..48])
}

fn generate_telegram_file_unique_id() -> String {
    use sha2::{Digest, Sha256};

    let raw = format!("{}:{}", Utc::now().timestamp_micros(), uuid::Uuid::new_v4());
    let mut hasher = Sha256::new();
    hasher.update(raw.as_bytes());
    let digest = hasher.finalize();
    let hexed = hex::encode(digest);
    format!("AQAD{}", &hexed[..24])
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

