use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Map, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, ensure_chat, lock_db, AppState
};

use crate::types::{strip_nulls, ApiError, ApiResult};

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
        can_manage_bots: None,
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
        managed_bot: None,
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
                Some(format!("{}@simula.local", user.username.clone().unwrap_or_else(|| format!("user{}", user.id))))
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
        managed_bot: None,
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
    let is_channel_post = paid_message
        .get("chat")
        .and_then(|chat| chat.get("type"))
        .and_then(Value::as_str)
        == Some("channel");

    let paid_update = serde_json::to_value(Update {
        update_id: 0,
        message: if is_channel_post {
            None
        } else {
            Some(serde_json::from_value(paid_message.clone()).map_err(ApiError::internal)?)
        },
        edited_message: None,
        channel_post: if is_channel_post {
            Some(serde_json::from_value(paid_message.clone()).map_err(ApiError::internal)?)
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