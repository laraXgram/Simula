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
    AnswerPreCheckoutQueryRequest, AnswerShippingQueryRequest, CreateInvoiceLinkRequest,
    GetStarTransactionsRequest, RefundStarPaymentRequest,
};

use crate::generated::types::{
    StarTransaction, StarTransactions, TransactionPartner,
};

use crate::handlers::{parse_request, generate_telegram_numeric_id};

fn transaction_partner_other() -> TransactionPartner {
    TransactionPartner {
        extra: json!({ "type": "other" }),
    }
}

pub fn handle_answer_pre_checkout_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerPreCheckoutQueryRequest = parse_request(params)?;
    if request.pre_checkout_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("pre_checkout_query_id is required"));
    }

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

pub fn handle_answer_shipping_query(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: AnswerShippingQueryRequest = parse_request(params)?;
    if request.shipping_query_id.trim().is_empty() {
        return Err(ApiError::bad_request("shipping_query_id is required"));
    }

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

    if request.ok {
        let shipping_options = request
            .shipping_options
            .as_ref()
            .ok_or_else(|| ApiError::bad_request("shipping_options is required when ok is true"))?;
        if shipping_options.is_empty() {
            return Err(ApiError::bad_request(
                "shipping_options must include at least one item when ok is true",
            ));
        }

        for option in shipping_options {
            if option.id.trim().is_empty() {
                return Err(ApiError::bad_request("shipping option id is required"));
            }
            if option.title.trim().is_empty() {
                return Err(ApiError::bad_request("shipping option title is required"));
            }
            if option.prices.is_empty() {
                return Err(ApiError::bad_request(
                    "shipping option prices must include at least one item",
                ));
            }
            for price in &option.prices {
                if price.label.trim().is_empty() {
                    return Err(ApiError::bad_request("shipping option price label is required"));
                }
            }
        }
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

pub fn handle_create_invoice_link(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: CreateInvoiceLinkRequest = parse_request(params)?;
    if let Some(business_connection_id) = request.business_connection_id.as_deref() {
        if business_connection_id.trim().is_empty() {
            return Err(ApiError::bad_request("business_connection_id is invalid"));
        }
    }

    let title = request.title.trim();
    let description = request.description.trim();
    let payload = request.payload.trim();
    let max_tip_amount = request.max_tip_amount.unwrap_or(0);
    let suggested_tip_amounts = request.suggested_tip_amounts.clone().unwrap_or_default();

    let normalized_currency = request.currency.trim().to_ascii_uppercase();
    if title.is_empty() {
        return Err(ApiError::bad_request("title is empty"));
    }
    if title.chars().count() > 32 {
        return Err(ApiError::bad_request("title must be 1-32 characters"));
    }
    if description.is_empty() {
        return Err(ApiError::bad_request("description is empty"));
    }
    if description.chars().count() > 255 {
        return Err(ApiError::bad_request("description must be 1-255 characters"));
    }
    if payload.is_empty() {
        return Err(ApiError::bad_request("payload is empty"));
    }
    if payload.as_bytes().len() > 128 {
        return Err(ApiError::bad_request("payload must be 1-128 bytes"));
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

    let is_stars_invoice = normalized_currency == "XTR";
    let provider_token = request
        .provider_token
        .as_deref()
        .map(str::trim)
        .unwrap_or("");

    if request.subscription_period.is_some() && normalized_currency != "XTR" {
        return Err(ApiError::bad_request(
            "subscription_period is supported only for XTR invoices",
        ));
    }

    if let Some(subscription_period) = request.subscription_period {
        if subscription_period != 2_592_000 {
            return Err(ApiError::bad_request(
                "subscription_period must be 2592000 seconds",
            ));
        }
    }

    if request.is_flexible.unwrap_or(false)
        && !request.need_shipping_address.unwrap_or(false)
        && !is_stars_invoice
    {
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
    } else if provider_token.is_empty() {
        return Err(ApiError::bad_request("provider_token is required for non-Stars invoices"));
    }

    let mut total_amount = 0_i64;
    for price in &request.prices {
        if price.label.trim().is_empty() {
            return Err(ApiError::bad_request("price label is empty"));
        }

        total_amount = total_amount
            .checked_add(price.amount)
            .ok_or_else(|| ApiError::bad_request("price total overflow"))?;
    }

    if total_amount <= 0 {
        return Err(ApiError::bad_request("total invoice amount must be greater than zero"));
    }

    if is_stars_invoice {
        if request.prices[0].amount <= 0 {
            return Err(ApiError::bad_request(
                "XTR invoice price amount must be greater than zero",
            ));
        }

        if request.subscription_period.is_some() && request.prices[0].amount > 10_000 {
            return Err(ApiError::bad_request(
                "subscription price must not exceed 10000 Telegram Stars",
            ));
        }
    }

    if request.business_connection_id.is_some() && !is_stars_invoice {
        return Err(ApiError::bad_request(
            "business_connection_id is supported only for XTR invoices",
        ));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let slug = payload
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect::<String>();
    Ok(json!(format!("https://simula.local/invoice/{}/{}", bot.id, slug)))
}

pub fn handle_get_star_transactions(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: GetStarTransactionsRequest = parse_request(params)?;
    let offset = request.offset.unwrap_or(0).max(0);
    let limit = request.limit.unwrap_or(100).clamp(1, 100);

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let mut stmt = conn
        .prepare(
            "SELECT telegram_payment_charge_id, amount, date, kind
             FROM star_transactions_ledger
             WHERE bot_id = ?1
             ORDER BY date DESC
             LIMIT ?2 OFFSET ?3",
        )
        .map_err(ApiError::internal)?;

    let rows = stmt
        .query_map(params![bot.id, limit, offset], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, i64>(1)?,
                r.get::<_, i64>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(ApiError::internal)?;

    let mut transactions = Vec::<StarTransaction>::new();
    for row in rows {
        let (transaction_id, amount, date, kind) = row.map_err(ApiError::internal)?;
        let (source, receiver) = if kind == "refund" || amount < 0 {
            (None, Some(transaction_partner_other()))
        } else {
            (Some(transaction_partner_other()), None)
        };

        transactions.push(StarTransaction {
            id: transaction_id,
            amount,
            nanostar_amount: None,
            date,
            source,
            receiver,
        });
    }

    serde_json::to_value(StarTransactions { transactions }).map_err(ApiError::internal)
}

pub fn handle_refund_star_payment(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: RefundStarPaymentRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.telegram_payment_charge_id.trim().is_empty() {
        return Err(ApiError::bad_request("telegram_payment_charge_id is required"));
    }

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
