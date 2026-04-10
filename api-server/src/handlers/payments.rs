use super::*;
use crate::generated::methods::{
    AnswerPreCheckoutQueryRequest, AnswerShippingQueryRequest, CreateInvoiceLinkRequest,
};

pub fn handle_answer_pre_checkout_query(
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

pub fn handle_answer_shipping_query(
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

pub fn handle_create_invoice_link(
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
    Ok(json!(format!("https://simula.local/invoice/{}/{}", bot.id, slug)))
}
