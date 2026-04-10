use super::*;
use crate::generated::methods::{
    GetStarTransactionsRequest, RefundStarPaymentRequest,
};

pub fn handle_get_star_transactions(
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

pub fn handle_refund_star_payment(
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
