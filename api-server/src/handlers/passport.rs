use super::*;
use crate::generated::methods::SetPassportDataErrorsRequest;

pub fn handle_set_passport_data_errors(
    state: &Data<AppState>,
    token: &str,
    params: &HashMap<String, Value>,
) -> ApiResult {
    let request: SetPassportDataErrorsRequest = parse_request(params)?;
    if request.user_id <= 0 {
        return Err(ApiError::bad_request("user_id is invalid"));
    }
    if request.errors.is_empty() {
        return Err(ApiError::bad_request("errors must include at least one item"));
    }

    let mut conn = lock_db(state)?;
    let bot = ensure_bot(&mut conn, token)?;
    let _ = ensure_sim_user_record(&mut conn, request.user_id)?;
    ensure_sim_passport_data_errors_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let mut normalized_errors = Vec::<Value>::with_capacity(request.errors.len());
    for error in request.errors {
        let error_value = error.extra;
        let source = error_value
            .get("source")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let element_type = error_value
            .get("type")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");
        let message = error_value
            .get("message")
            .and_then(Value::as_str)
            .map(str::trim)
            .unwrap_or("");

        if source.is_empty() {
            return Err(ApiError::bad_request("passport error source is required"));
        }
        if element_type.is_empty() {
            return Err(ApiError::bad_request("passport error type is required"));
        }
        if message.is_empty() {
            return Err(ApiError::bad_request("passport error message is required"));
        }

        normalized_errors.push(error_value);
    }

    conn.execute(
        "INSERT INTO sim_passport_data_errors (bot_id, user_id, errors_json, updated_at)
         VALUES (?1, ?2, ?3, ?4)
         ON CONFLICT(bot_id, user_id)
         DO UPDATE SET errors_json = excluded.errors_json, updated_at = excluded.updated_at",
        params![
            bot.id,
            request.user_id,
            serde_json::to_string(&normalized_errors).map_err(ApiError::internal)?,
            now,
        ],
    )
    .map_err(ApiError::internal)?;

    Ok(json!(true))
}
