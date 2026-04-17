use actix_web::web::Data;
use chrono::Utc;
use rusqlite::params;
use serde_json::{json, Value};
use std::collections::HashMap;

use crate::database::{
    ensure_bot, lock_db, AppState
};

use crate::types::{ApiError, ApiResult};

use crate::generated::methods::SetPassportDataErrorsRequest;

use crate::handlers::client::{bot, users};

use crate::handlers::parse_request;

const PASSPORT_DATA_FIELD_TYPES: &[&str] = &[
    "personal_details",
    "passport",
    "driver_license",
    "identity_card",
    "internal_passport",
    "address",
];

const PASSPORT_FRONT_SIDE_TYPES: &[&str] = &[
    "passport",
    "driver_license",
    "identity_card",
    "internal_passport",
];

const PASSPORT_REVERSE_SIDE_TYPES: &[&str] = &[
    "driver_license",
    "identity_card",
];

const PASSPORT_FILE_TYPES: &[&str] = &[
    "utility_bill",
    "bank_statement",
    "rental_agreement",
    "passport_registration",
    "temporary_registration",
];

const PASSPORT_TRANSLATION_TYPES: &[&str] = &[
    "passport",
    "driver_license",
    "identity_card",
    "internal_passport",
    "utility_bill",
    "bank_statement",
    "rental_agreement",
    "passport_registration",
    "temporary_registration",
];

const PASSPORT_UNSPECIFIED_TYPES: &[&str] = &[
    "personal_details",
    "passport",
    "driver_license",
    "identity_card",
    "internal_passport",
    "address",
    "utility_bill",
    "bank_statement",
    "rental_agreement",
    "passport_registration",
    "temporary_registration",
    "phone_number",
    "email",
];

fn require_non_empty_string_field(value: &Value, field_name: &str) -> Result<String, ApiError> {
    let raw = value
        .get(field_name)
        .and_then(Value::as_str)
        .map(str::trim)
        .unwrap_or("");

    if raw.is_empty() {
        return Err(ApiError::bad_request(format!(
            "passport error {} is required",
            field_name,
        )));
    }

    Ok(raw.to_string())
}

fn ensure_allowed_type(element_type: &str, allowed: &[&str], source: &str) -> Result<(), ApiError> {
    if allowed.iter().any(|candidate| *candidate == element_type) {
        return Ok(());
    }

    Err(ApiError::bad_request(format!(
        "passport error type is invalid for source {}",
        source,
    )))
}

fn require_non_empty_string_array_field(value: &Value, field_name: &str) -> Result<(), ApiError> {
    let Some(items) = value.get(field_name).and_then(Value::as_array) else {
        return Err(ApiError::bad_request(format!(
            "passport error {} is required",
            field_name,
        )));
    };

    if items.is_empty() {
        return Err(ApiError::bad_request(format!(
            "passport error {} must not be empty",
            field_name,
        )));
    }

    for item in items {
        let text = item.as_str().map(str::trim).unwrap_or("");
        if text.is_empty() {
            return Err(ApiError::bad_request(format!(
                "passport error {} contains an invalid value",
                field_name,
            )));
        }
    }

    Ok(())
}

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
    let _ = users::ensure_sim_user_record(&mut conn, request.user_id)?;
    bot::ensure_sim_passport_data_errors_storage(&mut conn)?;

    let now = Utc::now().timestamp();
    let mut normalized_errors = Vec::<Value>::with_capacity(request.errors.len());
    for error in request.errors {
        let error_value = error.extra;
        let source = require_non_empty_string_field(&error_value, "source")?;
        let element_type = require_non_empty_string_field(&error_value, "type")?;
        let _message = require_non_empty_string_field(&error_value, "message")?;

        match source.as_str() {
            "data" => {
                ensure_allowed_type(&element_type, PASSPORT_DATA_FIELD_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "field_name")?;
                let _ = require_non_empty_string_field(&error_value, "data_hash")?;
            }
            "front_side" => {
                ensure_allowed_type(&element_type, PASSPORT_FRONT_SIDE_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "file_hash")?;
            }
            "reverse_side" => {
                ensure_allowed_type(&element_type, PASSPORT_REVERSE_SIDE_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "file_hash")?;
            }
            "selfie" => {
                ensure_allowed_type(&element_type, PASSPORT_FRONT_SIDE_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "file_hash")?;
            }
            "file" => {
                ensure_allowed_type(&element_type, PASSPORT_FILE_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "file_hash")?;
            }
            "files" => {
                ensure_allowed_type(&element_type, PASSPORT_FILE_TYPES, &source)?;
                require_non_empty_string_array_field(&error_value, "file_hashes")?;
            }
            "translation_file" => {
                ensure_allowed_type(&element_type, PASSPORT_TRANSLATION_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "file_hash")?;
            }
            "translation_files" => {
                ensure_allowed_type(&element_type, PASSPORT_TRANSLATION_TYPES, &source)?;
                require_non_empty_string_array_field(&error_value, "file_hashes")?;
            }
            "unspecified" => {
                ensure_allowed_type(&element_type, PASSPORT_UNSPECIFIED_TYPES, &source)?;
                let _ = require_non_empty_string_field(&error_value, "element_hash")?;
            }
            _ => {
                return Err(ApiError::bad_request(
                    "passport error source is invalid",
                ));
            }
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
