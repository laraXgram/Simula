use actix_web::web::Data;
use chrono::Utc;
use rusqlite::{params, OptionalExtension};
use serde_json::{json, Value};
use std::time::Duration;

use crate::database::{
    push_runtime_request_log, AppState, RuntimeRequestLogEntry
};

use crate::types::{strip_nulls, ApiError};

use crate::handlers::utils::updates::value_to_chat_key;

use super::channels;

const WEBHOOK_PENDING_NONE: i64 = 0;
const WEBHOOK_PENDING_READY: i64 = 1;
const WEBHOOK_PENDING_IN_FLIGHT: i64 = 2;
const DEFAULT_EXCLUDED_UPDATE_TYPES: [&str; 3] = [
    "chat_member",
    "message_reaction",
    "message_reaction_count",
];

fn parse_allowed_updates_json(raw: Option<String>) -> Option<Vec<String>> {
    let text = raw
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())?;

    let parsed = serde_json::from_str::<Vec<String>>(text).ok()?;
    let mut normalized = Vec::with_capacity(parsed.len());
    for item in parsed {
        let normalized_item = item.trim().to_ascii_lowercase();
        if normalized_item.is_empty() {
            continue;
        }
        if !normalized.iter().any(|existing| existing == &normalized_item) {
            normalized.push(normalized_item);
        }
    }

    Some(normalized)
}

fn detect_update_type(update: &Value) -> Option<&'static str> {
    if update.get("message").is_some() {
        return Some("message");
    }
    if update.get("edited_message").is_some() {
        return Some("edited_message");
    }
    if update.get("channel_post").is_some() {
        return Some("channel_post");
    }
    if update.get("edited_channel_post").is_some() {
        return Some("edited_channel_post");
    }
    if update.get("business_connection").is_some() {
        return Some("business_connection");
    }
    if update.get("business_message").is_some() {
        return Some("business_message");
    }
    if update.get("edited_business_message").is_some() {
        return Some("edited_business_message");
    }
    if update.get("deleted_business_messages").is_some() {
        return Some("deleted_business_messages");
    }
    if update.get("message_reaction").is_some() {
        return Some("message_reaction");
    }
    if update.get("message_reaction_count").is_some() {
        return Some("message_reaction_count");
    }
    if update.get("inline_query").is_some() {
        return Some("inline_query");
    }
    if update.get("chosen_inline_result").is_some() {
        return Some("chosen_inline_result");
    }
    if update.get("callback_query").is_some() {
        return Some("callback_query");
    }
    if update.get("shipping_query").is_some() {
        return Some("shipping_query");
    }
    if update.get("pre_checkout_query").is_some() {
        return Some("pre_checkout_query");
    }
    if update.get("purchased_paid_media").is_some() {
        return Some("purchased_paid_media");
    }
    if update.get("poll").is_some() {
        return Some("poll");
    }
    if update.get("poll_answer").is_some() {
        return Some("poll_answer");
    }
    if update.get("my_chat_member").is_some() {
        return Some("my_chat_member");
    }
    if update.get("chat_member").is_some() {
        return Some("chat_member");
    }
    if update.get("chat_join_request").is_some() {
        return Some("chat_join_request");
    }
    if update.get("chat_boost").is_some() {
        return Some("chat_boost");
    }
    if update.get("removed_chat_boost").is_some() {
        return Some("removed_chat_boost");
    }
    if update.get("managed_bot").is_some() {
        return Some("managed_bot");
    }

    None
}

fn is_update_type_allowed(update: &Value, allowed_updates: Option<&[String]>) -> bool {
    let Some(update_type) = detect_update_type(update) else {
        return true;
    };

    if let Some(allowed) = allowed_updates {
        if !allowed.is_empty() {
            return allowed.iter().any(|item| item == update_type);
        }
    }

    !DEFAULT_EXCLUDED_UPDATE_TYPES.contains(&update_type)
}

pub fn persist_and_dispatch_update(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    token: &str,
    bot_id: i64,
    mut update_value: Value,
) -> Result<(), ApiError> {
    channels::enrich_channel_post_payloads(conn, bot_id, &mut update_value)?;

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
    dispatch_webhook_if_configured(state, conn, bot_id, clean_update.clone(), Some(update_id));

    if let Some(channel_post_value) = clean_update.get("channel_post") {
        let bot_record: Option<crate::database::BotInfoRecord> = conn
            .query_row(
                "SELECT id, first_name, username FROM bots WHERE id = ?1",
                params![bot_id],
                |row| {
                    Ok(crate::database::BotInfoRecord {
                        id: row.get(0)?,
                        first_name: row.get(1)?,
                        username: row.get(2)?,
                    })
                },
            )
            .optional()
            .map_err(ApiError::internal)?;

        if let Some(bot_record) = bot_record {
            if let Some(chat_id_value) = channel_post_value
                .get("chat")
                .and_then(Value::as_object)
                .and_then(|chat| chat.get("id"))
            {
                let channel_chat_key = value_to_chat_key(chat_id_value)?;
                channels::forward_channel_post_to_linked_discussion_best_effort(
                    state,
                    conn,
                    token,
                    &bot_record,
                    &channel_chat_key,
                    channel_post_value,
                );
            }
        }
    }

    Ok(())
}

pub fn dispatch_webhook_if_configured(
    state: &Data<AppState>,
    conn: &mut rusqlite::Connection,
    bot_id: i64,
    update: Value,
    update_id: Option<i64>,
) {
    let webhook: Result<Option<(String, String, Option<Vec<String>>)>, ApiError> = conn
        .query_row(
            "SELECT url, secret_token, allowed_updates_json FROM webhooks WHERE bot_id = ?1",
            params![bot_id],
            |row| {
                let allowed_updates_json: Option<String> = row.get(2)?;
                Ok((
                    row.get(0)?,
                    row.get(1)?,
                    parse_allowed_updates_json(allowed_updates_json),
                ))
            },
        )
        .optional()
        .map_err(ApiError::internal);

    let Ok(Some((url, secret_token, allowed_updates))) = webhook else {
        return;
    };

    let payload = strip_nulls(update);
    if !is_update_type_allowed(&payload, allowed_updates.as_deref()) {
        if let Some(done_update_id) = update_id {
            let _ = conn.execute(
                "UPDATE updates SET webhook_pending = ?2 WHERE update_id = ?1",
                params![done_update_id, WEBHOOK_PENDING_NONE],
            );
        }
        return;
    }

    if let Some(pending_update_id) = update_id {
        let claimed = conn
            .execute(
                "UPDATE updates
                 SET webhook_pending = ?2
                 WHERE update_id = ?1 AND webhook_pending = ?3",
                params![
                    pending_update_id,
                    WEBHOOK_PENDING_IN_FLIGHT,
                    WEBHOOK_PENDING_READY,
                ],
            )
            .map(|changed| changed > 0)
            .unwrap_or(false);
        if !claimed {
            return;
        }
    }

    let state_for_log = state.clone();
    std::thread::spawn(move || {
        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                if let Some(pending_update_id) = update_id {
                    set_update_pending_state(&state_for_log, pending_update_id, WEBHOOK_PENDING_READY);
                }

                let started_at = Utc::now().timestamp_millis();
                push_runtime_request_log(
                    &state_for_log,
                    RuntimeRequestLogEntry {
                        id: uuid::Uuid::new_v4().to_string(),
                        at: started_at,
                        method: "POST".to_string(),
                        path: "/webhook/dispatch".to_string(),
                        query: None,
                        status: 599,
                        duration_ms: 0,
                        remote_addr: None,
                        request: Some(json!({
                            "bot_id": bot_id,
                            "url": url,
                            "secret_token_set": !secret_token.is_empty(),
                            "update": payload,
                        })),
                        response: Some(json!({
                            "ok": false,
                            "description": format!("webhook client build failed: {}", error),
                        })),
                    },
                );
                return;
            }
        };

        let delivered = dispatch_single_webhook_update(
            &state_for_log,
            &client,
            bot_id,
            &url,
            &secret_token,
            payload,
            update_id,
        );

        if delivered {
            retry_pending_webhooks_for_bot(&state_for_log, bot_id, 200);
        }
    });
}

pub fn schedule_pending_retry(state: &Data<AppState>, bot_id: i64) {
    let state_for_retry = state.clone();
    std::thread::spawn(move || {
        retry_pending_webhooks_for_bot(&state_for_retry, bot_id, 200);
    });
}

pub fn retry_all_pending_webhooks(state: &Data<AppState>, per_bot_limit: usize) {
    let bot_ids = match load_webhook_bot_ids(state) {
        Ok(bot_ids) => bot_ids,
        Err(_) => return,
    };

    for bot_id in bot_ids {
        retry_pending_webhooks_for_bot(state, bot_id, per_bot_limit);
    }
}

fn load_webhook_bot_ids(state: &Data<AppState>) -> Result<Vec<i64>, ApiError> {
    let db = state
        .db
        .lock()
        .map_err(|_| ApiError::internal("database lock poisoned"))?;

    let mut stmt = db
        .prepare("SELECT bot_id FROM webhooks ORDER BY bot_id ASC")
        .map_err(ApiError::internal)?;
    let rows = stmt
        .query_map([], |row| row.get::<_, i64>(0))
        .map_err(ApiError::internal)?;

    rows.collect::<Result<Vec<_>, _>>().map_err(ApiError::internal)
}

fn load_webhook_config(state: &Data<AppState>, bot_id: i64) -> Option<(String, String, Option<Vec<String>>)> {
    let Ok(db) = state.db.lock() else {
        return None;
    };

    db.query_row(
        "SELECT url, secret_token, allowed_updates_json FROM webhooks WHERE bot_id = ?1",
        params![bot_id],
        |row| {
            let allowed_updates_json: Option<String> = row.get(2)?;
            Ok((
                row.get(0)?,
                row.get(1)?,
                parse_allowed_updates_json(allowed_updates_json),
            ))
        },
    )
    .optional()
    .ok()
    .flatten()
}

fn load_next_pending_update(state: &Data<AppState>, bot_id: i64) -> Option<(i64, String)> {
    let Ok(db) = state.db.lock() else {
        return None;
    };

    db.query_row(
        "SELECT update_id, update_json FROM updates
         WHERE bot_id = ?1 AND bot_visible = 1 AND webhook_pending = ?2
         ORDER BY update_id ASC
         LIMIT 1",
        params![bot_id, WEBHOOK_PENDING_READY],
        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)),
    )
    .optional()
    .ok()
    .flatten()
}

fn claim_pending_update(state: &Data<AppState>, update_id: i64) -> bool {
    let Ok(db) = state.db.lock() else {
        return false;
    };

    db.execute(
        "UPDATE updates
         SET webhook_pending = ?2
         WHERE update_id = ?1 AND webhook_pending = ?3",
        params![update_id, WEBHOOK_PENDING_IN_FLIGHT, WEBHOOK_PENDING_READY],
    )
    .map(|changed| changed > 0)
    .unwrap_or(false)
}

fn set_update_pending_state(state: &Data<AppState>, update_id: i64, next_state: i64) {
    if let Ok(db) = state.db.lock() {
        let _ = db.execute(
            "UPDATE updates SET webhook_pending = ?2 WHERE update_id = ?1",
            params![update_id, next_state],
        );
    }
}

fn retry_pending_webhooks_for_bot(state: &Data<AppState>, bot_id: i64, per_bot_limit: usize) {
    let Some((url, secret_token, allowed_updates)) = load_webhook_config(state, bot_id) else {
        return;
    };

    let capped_limit = per_bot_limit.clamp(1, 500);
    let client = match reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(4))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };

    for _ in 0..capped_limit {
        let Some((update_id, raw_update_json)) = load_next_pending_update(state, bot_id) else {
            break;
        };

        if !claim_pending_update(state, update_id) {
            continue;
        }

        let parsed_update = match serde_json::from_str::<Value>(&raw_update_json) {
            Ok(value) => strip_nulls(value),
            Err(error) => {
                set_update_pending_state(state, update_id, WEBHOOK_PENDING_NONE);
                push_runtime_request_log(
                    state,
                    RuntimeRequestLogEntry {
                        id: uuid::Uuid::new_v4().to_string(),
                        at: Utc::now().timestamp_millis(),
                        method: "POST".to_string(),
                        path: "/webhook/dispatch".to_string(),
                        query: None,
                        status: 500,
                        duration_ms: 0,
                        remote_addr: None,
                        request: Some(json!({
                            "bot_id": bot_id,
                            "url": url,
                            "update_id": update_id,
                        })),
                        response: Some(json!({
                            "ok": false,
                            "description": format!("stored update payload is invalid JSON: {}", error),
                        })),
                    },
                );
                continue;
            }
        };

        if !is_update_type_allowed(&parsed_update, allowed_updates.as_deref()) {
            set_update_pending_state(state, update_id, WEBHOOK_PENDING_NONE);
            continue;
        }

        let delivered = dispatch_single_webhook_update(
            state,
            &client,
            bot_id,
            &url,
            &secret_token,
            parsed_update,
            Some(update_id),
        );

        if !delivered {
            break;
        }
    }
}

fn dispatch_single_webhook_update(
    state: &Data<AppState>,
    client: &reqwest::blocking::Client,
    bot_id: i64,
    url: &str,
    secret_token: &str,
    payload: Value,
    update_id: Option<i64>,
) -> bool {
    let started_at = Utc::now().timestamp_millis();
    let timer = std::time::Instant::now();
    let request_payload = json!({
        "bot_id": bot_id,
        "url": url,
        "secret_token_set": !secret_token.is_empty(),
        "update": payload,
    });

    let webhook_update = request_payload
        .get("update")
        .cloned()
        .unwrap_or(Value::Null);
    let mut request = client.post(url).json(&webhook_update);
    if !secret_token.is_empty() {
        request = request.header("X-Telegram-Bot-Api-Secret-Token", secret_token);
    }

    let (status, response_payload) = match request.send() {
        Ok(response) => {
            let status = response.status().as_u16();
            let response_ok = response.status().is_success();
            let mut response_body_text = response.text().unwrap_or_default();
            let mut truncated = false;
            if response_body_text.chars().count() > 4000 {
                response_body_text = response_body_text.chars().take(4000).collect::<String>();
                truncated = true;
            }
            let response_body_value = if response_body_text.trim().is_empty() {
                Value::Null
            } else {
                serde_json::from_str::<Value>(&response_body_text)
                    .unwrap_or_else(|_| Value::String(response_body_text))
            };
            (
                status,
                json!({
                    "ok": response_ok,
                    "status": status,
                    "body": response_body_value,
                    "truncated": truncated,
                }),
            )
        }
        Err(error) => {
            let status = error.status().map(|value| value.as_u16()).unwrap_or(599);
            (
                status,
                json!({
                    "ok": false,
                    "description": error.to_string(),
                }),
            )
        }
    };

    let delivered = status < 400;
    if let Some(done_update_id) = update_id {
        set_update_pending_state(
            state,
            done_update_id,
            if delivered {
                WEBHOOK_PENDING_NONE
            } else {
                WEBHOOK_PENDING_READY
            },
        );
    }

    push_runtime_request_log(
        state,
        RuntimeRequestLogEntry {
            id: uuid::Uuid::new_v4().to_string(),
            at: started_at,
            method: "POST".to_string(),
            path: "/webhook/dispatch".to_string(),
            query: None,
            status,
            duration_ms: timer.elapsed().as_millis() as u64,
            remote_addr: None,
            request: Some(request_payload),
            response: Some(response_payload),
        },
    );

    delivered
}
