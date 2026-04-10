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
    dispatch_webhook_if_configured(state, conn, bot_id, clean_update.clone());

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
) {
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
    let state_for_log = state.clone();
    std::thread::spawn(move || {
        let started_at = Utc::now().timestamp_millis();
        let timer = std::time::Instant::now();
        let request_payload = json!({
            "url": url.clone(),
            "secret_token_set": !secret_token.is_empty(),
            "update": payload,
        });

        let client = match reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(4))
            .build()
        {
            Ok(client) => client,
            Err(error) => {
                push_runtime_request_log(
                    &state_for_log,
                    RuntimeRequestLogEntry {
                        id: uuid::Uuid::new_v4().to_string(),
                        at: started_at,
                        method: "POST".to_string(),
                        path: "/webhook/dispatch".to_string(),
                        query: None,
                        status: 599,
                        duration_ms: timer.elapsed().as_millis() as u64,
                        remote_addr: None,
                        request: Some(request_payload),
                        response: Some(json!({
                            "ok": false,
                            "description": format!("webhook client build failed: {}", error),
                        })),
                    },
                );
                return;
            }
        };

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

        push_runtime_request_log(
            &state_for_log,
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
    });
}
