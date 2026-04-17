use actix_multipart::Multipart;
use actix_web::{
    get, post, put,
    web::{self, Bytes, Data, Query},
    HttpRequest, HttpResponse, Responder,
};
use chrono::Utc;
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::json;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};
use rusqlite::{params, OptionalExtension};
use uuid::Uuid;

use crate::database::{
    clear_runtime_logs, clear_runtime_transition, is_api_enabled, push_runtime_request_log,
    runtime_logs_snapshot, set_api_enabled, lock_db, AppState, RuntimeRequestLogEntry,
};

use crate::handlers::{
    dispatch_method, 
    client::bot::handle_sim_create_bot,
    client::bot::handle_sim_get_privacy_mode,
    client::bot::handle_sim_set_privacy_mode,
    client::bot::handle_sim_update_bot,
    client::groups::handle_sim_create_group,
    client::groups::handle_sim_create_group_invite_link,
    client::groups::handle_sim_delete_group,
    client::groups::handle_sim_set_bot_group_membership,
    client::groups::handle_sim_join_group,
    client::groups::handle_sim_join_group_by_invite_link,
    client::groups::handle_sim_leave_group,
    client::groups::handle_sim_update_group,
    client::chats::handle_sim_decline_join_request,
    client::chats::handle_sim_approve_join_request,
    client::chats::handle_sim_bootstrap, 
    client::chats::handle_sim_add_user_chat_boosts,
    client::chats::handle_sim_remove_user_chat_boosts,
    client::channels::handle_sim_mark_channel_message_view,
    client::channels::handle_sim_open_channel_direct_messages,
    client::inlines::handle_sim_choose_inline_result,
    client::inlines::handle_sim_get_inline_query_answer,
    client::inlines::handle_sim_send_inline_query,
    client::inlines::handle_sim_press_inline_button,
    client::callbacks::handle_sim_get_callback_query_answer,
    client::messages::handle_sim_clear_history, 
    client::messages::handle_sim_edit_user_message_media, 
    client::messages::handle_sim_send_user_media, 
    client::messages::handle_sim_send_user_message,
    client::messages::handle_sim_send_user_contact, 
    client::messages::handle_sim_send_user_dice, 
    client::messages::handle_sim_send_user_game, 
    client::messages::handle_sim_send_user_location, 
    client::messages::handle_sim_send_user_venue,
    client::messages::handle_sim_set_user_reaction,
    client::messages::handle_download_file,
    client::messages::handle_sim_purchase_paid_media,
    client::business::handle_sim_remove_business_connection,
    client::business::handle_sim_set_business_connection,
    client::users::handle_sim_delete_user,
    client::users::handle_sim_delete_user_profile_audio,
    client::users::handle_sim_upload_user_profile_audio,
    client::users::handle_sim_upsert_user,
    client::users::handle_sim_set_user_profile_audio,
    client::payments::handle_sim_pay_invoice,
    client::gifts::handle_sim_delete_owned_gift,
    client::polls::handle_sim_get_poll_voters,
    client::polls::handle_sim_vote_poll,
    utils::updates::with_request_actor_user_id,
    client::types::inlines::SimChooseInlineResultRequest, client::types::messages::SimClearHistoryRequest,
    client::types::bot::SimCreateBotRequest, client::types::groups::SimCreateGroupInviteLinkRequest,
    client::types::groups::SimCreateGroupRequest, client::types::groups::SimDeleteGroupRequest,
    client::types::groups::SimJoinGroupByInviteLinkRequest, client::types::groups::SimJoinGroupRequest, 
    client::types::groups::SimLeaveGroupRequest, client::types::channels::SimMarkChannelMessageViewRequest, 
    client::types::payments::SimPayInvoiceRequest, client::types::payments::SimPurchasePaidMediaRequest, 
    client::types::inlines::SimPressInlineButtonRequest, client::types::channels::SimResolveJoinRequestRequest, 
    client::types::inlines::SimSendInlineQueryRequest, client::types::messages::SimSendUserMessageRequest, 
    client::types::groups::SimSetBotGroupMembershipRequest, client::types::messages::SimSetUserReactionRequest, 
    client::types::bot::SimUpdateBotRequest, client::types::groups::SimUpdateGroupRequest,
    client::types::messages::SimSendUserContactRequest, client::types::messages::SimSendUserDiceRequest, 
    client::types::messages::SimSendUserGameRequest, client::types::messages::SimSendUserLocationRequest, 
    client::types::messages::SimSendUserVenueRequest, client::types::users::SimDeleteUserRequest,
    client::types::users::SimDeleteUserProfileAudioRequest, client::types::users::SimAddUserChatBoostsRequest,
    client::types::users::SimRemoveUserChatBoostsRequest, client::types::users::SimSetUserProfileAudioRequest,
    client::types::bot::SimSetPrivacyModeRequest, client::types::business::SimRemoveBusinessConnectionRequest,
    client::types::business::SimSetBusinessConnectionRequest, client::types::channels::SimOpenChannelDirectMessagesRequest,
    client::types::messages::SimVotePollRequest, client::types::users::SimUpsertUserRequest,
    client::types::gifts::SimDeleteOwnedGiftRequest,
};
use crate::types::{into_telegram_response, strip_nulls, ApiError, ApiResult};

#[derive(Debug, Deserialize)]
pub struct RuntimeLogsQuery {
    pub limit: Option<usize>,
}

#[derive(Debug, Deserialize)]
pub struct RuntimePowerRequest {
    pub enabled: bool,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeEnvPatchRequest {
    pub values: BTreeMap<String, Option<String>>,
}

#[derive(Debug, Deserialize)]
pub struct RuntimeServiceActionRequest {
    pub action: String,
}

#[get("/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
}

#[get("/client-api/runtime/info")]
pub async fn runtime_info(state: Data<AppState>) -> impl Responder {
    let env_values = read_runtime_env_file().unwrap_or_else(|_| runtime_env_defaults());
    let workspace_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let workspace_dir = workspace_root.to_string_lossy().to_string();
    let env_file_path = runtime_env_file_path();
    let api_host = env_values
        .get("API_HOST")
        .cloned()
        .unwrap_or_else(|| "127.0.0.1".to_string());
    let api_port = env_values
        .get("API_PORT")
        .cloned()
        .unwrap_or_else(|| "8081".to_string());
    let database_raw = env_values
        .get("DATABASE_URL")
        .cloned()
        .unwrap_or_else(|| "simula.db".to_string());
    let storage_raw = env_values
        .get("FILE_STORAGE_DIR")
        .cloned()
        .unwrap_or_else(|| "files".to_string());
    let logs_raw = env_values
        .get("LOG_DIR")
        .cloned()
        .unwrap_or_else(|| "stdout".to_string());
    let service_snapshot = runtime_service_snapshot(&state);
    let api_enabled = service_snapshot
        .get("active")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| is_api_enabled(&state));

    HttpResponse::Ok().json(serde_json::json!({
        "status": "ok",
        "runtime": {
            "api_host": api_host,
            "api_port": api_port,
            "web_port": "8888",
            "database_path": resolve_runtime_path(&workspace_root, &database_raw),
            "storage_path": resolve_runtime_path(&workspace_root, &storage_raw),
            "logs_path": resolve_runtime_path(&workspace_root, &logs_raw),
            "workspace_dir": workspace_dir,
            "api_enabled": api_enabled,
            "env_file_path": env_file_path.to_string_lossy().to_string(),
            "env_values": env_values,
            "service": service_snapshot,
        }
    }))
}

#[get("/client-api/runtime/logs")]
pub async fn runtime_logs(state: Data<AppState>, query: Query<RuntimeLogsQuery>) -> impl Responder {
    let limit = query.limit.unwrap_or(200).clamp(1, 1000);
    HttpResponse::Ok().json(json!({
        "ok": true,
        "result": {
            "items": runtime_logs_snapshot(&state, limit),
            "api_enabled": is_api_enabled(&state),
        }
    }))
}

#[post("/client-api/runtime/logs/clear")]
pub async fn runtime_logs_clear(state: Data<AppState>) -> impl Responder {
    clear_runtime_logs(&state);
    HttpResponse::Ok().json(json!({
        "ok": true,
        "result": true,
    }))
}

#[get("/client-api/runtime/power")]
pub async fn runtime_power_status(state: Data<AppState>) -> impl Responder {
    let service_snapshot = runtime_service_snapshot(&state);
    let enabled = service_snapshot
        .get("active")
        .and_then(Value::as_bool)
        .unwrap_or_else(|| is_api_enabled(&state));

    HttpResponse::Ok().json(json!({
        "ok": true,
        "result": {
            "enabled": enabled,
            "service": service_snapshot,
        }
    }))
}

#[post("/client-api/runtime/power")]
pub async fn runtime_power_set(
    state: Data<AppState>,
    payload: web::Json<RuntimePowerRequest>,
) -> impl Responder {
    if !payload.enabled {
        return HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": 400,
            "description": "Runtime stop/start has been removed. Use restart from runtime service controls.",
        }));
    }

    match perform_runtime_service_action(&state, "restart") {
        Ok(service_snapshot) => {
            let enabled = service_snapshot
                .get("active")
                .and_then(Value::as_bool)
                .unwrap_or_else(|| is_api_enabled(&state));
            HttpResponse::Ok().json(json!({
                "ok": true,
                "result": {
                    "enabled": enabled,
                    "service": service_snapshot,
                }
            }))
        }
        Err(error) => HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": error.code,
            "description": error.description,
        })),
    }
}

#[get("/client-api/runtime/service")]
pub async fn runtime_service_status(state: Data<AppState>) -> impl Responder {
    HttpResponse::Ok().json(json!({
        "ok": true,
        "result": runtime_service_snapshot(&state),
    }))
}

#[post("/client-api/runtime/service")]
pub async fn runtime_service_action(
    state: Data<AppState>,
    payload: web::Json<RuntimeServiceActionRequest>,
) -> impl Responder {
    let action = payload.action.trim().to_ascii_lowercase();
    if action != "restart" {
        return HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": 400,
            "description": "Invalid service action. Only restart is supported.",
        }));
    }

    match perform_runtime_service_action(&state, &action) {
        Ok(service_snapshot) => HttpResponse::Ok().json(json!({
            "ok": true,
            "result": service_snapshot,
        })),
        Err(error) => HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": error.code,
            "description": error.description,
        })),
    }
}

#[get("/client-api/runtime/env")]
pub async fn runtime_env_get() -> impl Responder {
    match read_runtime_env_file() {
        Ok(values) => HttpResponse::Ok().json(json!({
            "ok": true,
            "result": {
                "file_path": runtime_env_file_path().to_string_lossy().to_string(),
                "values": values,
            }
        })),
        Err(error) => HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": error.code,
            "description": error.description,
        })),
    }
}

#[put("/client-api/runtime/env")]
pub async fn runtime_env_set(payload: web::Json<RuntimeEnvPatchRequest>) -> impl Responder {
    let mut current_values = match read_runtime_env_file() {
        Ok(values) => values,
        Err(error) => {
            return HttpResponse::Ok().json(json!({
                "ok": false,
                "error_code": error.code,
                "description": error.description,
            }));
        }
    };

    for (key, value) in &payload.values {
        let normalized_key = key.trim();
        if normalized_key.is_empty() {
            continue;
        }

        match value {
            Some(raw_value) => {
                current_values.insert(normalized_key.to_string(), raw_value.clone());
            }
            None => {
                current_values.remove(normalized_key);
            }
        }
    }

    ensure_runtime_env_invariants(&mut current_values);

    match write_runtime_env_file(&current_values) {
        Ok(()) => {
            apply_runtime_env_to_process(&current_values);
            HttpResponse::Ok().json(json!({
                "ok": true,
                "result": {
                    "file_path": runtime_env_file_path().to_string_lossy().to_string(),
                    "values": current_values,
                }
            }))
        }
        Err(error) => HttpResponse::Ok().json(json!({
            "ok": false,
            "error_code": error.code,
            "description": error.description,
        })),
    }
}

#[get("/bot/{method}")]
pub async fn bot_api_missing_token_get(_path: web::Path<String>) -> impl Responder {
    into_telegram_response(Err(telegram_not_found_error()))
}

#[post("/bot/{method}")]
pub async fn bot_api_missing_token_post(_path: web::Path<String>) -> impl Responder {
    into_telegram_response(Err(telegram_not_found_error()))
}

#[get("/")]
pub async fn api_root_not_found_get() -> impl Responder {
    into_telegram_response(Err(telegram_not_found_error()))
}

#[post("/")]
pub async fn api_root_not_found_post() -> impl Responder {
    into_telegram_response(Err(telegram_not_found_error()))
}

pub async fn api_not_found_fallback() -> impl Responder {
    into_telegram_response(Err(telegram_not_found_error()))
}

fn webhook_success_description(method: &str, result: &Value) -> Option<&'static str> {
    if !matches!(result, Value::Bool(true)) {
        return None;
    }

    match method.trim().to_ascii_lowercase().as_str() {
        "setwebhook" => Some("Webhook was set"),
        "deletewebhook" => Some("Webhook was deleted"),
        _ => None,
    }
}

fn into_telegram_response_for_method(method: &str, result: ApiResult) -> HttpResponse {
    match result {
        Ok(value) => {
            if let Some(description) = webhook_success_description(method, &value) {
                return HttpResponse::Ok().json(json!({
                    "ok": true,
                    "result": strip_nulls(value),
                    "description": description,
                }));
            }

            into_telegram_response(Ok(value))
        }
        Err(error) => into_telegram_response(Err(error)),
    }
}

#[get("/bot{token}/{method}")]
pub async fn bot_api_get(
    state: Data<AppState>,
    path: web::Path<(String, String)>,
    req: HttpRequest,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let (token, method) = path.into_inner();
    let params = query_to_json_map(&query.into_inner());
    let request_payload = Value::Object(params.clone().into_iter().collect());
    let started_at = Utc::now().timestamp_millis();
    let timer = Instant::now();
    if let Err(error) = validate_public_bot_token(&state, &token) {
        let error_result = Err(error);
        log_route_json_result(
            &state,
            &req,
            request_payload,
            &error_result,
            started_at,
            timer.elapsed().as_millis() as u64,
        );
        return into_telegram_response(error_result);
    }
    let actor_user_id = extract_request_actor_user_id(req.headers());
    let result = with_request_actor_user_id(actor_user_id, || {
        dispatch_method(&state, &token, &method, params)
    });
    log_route_json_result(
        &state,
        &req,
        request_payload,
        &result,
        started_at,
        timer.elapsed().as_millis() as u64,
    );
    into_telegram_response_for_method(&method, result)
}

#[post("/bot{token}/{method}")]
pub async fn bot_api_post(
    state: Data<AppState>,
    path: web::Path<(String, String)>,
    query: Query<HashMap<String, String>>,
    req: HttpRequest,
    mut payload: web::Payload,
) -> impl Responder {
    let (token, method) = path.into_inner();
    let mut params = query_to_json_map(&query.into_inner());
    let started_at = Utc::now().timestamp_millis();
    let timer = Instant::now();

    if let Err(error) = validate_public_bot_token(&state, &token) {
        let request_payload = Value::Object(params.clone().into_iter().collect());
        let error_result = Err(error);
        log_route_json_result(
            &state,
            &req,
            request_payload,
            &error_result,
            started_at,
            timer.elapsed().as_millis() as u64,
        );
        return into_telegram_response(error_result);
    }

    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.starts_with("multipart/form-data") {
        let multipart_params = match parse_multipart_payload(req.headers(), payload).await {
            Ok(v) => v,
            Err(err) => {
                let request_payload = Value::Object(params.clone().into_iter().collect());
                let error_result = Err(err);
                log_route_json_result(
                    &state,
                    &req,
                    request_payload,
                    &error_result,
                    started_at,
                    timer.elapsed().as_millis() as u64,
                );
                return into_telegram_response(error_result);
            }
        };

        for (k, v) in multipart_params {
            params.insert(k, v);
        }
    } else {
        let mut body = Bytes::new();
        while let Some(chunk) = payload.next().await {
            let chunk = match chunk {
                Ok(bytes) => bytes,
                Err(_) => {
                    let request_payload = Value::Object(params.clone().into_iter().collect());
                    let error_result = Err(ApiError::bad_request("can't parse request JSON object"));
                    log_route_json_result(
                        &state,
                        &req,
                        request_payload,
                        &error_result,
                        started_at,
                        timer.elapsed().as_millis() as u64,
                    );
                    return into_telegram_response(error_result);
                }
            };
            if body.is_empty() {
                body = chunk;
            } else {
                let mut merged = web::BytesMut::with_capacity(body.len() + chunk.len());
                merged.extend_from_slice(&body);
                merged.extend_from_slice(&chunk);
                body = merged.freeze();
            }
        }

        if !body.is_empty() {
            if let Ok(json_body) = serde_json::from_slice::<Value>(&body) {
                if let Some(obj) = json_body.as_object() {
                    for (k, v) in obj {
                        params.insert(k.clone(), v.clone());
                    }
                }
            } else if let Ok(form_body) = serde_urlencoded::from_bytes::<HashMap<String, String>>(&body) {
                for (k, v) in form_body {
                    params.insert(k, guess_json_value(&v));
                }
            }
        }
    }

    let request_payload = Value::Object(params.clone().into_iter().collect());
    let actor_user_id = extract_request_actor_user_id(req.headers());
    let result = with_request_actor_user_id(actor_user_id, || {
        dispatch_method(&state, &token, &method, params)
    });
    log_route_json_result(
        &state,
        &req,
        request_payload,
        &result,
        started_at,
        timer.elapsed().as_millis() as u64,
    );
    into_telegram_response_for_method(&method, result)
}

#[get("/client-api/bot{token}/bootstrap")]
pub async fn sim_bootstrap(state: Data<AppState>, path: web::Path<String>) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_bootstrap(&state, &token))
}

#[get("/client-api/bot{token}/privacy-mode")]
pub async fn sim_get_privacy_mode(state: Data<AppState>, path: web::Path<String>) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_get_privacy_mode(&state, &token))
}

#[post("/client-api/bot{token}/privacy-mode")]
pub async fn sim_set_privacy_mode(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSetPrivacyModeRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_set_privacy_mode(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/business/connection")]
pub async fn sim_set_business_connection(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSetBusinessConnectionRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_set_business_connection(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/business/connection/remove")]
pub async fn sim_remove_business_connection(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimRemoveBusinessConnectionRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_remove_business_connection(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/channels/direct-messages/open")]
pub async fn sim_open_channel_direct_messages(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimOpenChannelDirectMessagesRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_open_channel_direct_messages(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserMessage")]
pub async fn sim_send_user_message(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserMessageRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_message(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserMedia")]
pub async fn sim_send_user_media(
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<HashMap<String, String>>,
    req: HttpRequest,
    mut payload: web::Payload,
) -> impl Responder {
    let token = path.into_inner();
    let mut params = query_to_json_map(&query.into_inner());
    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.starts_with("multipart/form-data") {
        let multipart_params = match parse_multipart_payload(req.headers(), payload).await {
            Ok(value) => value,
            Err(err) => return into_telegram_response(Err(err)),
        };
        for (k, v) in multipart_params {
            params.insert(k, v);
        }
    } else {
        let mut body = Bytes::new();
        while let Some(chunk) = payload.next().await {
            let chunk = match chunk {
                Ok(bytes) => bytes,
                Err(_) => return into_telegram_response(Err(ApiError::bad_request("can't parse request JSON object"))),
            };
            if body.is_empty() {
                body = chunk;
            } else {
                let mut merged = web::BytesMut::with_capacity(body.len() + chunk.len());
                merged.extend_from_slice(&body);
                merged.extend_from_slice(&chunk);
                body = merged.freeze();
            }
        }

        if !body.is_empty() {
            if let Ok(json_body) = serde_json::from_slice::<Value>(&body) {
                if let Some(obj) = json_body.as_object() {
                    for (k, v) in obj {
                        params.insert(k.clone(), v.clone());
                    }
                }
            } else if let Ok(form_body) = serde_urlencoded::from_bytes::<HashMap<String, String>>(&body) {
                for (k, v) in form_body {
                    params.insert(k, guess_json_value(&v));
                }
            }
        }
    }

    into_telegram_response(handle_sim_send_user_media(&state, &token, params))
}

#[post("/client-api/bot{token}/sendUserDice")]
pub async fn sim_send_user_dice(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserDiceRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_dice(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserGame")]
pub async fn sim_send_user_game(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserGameRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_game(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserContact")]
pub async fn sim_send_user_contact(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserContactRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_contact(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserLocation")]
pub async fn sim_send_user_location(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserLocationRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_location(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendUserVenue")]
pub async fn sim_send_user_venue(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendUserVenueRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_user_venue(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/editUserMessageMedia")]
pub async fn sim_edit_user_message_media(
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<HashMap<String, String>>,
    req: HttpRequest,
    mut payload: web::Payload,
) -> impl Responder {
    let token = path.into_inner();
    let mut params = query_to_json_map(&query.into_inner());
    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.starts_with("multipart/form-data") {
        let multipart_params = match parse_multipart_payload(req.headers(), payload).await {
            Ok(value) => value,
            Err(err) => return into_telegram_response(Err(err)),
        };
        for (k, v) in multipart_params {
            params.insert(k, v);
        }
    } else {
        let mut body = Bytes::new();
        while let Some(chunk) = payload.next().await {
            let chunk = match chunk {
                Ok(bytes) => bytes,
                Err(_) => return into_telegram_response(Err(ApiError::bad_request("can't parse request JSON object"))),
            };
            if body.is_empty() {
                body = chunk;
            } else {
                let mut merged = web::BytesMut::with_capacity(body.len() + chunk.len());
                merged.extend_from_slice(&body);
                merged.extend_from_slice(&chunk);
                body = merged.freeze();
            }
        }

        if !body.is_empty() {
            if let Ok(json_body) = serde_json::from_slice::<Value>(&body) {
                if let Some(obj) = json_body.as_object() {
                    for (k, v) in obj {
                        params.insert(k.clone(), v.clone());
                    }
                }
            } else if let Ok(form_body) = serde_urlencoded::from_bytes::<HashMap<String, String>>(&body) {
                for (k, v) in form_body {
                    params.insert(k, guess_json_value(&v));
                }
            }
        }
    }

    let actor_user_id = extract_request_actor_user_id(req.headers());
    let result = with_request_actor_user_id(actor_user_id, || {
        handle_sim_edit_user_message_media(&state, &token, params)
    });
    into_telegram_response(result)
}

#[post("/client-api/bot{token}/setUserReaction")]
pub async fn sim_set_user_reaction(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSetUserReactionRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_set_user_reaction(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/votePoll")]
pub async fn sim_vote_poll(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimVotePollRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_vote_poll(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/payInvoice")]
pub async fn sim_pay_invoice(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimPayInvoiceRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_pay_invoice(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/purchasePaidMedia")]
pub async fn sim_purchase_paid_media(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimPurchasePaidMediaRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_purchase_paid_media(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/pressInlineButton")]
pub async fn sim_press_inline_button(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimPressInlineButtonRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_press_inline_button(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/sendInlineQuery")]
pub async fn sim_send_inline_query(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSendInlineQueryRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_send_inline_query(&state, &token, payload.into_inner()))
}

#[get("/client-api/bot{token}/inlineQueryAnswer")]
pub async fn sim_inline_query_answer(
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let token = path.into_inner();
    let inline_query_id = query
        .get("inline_query_id")
        .map(String::as_str)
        .unwrap_or_default();
    into_telegram_response(handle_sim_get_inline_query_answer(&state, &token, inline_query_id))
}

#[get("/client-api/bot{token}/callbackQueryAnswer")]
pub async fn sim_callback_query_answer(
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let token = path.into_inner();
    let callback_query_id = query
        .get("callback_query_id")
        .map(String::as_str)
        .unwrap_or_default();
    into_telegram_response(handle_sim_get_callback_query_answer(&state, &token, callback_query_id))
}

#[get("/client-api/bot{token}/pollVoters")]
pub async fn sim_poll_voters(
    state: Data<AppState>,
    path: web::Path<String>,
    query: Query<HashMap<String, String>>,
) -> impl Responder {
    let token = path.into_inner();
    let chat_id = query
        .get("chat_id")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or_default();
    let message_id = query
        .get("message_id")
        .and_then(|v| v.parse::<i64>().ok())
        .unwrap_or_default();

    into_telegram_response(handle_sim_get_poll_voters(&state, &token, chat_id, message_id))
}

#[post("/client-api/bot{token}/chooseInlineResult")]
pub async fn sim_choose_inline_result(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimChooseInlineResultRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_choose_inline_result(&state, &token, payload.into_inner()))
}

#[post("/client-api/bots/create")]
pub async fn sim_create_bot(
    state: Data<AppState>,
    payload: web::Json<SimCreateBotRequest>,
) -> impl Responder {
    into_telegram_response(handle_sim_create_bot(&state, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/create")]
pub async fn sim_create_group(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimCreateGroupRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_create_group(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/join")]
pub async fn sim_join_group(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimJoinGroupRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_join_group(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/leave")]
pub async fn sim_leave_group(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimLeaveGroupRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_leave_group(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/channels/views/mark")]
pub async fn sim_mark_channel_message_view(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimMarkChannelMessageViewRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_mark_channel_message_view(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/update")]
pub async fn sim_update_group(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimUpdateGroupRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_update_group(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/delete")]
pub async fn sim_delete_group(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimDeleteGroupRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_delete_group(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/bot-membership")]
pub async fn sim_set_bot_group_membership(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSetBotGroupMembershipRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_set_bot_group_membership(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/invite/create")]
pub async fn sim_create_group_invite_link(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimCreateGroupInviteLinkRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_create_group_invite_link(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/invite/join")]
pub async fn sim_join_group_by_invite_link(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimJoinGroupByInviteLinkRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_join_group_by_invite_link(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/join-requests/approve")]
pub async fn sim_approve_join_request(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimResolveJoinRequestRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_approve_join_request(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/groups/join-requests/decline")]
pub async fn sim_decline_join_request(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimResolveJoinRequestRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_decline_join_request(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/update")]
pub async fn sim_update_bot(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimUpdateBotRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_update_bot(&state, &token, payload.into_inner()))
}

#[post("/client-api/users/upsert")]
pub async fn sim_upsert_user(
    state: Data<AppState>,
    payload: web::Json<SimUpsertUserRequest>,
) -> impl Responder {
    into_telegram_response(handle_sim_upsert_user(&state, payload.into_inner()))
}

#[post("/client-api/users/delete")]
pub async fn sim_delete_user(
    state: Data<AppState>,
    payload: web::Json<SimDeleteUserRequest>,
) -> impl Responder {
    into_telegram_response(handle_sim_delete_user(&state, payload.into_inner()))
}

#[post("/client-api/bot{token}/users/profile-audio")]
pub async fn sim_set_user_profile_audio(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimSetUserProfileAudioRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_set_user_profile_audio(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/users/profile-audio/upload")]
pub async fn sim_upload_user_profile_audio(
    state: Data<AppState>,
    path: web::Path<String>,
    req: HttpRequest,
    payload: web::Payload,
) -> impl Responder {
    let token = path.into_inner();

    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if !content_type.starts_with("multipart/form-data") {
        return into_telegram_response(Err(ApiError::bad_request(
            "content-type must be multipart/form-data",
        )));
    }

    let params = match parse_multipart_payload(req.headers(), payload).await {
        Ok(value) => value,
        Err(error) => return into_telegram_response(Err(error)),
    };

    let actor_user_id = extract_request_actor_user_id(req.headers());
    let result = with_request_actor_user_id(actor_user_id, || {
        handle_sim_upload_user_profile_audio(&state, &token, &params)
    });
    into_telegram_response(result)
}

#[post("/client-api/bot{token}/users/profile-audio/delete")]
pub async fn sim_delete_user_profile_audio(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimDeleteUserProfileAudioRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_delete_user_profile_audio(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/users/chat-boosts/add")]
pub async fn sim_add_user_chat_boosts(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimAddUserChatBoostsRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_add_user_chat_boosts(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/users/chat-boosts/remove")]
pub async fn sim_remove_user_chat_boosts(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimRemoveUserChatBoostsRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_remove_user_chat_boosts(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/deleteOwnedGift")]
pub async fn sim_delete_owned_gift(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimDeleteOwnedGiftRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_delete_owned_gift(&state, &token, payload.into_inner()))
}

#[post("/client-api/bot{token}/clearHistory")]
pub async fn sim_clear_history(
    state: Data<AppState>,
    path: web::Path<String>,
    payload: web::Json<SimClearHistoryRequest>,
) -> impl Responder {
    let token = path.into_inner();
    into_telegram_response(handle_sim_clear_history(&state, &token, payload.into_inner()))
}

#[get("/file/bot{token}/{file_path:.*}")]
pub async fn file_download(
    state: Data<AppState>,
    path: web::Path<(String, String)>,
) -> impl Responder {
    let (token, file_path) = path.into_inner();
    match handle_download_file(&state, &token, &file_path) {
        Ok((bytes, mime_type)) => {
            let mut response = HttpResponse::Ok();
            if let Some(mt) = mime_type {
                response.content_type(mt);
            } else {
                response.content_type("application/octet-stream");
            }
            response.body(bytes)
        }
        Err(err) => into_telegram_response(Err(err)),
    }
}

fn log_route_json_result(
    state: &Data<AppState>,
    req: &HttpRequest,
    request_payload: Value,
    result: &Result<Value, ApiError>,
    started_at: i64,
    duration_ms: u64,
) {
    let response_payload = match result {
        Ok(value) => json!({
            "ok": true,
            "result": strip_nulls(value.clone()),
        }),
        Err(error) => json!({
            "ok": false,
            "error_code": error.code,
            "description": error.description,
        }),
    };

    push_runtime_request_log(
        state,
        RuntimeRequestLogEntry {
            id: Uuid::new_v4().to_string(),
            at: started_at,
            method: req.method().as_str().to_string(),
            path: req.path().to_string(),
            query: if req.query_string().trim().is_empty() {
                None
            } else {
                Some(req.query_string().to_string())
            },
            status: result.as_ref().map(|_| 200).unwrap_or_else(|error| error.code),
            duration_ms,
            remote_addr: req
                .connection_info()
                .realip_remote_addr()
                .map(|value| value.to_string()),
            request: Some(request_payload),
            response: Some(response_payload),
        },
    );
}

fn runtime_service_mode() -> String {
    std::env::var("RUNTIME_SERVICE_MODE")
    .unwrap_or_else(|_| "auto".to_string())
        .trim()
        .to_ascii_lowercase()
}

fn runtime_service_name() -> String {
    std::env::var("RUNTIME_SERVICE_NAME")
        .unwrap_or_else(|_| "simula-api-server".to_string())
        .trim()
        .to_string()
}

fn command_available(command: &str) -> bool {
    Command::new(command).output().is_ok()
}

fn systemctl_available() -> bool {
    command_available("systemctl")
}

fn launchctl_available() -> bool {
    command_available("launchctl")
}

fn windows_sc_available() -> bool {
    command_available("sc")
}

fn systemd_load_state(service_name: &str) -> Option<String> {
    let output = Command::new("systemctl")
        .args(["show", service_name, "--property", "LoadState", "--value"])
        .output()
        .ok()?;

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_ascii_lowercase();
    if stdout.is_empty() {
        None
    } else {
        Some(stdout)
    }
}

fn systemd_unit_loaded(service_name: &str) -> bool {
    matches!(systemd_load_state(service_name).as_deref(), Some("loaded"))
}

fn read_systemd_service_state(service_name: &str) -> (String, bool) {
    if !systemctl_available() {
        return ("systemctl-not-found".to_string(), false);
    }
    if !systemd_unit_loaded(service_name) {
        return ("not-loaded".to_string(), false);
    }

    let output = Command::new("systemctl")
        .arg("is-active")
        .arg(service_name)
        .output();

    match output {
        Ok(value) => {
            let stdout = String::from_utf8_lossy(&value.stdout).trim().to_ascii_lowercase();
            let stderr = String::from_utf8_lossy(&value.stderr).trim().to_ascii_lowercase();
            let status = if !stdout.is_empty() {
                stdout
            } else if !stderr.is_empty() {
                stderr
            } else {
                "unknown".to_string()
            };
            let active = status == "active";
            (status, active)
        }
        Err(_) => ("unknown".to_string(), false),
    }
}

fn read_launchctl_service_state(service_name: &str) -> (String, bool, bool) {
    if !launchctl_available() {
        return ("launchctl-not-found".to_string(), false, false);
    }

    let output = Command::new("launchctl").arg("list").output();
    match output {
        Ok(value) => {
            let stdout = String::from_utf8_lossy(&value.stdout);
            let mut loaded = false;
            let mut active = false;

            for line in stdout.lines() {
                if !line.contains(service_name) {
                    continue;
                }
                loaded = true;
                let cols: Vec<&str> = line.split_whitespace().collect();
                if cols.len() >= 3 {
                    let pid = cols[0].trim();
                    if pid != "-" && pid != "0" {
                        active = true;
                    }
                }
                break;
            }

            if !loaded {
                ("not-loaded".to_string(), false, false)
            } else if active {
                ("running".to_string(), true, true)
            } else {
                ("loaded".to_string(), false, true)
            }
        }
        Err(_) => ("unknown".to_string(), false, false),
    }
}

fn read_windows_service_state(service_name: &str) -> (String, bool, bool) {
    if !windows_sc_available() {
        return ("sc-not-found".to_string(), false, false);
    }

    let output = Command::new("sc").args(["query", service_name]).output();
    match output {
        Ok(value) => {
            let text = format!(
                "{}\n{}",
                String::from_utf8_lossy(&value.stdout),
                String::from_utf8_lossy(&value.stderr)
            )
            .to_ascii_lowercase();

            if text.contains("1060") || text.contains("does not exist") {
                return ("not-loaded".to_string(), false, false);
            }

            let active = text.contains("running");
            if active {
                ("running".to_string(), true, true)
            } else if text.contains("stopped") {
                ("stopped".to_string(), false, true)
            } else {
                ("installed".to_string(), false, true)
            }
        }
        Err(_) => ("unknown".to_string(), false, false),
    }
}

fn resolve_runtime_service_mode(service_name: &str) -> (String, Option<String>) {
    let requested_mode = runtime_service_mode();
    if requested_mode == "runtime-gate" || requested_mode == "self-process" {
        return (requested_mode, None);
    }

    if requested_mode == "systemd" {
        if systemctl_available() && systemd_unit_loaded(service_name) {
            return ("systemd".to_string(), None);
        }
        return ("self-process".to_string(), None);
    }

    if requested_mode == "launchctl" {
        let (_, _, loaded) = read_launchctl_service_state(service_name);
        if loaded {
            return ("launchctl".to_string(), None);
        }
        return ("self-process".to_string(), None);
    }

    if requested_mode == "windows-service" {
        let (_, _, loaded) = read_windows_service_state(service_name);
        if loaded {
            return ("windows-service".to_string(), None);
        }
        return ("self-process".to_string(), None);
    }

    if requested_mode != "auto" {
        return ("self-process".to_string(), None);
    }

    match std::env::consts::OS {
        "linux" => {
            if systemctl_available() && systemd_unit_loaded(service_name) {
                ("systemd".to_string(), None)
            } else {
                ("self-process".to_string(), None)
            }
        }
        "macos" => {
            let (_, _, loaded) = read_launchctl_service_state(service_name);
            if loaded {
                ("launchctl".to_string(), None)
            } else {
                ("self-process".to_string(), None)
            }
        }
        "windows" => {
            let (_, _, loaded) = read_windows_service_state(service_name);
            if loaded {
                ("windows-service".to_string(), None)
            } else {
                ("self-process".to_string(), None)
            }
        }
        _ => ("self-process".to_string(), None),
    }
}

fn current_process_binary_path() -> Option<String> {
    std::env::current_exe()
        .ok()
        .map(|value| value.to_string_lossy().to_string())
}

fn runtime_service_snapshot(state: &Data<AppState>) -> Value {
    let service_name = runtime_service_name();
    let requested_mode = runtime_service_mode();
    let (mode, note) = resolve_runtime_service_mode(&service_name);

    match mode.as_str() {
        "systemd" => {
            let (status, active) = read_systemd_service_state(&service_name);
            json!({
                "mode": mode,
                "requested_mode": requested_mode,
                "name": service_name,
                "available": status != "systemctl-not-found" && status != "not-loaded",
                "active": active,
                "status": status,
                "note": note,
            })
        }
        "launchctl" => {
            let (status, active, loaded) = read_launchctl_service_state(&service_name);
            json!({
                "mode": mode,
                "requested_mode": requested_mode,
                "name": service_name,
                "available": loaded,
                "active": active,
                "status": status,
                "note": note,
            })
        }
        "windows-service" => {
            let (status, active, loaded) = read_windows_service_state(&service_name);
            json!({
                "mode": mode,
                "requested_mode": requested_mode,
                "name": service_name,
                "available": loaded,
                "active": active,
                "status": status,
                "note": note,
            })
        }
        "self-process" => {
            let active = is_api_enabled(state);
            let status = if active { "running" } else { "stopped" };

            json!({
                "mode": mode,
                "requested_mode": requested_mode,
                "name": service_name,
                "available": true,
                "active": active,
                "status": status,
                "note": note,
                "pid": std::process::id(),
                "binary_path": current_process_binary_path(),
            })
        }
        "runtime-gate" => {
            let active = is_api_enabled(state);
            json!({
                "mode": mode,
                "requested_mode": requested_mode,
                "name": service_name,
                "available": true,
                "active": active,
                "status": if active { "enabled" } else { "disabled" },
                "note": note,
            })
        }
        _ => json!({
            "mode": mode,
            "requested_mode": requested_mode,
            "name": service_name,
            "available": false,
            "active": false,
            "status": "unsupported-manager",
            "note": Some("Unsupported runtime service mode."),
        }),
    }
}

fn run_systemd_action(action: &str, service_name: &str) -> Result<(), ApiError> {
    if !systemctl_available() {
        return Err(ApiError::bad_request("systemctl command was not found"));
    }

    let output = Command::new("systemctl")
        .arg(action)
        .arg(service_name)
        .output()
        .map_err(ApiError::internal)?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let reason = if !stderr.is_empty() { stderr } else { stdout };

    Err(ApiError::bad_request(format!(
        "systemctl {} {} failed: {}",
        action,
        service_name,
        if reason.is_empty() { "unknown error" } else { &reason }
    )))
}

fn run_launchctl_action(action: &str, service_name: &str) -> Result<(), ApiError> {
    if !launchctl_available() {
        return Err(ApiError::bad_request("launchctl command was not found"));
    }

    let uid = std::env::var("UID").unwrap_or_else(|_| "0".to_string());
    let targets = vec![
        format!("system/{}", service_name),
        format!("gui/{}/{}", uid, service_name),
    ];

    let mut failures = Vec::<String>::new();
    for target in targets {
        let output = match action {
            "start" | "restart" => Command::new("launchctl")
                .args(["kickstart", "-k", &target])
                .output(),
            "stop" => Command::new("launchctl").args(["bootout", &target]).output(),
            _ => {
                return Err(ApiError::bad_request("unsupported launchctl action"));
            }
        }
        .map_err(ApiError::internal)?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let reason = if !stderr.is_empty() { stderr } else { stdout };
        failures.push(format!("{} => {}", target, if reason.is_empty() { "unknown error" } else { &reason }));
    }

    Err(ApiError::bad_request(format!(
        "launchctl {} {} failed: {}",
        action,
        service_name,
        failures.join(" | ")
    )))
}

fn run_windows_sc_action(action: &str, service_name: &str) -> Result<(), ApiError> {
    if !windows_sc_available() {
        return Err(ApiError::bad_request("sc command was not found"));
    }

    let output = Command::new("sc")
        .arg(action)
        .arg(service_name)
        .output()
        .map_err(ApiError::internal)?;

    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let reason = if !stderr.is_empty() { stderr } else { stdout };
    Err(ApiError::bad_request(format!(
        "sc {} {} failed: {}",
        action,
        service_name,
        if reason.is_empty() { "unknown error" } else { &reason }
    )))
}

fn run_windows_service_action(action: &str, service_name: &str) -> Result<(), ApiError> {
    if action == "restart" {
        let _ = run_windows_sc_action("stop", service_name);
        return run_windows_sc_action("start", service_name);
    }

    match action {
        "start" | "stop" => run_windows_sc_action(action, service_name),
        _ => Err(ApiError::bad_request("unsupported windows-service action")),
    }
}

fn run_runtime_gate_action(state: &Data<AppState>, action: &str) -> Result<(), ApiError> {
    if action != "restart" {
        return Err(ApiError::bad_request("unsupported runtime-gate action: only restart is allowed"));
    }

    let env_values = read_runtime_env_file().unwrap_or_else(|_| runtime_env_defaults());
    set_api_enabled(state, false);
    apply_runtime_env_to_process(&env_values);
    set_api_enabled(state, true);
    clear_runtime_transition(state);
    Ok(())
}

fn restart_self_process(
    binary_path: &PathBuf,
    runtime_root: &PathBuf,
    env_values: &BTreeMap<String, String>,
) -> Result<(), String> {
    let mut command = Command::new(binary_path);
    command.current_dir(runtime_root);
    command.envs(env_values.iter().map(|(key, value)| (key.as_str(), value.as_str())));

    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        let error = command.exec();
        Err(format!("failed to exec restarted api-server process: {}", error))
    }

    #[cfg(not(unix))]
    {
        command
            .spawn()
            .map_err(|error| format!("failed to spawn restarted api-server process: {}", error))?;
        Ok(())
    }
}

fn schedule_self_process_restart(env_values: BTreeMap<String, String>) -> Result<(), ApiError> {
    let runtime_root = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    let binary_path = std::env::current_exe().map_err(ApiError::internal)?;

    thread::spawn(move || {
        thread::sleep(Duration::from_millis(450));
        if let Err(error) = restart_self_process(&binary_path, &runtime_root, &env_values) {
            eprintln!("{}", error);
        }
        std::process::exit(0);
    });

    Ok(())
}

fn run_self_process_action(state: &Data<AppState>, action: &str) -> Result<(), ApiError> {
    if action != "restart" {
        return Err(ApiError::bad_request("unsupported self-process action: only restart is allowed"));
    }

    let env_values = read_runtime_env_file().unwrap_or_else(|_| runtime_env_defaults());
    apply_runtime_env_to_process(&env_values);
    clear_runtime_transition(state);
    schedule_self_process_restart(env_values)
}

fn perform_runtime_service_action(
    state: &Data<AppState>,
    action: &str,
) -> Result<Value, ApiError> {
    if action != "restart" {
        return Err(ApiError::bad_request("Unsupported runtime action. Only restart is allowed."));
    }

    let service_name = runtime_service_name();
    let (mode, _) = resolve_runtime_service_mode(&service_name);

    match mode.as_str() {
        "systemd" => run_systemd_action(action, &service_name)?,
        "launchctl" => run_launchctl_action(action, &service_name)?,
        "windows-service" => run_windows_service_action(action, &service_name)?,
        "self-process" => run_self_process_action(state, action)?,
        "runtime-gate" => run_runtime_gate_action(state, action)?,
        _ => {
            return Err(ApiError::bad_request(format!(
                "Unsupported runtime service mode: {}",
                mode
            )));
        }
    }

    Ok(runtime_service_snapshot(state))
}

fn runtime_env_defaults() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("API_HOST".to_string(), "127.0.0.1".to_string()),
        ("API_PORT".to_string(), "8081".to_string()),
        ("DATABASE_URL".to_string(), "simula.db".to_string()),
        ("FILE_STORAGE_DIR".to_string(), "files".to_string()),
        ("LOG_DIR".to_string(), "stdout".to_string()),
    ])
}

fn runtime_env_allowed_keys() -> [&'static str; 5] {
    [
        "API_HOST",
        "API_PORT",
        "DATABASE_URL",
        "FILE_STORAGE_DIR",
        "LOG_DIR",
    ]
}

fn resolve_runtime_path(workspace_root: &Path, raw: &str) -> String {
    let normalized = raw.trim();
    if normalized.is_empty() {
        return workspace_root.to_string_lossy().to_string();
    }
    if normalized.eq_ignore_ascii_case("stdout") || normalized.contains("://") {
        return normalized.to_string();
    }

    let candidate = PathBuf::from(normalized);
    let resolved = if candidate.is_absolute() {
        candidate
    } else {
        workspace_root.join(candidate)
    };
    resolved.to_string_lossy().to_string()
}

fn ensure_runtime_env_invariants(values: &mut BTreeMap<String, String>) {
    let defaults = runtime_env_defaults();
    let allowed_keys = runtime_env_allowed_keys();

    values.retain(|key, _| allowed_keys.contains(&key.as_str()));

    for key in allowed_keys {
        let missing_or_empty = values
            .get(key)
            .map(|value| value.trim().is_empty())
            .unwrap_or(true);
        if missing_or_empty {
            if let Some(default_value) = defaults.get(key) {
                values.insert(key.to_string(), default_value.clone());
            }
        }
    }

    let api_host_missing = values
        .get("API_HOST")
        .map(|value| value.trim().is_empty())
        .unwrap_or(true);
    if api_host_missing {
        values.insert("API_HOST".to_string(), "127.0.0.1".to_string());
    }
}

fn apply_runtime_env_to_process(values: &BTreeMap<String, String>) {
    for (key, value) in values {
        std::env::set_var(key, value);
    }
}

fn runtime_env_file_path() -> PathBuf {
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".env")
}

fn read_runtime_env_file() -> Result<BTreeMap<String, String>, ApiError> {
    let path = runtime_env_file_path();
    let mut values = runtime_env_defaults();

    if !path.exists() {
        return Ok(values);
    }

    let content = fs::read_to_string(path).map_err(ApiError::internal)?;
    for raw_line in content.lines() {
        let line = raw_line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let (raw_key, raw_value) = match line.split_once('=') {
            Some(parts) => parts,
            None => continue,
        };

        let key = raw_key.trim().trim_start_matches("export ").trim().to_string();
        if key.is_empty() {
            continue;
        }

        let value = raw_value.trim();
        let normalized_value = if value.len() >= 2
            && ((value.starts_with('"') && value.ends_with('"'))
                || (value.starts_with('\'') && value.ends_with('\'')))
        {
            value[1..value.len() - 1].to_string()
        } else {
            value.to_string()
        };
        values.insert(key, normalized_value);
    }

    ensure_runtime_env_invariants(&mut values);

    Ok(values)
}

fn encode_env_value(raw: &str) -> String {
    let is_simple = !raw.is_empty()
        && raw
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | '/' | ':' | ','));
    if is_simple {
        return raw.to_string();
    }

    let escaped = raw
        .replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r");
    format!("\"{}\"", escaped)
}

fn write_runtime_env_file(values: &BTreeMap<String, String>) -> Result<(), ApiError> {
    let path = runtime_env_file_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(ApiError::internal)?;
    }

    let mut normalized_values = values.clone();
    ensure_runtime_env_invariants(&mut normalized_values);

    let mut lines = String::new();
    for (key, value) in &normalized_values {
        let normalized_key = key.trim();
        if normalized_key.is_empty() {
            continue;
        }
        lines.push_str(normalized_key);
        lines.push('=');
        lines.push_str(&encode_env_value(value));
        lines.push('\n');
    }

    fs::write(path, lines).map_err(ApiError::internal)
}

fn query_to_json_map(query: &HashMap<String, String>) -> HashMap<String, Value> {
    query
        .iter()
        .map(|(k, v)| (k.clone(), guess_json_value(v)))
        .collect()
}

fn telegram_not_found_error() -> ApiError {
    ApiError {
        code: 404,
        description: "Not Found".to_string(),
    }
}

fn telegram_forbidden_error() -> ApiError {
    ApiError {
        code: 403,
        description: "Forbidden".to_string(),
    }
}

fn validate_public_bot_token(state: &Data<AppState>, token: &str) -> Result<(), ApiError> {
    let normalized = token.trim();
    if normalized.is_empty() {
        return Err(telegram_not_found_error());
    }

    let conn = lock_db(state)?;
    let existing_bot_id: Option<i64> = conn
        .query_row(
            "SELECT id FROM bots WHERE token = ?1 LIMIT 1",
            params![normalized],
            |row| row.get(0),
        )
        .optional()
        .map_err(ApiError::internal)?;

    if existing_bot_id.is_none() {
        return Err(telegram_forbidden_error());
    }

    Ok(())
}

fn extract_request_actor_user_id(headers: &actix_web::http::header::HeaderMap) -> Option<i64> {
    headers
        .get("x-simula-actor-user-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|value| *value > 0)
}

fn guess_json_value(raw: &str) -> Value {
    // Keep oversized integer-like tokens (e.g., callback_query_id/inline_query_id)
    // as strings; coercing them to JSON numbers can lose precision.
    let looks_integer = raw
        .chars()
        .enumerate()
        .all(|(idx, ch)| ch.is_ascii_digit() || (idx == 0 && ch == '-'));
    let digit_count = raw.chars().filter(|ch| ch.is_ascii_digit()).count();
    if looks_integer && digit_count > 15 {
        return Value::String(raw.to_string());
    }

    if let Ok(v) = serde_json::from_str::<Value>(raw) {
        return v;
    }
    Value::String(raw.to_string())
}

async fn parse_multipart_payload(
    headers: &actix_web::http::header::HeaderMap,
    payload: web::Payload,
) -> Result<HashMap<String, Value>, ApiError> {
    let mut multipart = Multipart::new(headers, payload.into_inner());
    let mut params: HashMap<String, Value> = HashMap::new();
    let mut file_field_map: HashMap<String, String> = HashMap::new();

    while let Some(item) = multipart.next().await {
        let mut field = item.map_err(|_| ApiError::bad_request("invalid multipart field"))?;
        let field_name = field.name().to_string();
        if field_name.is_empty() {
            continue;
        }

        let filename = field
            .content_disposition()
            .get_filename()
            .map(|v| v.to_string());

        let mut bytes = Vec::<u8>::new();
        while let Some(chunk) = field.next().await {
            let chunk = chunk.map_err(|_| ApiError::bad_request("failed to read multipart chunk"))?;
            bytes.extend_from_slice(&chunk);
            if bytes.len() > 52_428_800 {
                return Err(ApiError::bad_request("uploaded file is too large"));
            }
        }

        if filename.is_some() {
            let local_path = save_uploaded_temp_file(&field_name, &bytes)?;
            params.insert(field_name.clone(), Value::String(local_path.clone()));
            file_field_map.insert(field_name, local_path);
            continue;
        }

        let text = String::from_utf8_lossy(&bytes).trim().to_string();
        params.insert(field_name, guess_json_value(&text));
    }

    for value in params.values_mut() {
        resolve_attach_in_value(value, &file_field_map);
    }

    Ok(params)
}

fn resolve_attach_in_value(value: &mut Value, file_field_map: &HashMap<String, String>) {
    match value {
        Value::String(raw) => {
            if let Some(attach_name) = raw.strip_prefix("attach://") {
                if let Some(path) = file_field_map.get(attach_name) {
                    *value = Value::String(path.clone());
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                resolve_attach_in_value(item, file_field_map);
            }
        }
        Value::Object(map) => {
            for item in map.values_mut() {
                resolve_attach_in_value(item, file_field_map);
            }
        }
        _ => {}
    }
}

fn save_uploaded_temp_file(field_name: &str, bytes: &[u8]) -> Result<String, ApiError> {
    let root = std::env::var("FILE_STORAGE_DIR").unwrap_or_else(|_| "files".to_string());
    let upload_dir = Path::new(&root).join("uploads");
    fs::create_dir_all(&upload_dir).map_err(ApiError::internal)?;

    let safe_field = field_name
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect::<String>();
    let file_name = format!("{}_{}", safe_field, uuid::Uuid::new_v4().simple());
    let full_path = upload_dir.join(file_name);

    fs::write(&full_path, bytes).map_err(ApiError::internal)?;
    Ok(full_path.to_string_lossy().to_string())
}
