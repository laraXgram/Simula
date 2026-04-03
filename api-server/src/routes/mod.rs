use actix_multipart::Multipart;
use actix_web::{
    get, post,
    web::{self, Bytes, Data, Query},
    HttpRequest, HttpResponse, Responder,
};
use futures_util::StreamExt;
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::database::AppState;
use crate::handlers::{
    dispatch_method, handle_sim_bootstrap, handle_sim_clear_history, handle_sim_create_bot,
    handle_sim_create_group,
    handle_sim_create_group_invite_link,
    handle_sim_decline_join_request,
    handle_sim_delete_group,
    handle_sim_set_bot_group_membership,
    handle_sim_join_group,
    handle_sim_join_group_by_invite_link,
    handle_sim_leave_group,
    handle_sim_approve_join_request,
    handle_sim_update_group,
    handle_sim_choose_inline_result,
    handle_sim_get_callback_query_answer,
    handle_sim_get_inline_query_answer,
    handle_sim_get_poll_voters,
    handle_sim_pay_invoice,
    handle_sim_edit_user_message_media, handle_sim_send_user_media, handle_sim_send_user_message,
    handle_sim_send_user_contact, handle_sim_send_user_dice, handle_sim_send_user_game, handle_sim_send_user_location, handle_sim_send_user_venue,
    handle_sim_send_inline_query,
    handle_sim_press_inline_button,
    handle_sim_get_privacy_mode,
    handle_sim_set_user_reaction,
    handle_sim_set_privacy_mode,
    handle_sim_vote_poll,
    handle_sim_update_bot,
    handle_sim_upsert_user,
    handle_download_file,
    with_request_actor_user_id,
    SimChooseInlineResultRequest, SimClearHistoryRequest, SimCreateBotRequest, SimCreateGroupInviteLinkRequest, SimCreateGroupRequest, SimDeleteGroupRequest, SimJoinGroupByInviteLinkRequest, SimJoinGroupRequest, SimLeaveGroupRequest, SimPayInvoiceRequest, SimPressInlineButtonRequest, SimResolveJoinRequestRequest, SimSendInlineQueryRequest, SimSendUserMessageRequest, SimSetBotGroupMembershipRequest, SimSetUserReactionRequest, SimUpdateBotRequest, SimUpdateGroupRequest,
    SimSendUserContactRequest, SimSendUserDiceRequest, SimSendUserGameRequest, SimSendUserLocationRequest, SimSendUserVenueRequest,
    SimSetPrivacyModeRequest,
    SimVotePollRequest,
    SimUpsertUserRequest,
};
use crate::types::{into_telegram_response, ApiError};

#[get("/health")]
pub async fn health() -> impl Responder {
    HttpResponse::Ok().json(serde_json::json!({"status": "ok"}))
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
    let actor_user_id = extract_request_actor_user_id(req.headers());
    let result = with_request_actor_user_id(actor_user_id, || {
        dispatch_method(&state, &token, &method, params)
    });
    into_telegram_response(result)
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

    let content_type = req
        .headers()
        .get(actix_web::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();

    if content_type.starts_with("multipart/form-data") {
        let multipart_params = match parse_multipart_payload(req.headers(), payload).await {
            Ok(v) => v,
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
                Err(_) => return into_telegram_response(Err(ApiError::bad_request("invalid request body"))),
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
        dispatch_method(&state, &token, &method, params)
    });
    into_telegram_response(result)
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
                Err(_) => return into_telegram_response(Err(ApiError::bad_request("invalid request body"))),
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
                Err(_) => return into_telegram_response(Err(ApiError::bad_request("invalid request body"))),
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

fn query_to_json_map(query: &HashMap<String, String>) -> HashMap<String, Value> {
    query
        .iter()
        .map(|(k, v)| (k.clone(), guess_json_value(v)))
        .collect()
}

fn extract_request_actor_user_id(headers: &actix_web::http::header::HeaderMap) -> Option<i64> {
    headers
        .get("x-laragram-actor-user-id")
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
