//! Acting as an account: any TL function (`call`), or common actions made simple
//! (`send`, `press`).

use std::sync::Arc;

use axum::Json;
use axum::extract::{Path, State};
use serde_json::Value;
use simula_core::{Caller, Peer, RpcError, Sandbox, UserId, store};
use simula_tl::{Function, functions};

use super::ok;
use crate::error::ControlResult;

fn as_account(id: i64) -> Caller {
    // No session: every session of the account (e.g. an open client) sees the result.
    Caller {
        account: UserId(id),
        session: None,
    }
}

/// `POST /accounts/{id}/call` with a JSON-TL function, e.g.
/// `{"_": "messages.sendMessage", "peer": {...}, "message": "hi", "random_id": "1"}`.
pub async fn call(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
    Json(body): Json<Value>,
) -> ControlResult<Json<Value>> {
    let function: Function = serde_json::from_value(body)
        .map_err(|e| RpcError::bad_request(format!("INPUT_INVALID: {e}")))?;
    let reply = sandbox.invoke(as_account(id), function).await?;
    Ok(ok(reply.to_json()))
}

#[derive(serde::Deserialize)]
pub struct Send {
    /// Bot API style: a user id, or `@username`.
    pub chat_id: Value,
    pub text: String,
}

/// `POST /accounts/{id}/send` — send a text message as this account.
pub async fn send(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
    Json(body): Json<Send>,
) -> ControlResult<Json<Value>> {
    let peer = resolve(&sandbox, &body.chat_id).await?;
    let call = functions::messages::SendMessage {
        peer: sandbox.input_peer(UserId(id), peer),
        message: body.text,
        random_id: rand::random(),
        ..Default::default()
    };
    let updates = sandbox.call(as_account(id), call).await?;
    Ok(ok(serde_json::to_value(updates).unwrap_or_default()))
}

async fn resolve(sandbox: &Sandbox, chat_id: &Value) -> Result<Peer, RpcError> {
    match chat_id {
        Value::Number(n) => n
            .as_i64()
            .and_then(Peer::from_bot_api_id)
            .ok_or_else(RpcError::peer_id_invalid),
        Value::String(s) if s.starts_with('@') => {
            let name = s.clone();
            let account = sandbox
                .run(move |w| store::accounts::by_username(w.connection(), &name))
                .await?;
            account
                .map(|a| Peer::User(a.id))
                .ok_or_else(|| RpcError::bad_request("USERNAME_NOT_OCCUPIED"))
        }
        Value::String(s) => s
            .parse()
            .ok()
            .and_then(Peer::from_bot_api_id)
            .ok_or_else(RpcError::peer_id_invalid),
        _ => Err(RpcError::peer_id_invalid()),
    }
}

#[derive(serde::Deserialize)]
pub struct Press {
    /// The chat, Bot API style (a user id or `@username`).
    pub chat_id: Value,
    /// The message, by id in this account's own box.
    pub message_id: i32,
    /// The button's callback data…
    #[serde(default)]
    pub data: Option<String>,
    /// …or the button's text.
    #[serde(default)]
    pub text: Option<String>,
}

/// `POST /accounts/{id}/press` — press a callback button and wait for the bot's answer
/// (`messages.getBotCallbackAnswer`). Answers `BOT_RESPONSE_TIMEOUT` if the bot is silent.
pub async fn press(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
    Json(body): Json<Press>,
) -> ControlResult<Json<Value>> {
    let peer = resolve(&sandbox, &body.chat_id).await?;
    let data = match (body.data, body.text) {
        (Some(data), _) => data.into_bytes(),
        (None, Some(text)) => button_data(&sandbox, id, body.message_id, &text).await?,
        (None, None) => return Err(RpcError::bad_request("DATA_INVALID: give data or text").into()),
    };
    let call = functions::messages::GetBotCallbackAnswer {
        peer: sandbox.input_peer(UserId(id), peer),
        msg_id: body.message_id,
        data: Some(data),
        ..Default::default()
    };
    let answer = sandbox.call(as_account(id), call).await?;
    Ok(ok(serde_json::to_value(answer).unwrap_or_default()))
}

/// The callback data of the button labelled `text` on the account's message.
async fn button_data(
    sandbox: &Sandbox,
    id: i64,
    message_id: i32,
    text: &str,
) -> Result<Vec<u8>, RpcError> {
    let found = sandbox
        .call(
            as_account(id),
            functions::messages::GetMessages {
                id: vec![simula_tl::types::InputMessageID { id: message_id }.into()],
            },
        )
        .await?;
    let simula_tl::enums::messages::Messages::Messages(found) = found else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    let Some(simula_tl::enums::Message::Message(message)) = found.messages.first() else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    use simula_tl::enums::{
        InlineButtonType, KeyboardInlineButton, KeyboardInlineButtonRow, ReplyMarkup,
    };
    let Some(ReplyMarkup::ReplyInlineMarkup(inline)) = &message.reply_markup else {
        return Err(RpcError::bad_request("DATA_INVALID: no inline keyboard"));
    };
    inline
        .rows
        .iter()
        .flat_map(|KeyboardInlineButtonRow::KeyboardInlineButtonRow(row)| &row.buttons)
        .find_map(
            |KeyboardInlineButton::KeyboardInlineButton(button)| match &button.r#type {
                InlineButtonType::Callback(callback) if button.text == text => {
                    Some(callback.data.clone())
                }
                _ => None,
            },
        )
        .ok_or_else(|| RpcError::bad_request("DATA_INVALID: no callback button with that text"))
}
