//! Acting as an account: any TL function (`call`), or common actions made simple (`send`).

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
    Caller { account: UserId(id), session: None }
}

/// `POST /accounts/{id}/call` with a JSON-TL function, e.g.
/// `{"_": "messages.sendMessage", "peer": {...}, "message": "hi", "random_id": "1"}`.
pub async fn call(
    State(sandbox): State<Arc<Sandbox>>,
    Path(id): Path<i64>,
    Json(body): Json<Value>,
) -> ControlResult<Json<Value>> {
    let function: Function =
        serde_json::from_value(body).map_err(|e| RpcError::bad_request(format!("INPUT_INVALID: {e}")))?;
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
        Value::Number(n) => n.as_i64().and_then(Peer::from_bot_api_id).ok_or_else(RpcError::peer_id_invalid),
        Value::String(s) if s.starts_with('@') => {
            let name = s.clone();
            let account = sandbox.run(move |w| store::accounts::by_username(w.connection(), &name)).await?;
            account.map(|a| Peer::User(a.id)).ok_or_else(|| RpcError::bad_request("USERNAME_NOT_OCCUPIED"))
        }
        Value::String(s) => s.parse().ok().and_then(Peer::from_bot_api_id).ok_or_else(RpcError::peer_id_invalid),
        _ => Err(RpcError::peer_id_invalid()),
    }
}
