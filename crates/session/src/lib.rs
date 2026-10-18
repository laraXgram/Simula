//! Client sessions: a simulated user's connection to the core, speaking JSON-TL over a
//! WebSocket — the same functions and updates as MTProto, without its wire format
//! (that transport comes with P8).
//!
//! `GET /session?account=<id>` upgrades to a WebSocket for that account. Frames are
//! JSON text:
//!
//! - first, the server says who the session is:
//!   `{"session": {"id": "…", "account": "…"}}`
//! - the client calls TL functions: `{"id": 1, "call": {"_": "messages.getDialogs", …}}`
//! - each call is answered: `{"id": 1, "result": {…}}` or
//!   `{"id": 1, "error": {"code": 400, "message": "PEER_ID_INVALID"}}`
//! - updates for the account are pushed: `{"updates": {"_": "updates", …}}`. Updates
//!   caused by this session's own calls are not pushed (they are the call's result),
//!   exactly as Telegram does per MTProto session.
//!
//! Calls run concurrently: pressing a button waits for the bot's answer without holding
//! up other calls. `long` values are strings, as everywhere in JSON-TL.

use std::sync::Arc;

use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde_json::{Value, json};
use simula_core::{Caller, RpcError, Sandbox, SessionId, UserId};
use simula_tl::Function;
use tokio::sync::{broadcast, mpsc};

pub fn router(sandbox: Arc<Sandbox>) -> Router {
    Router::new()
        .route("/session", get(upgrade))
        .with_state(sandbox)
}

#[derive(serde::Deserialize)]
struct Params {
    account: i64,
}

async fn upgrade(
    State(sandbox): State<Arc<Sandbox>>,
    Query(params): Query<Params>,
    ws: WebSocketUpgrade,
) -> Response {
    let account = UserId(params.account);
    match sandbox.account(account).await {
        Ok(Some(_)) => ws.on_upgrade(move |socket| run(sandbox, account, socket)),
        Ok(None) => (StatusCode::NOT_FOUND, "no such account").into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()).into_response(),
    }
}

async fn run(sandbox: Arc<Sandbox>, account: UserId, mut socket: WebSocket) {
    let session = SessionId(rand::random());
    let caller = Caller {
        account,
        session: Some(session),
    };
    let mut updates = sandbox.subscribe_updates();
    let (out, mut outgoing) = mpsc::unbounded_channel::<Value>();
    let hello =
        json!({ "session": { "id": session.0.to_string(), "account": account.0.to_string() } });
    if socket
        .send(Message::Text(hello.to_string().into()))
        .await
        .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            frame = socket.recv() => {
                let Some(Ok(frame)) = frame else { break };
                let text = match frame {
                    Message::Text(text) => text,
                    Message::Close(_) => break,
                    _ => continue,
                };
                handle_frame(&sandbox, caller, text.as_str(), &out);
            }
            Some(value) = outgoing.recv() => {
                if socket.send(Message::Text(value.to_string().into())).await.is_err() {
                    break;
                }
            }
            batch = updates.recv() => match batch {
                Ok(batch) if batch.account == account && batch.origin != Some(session) => {
                    let frame = json!({ "updates": batch.updates });
                    if socket.send(Message::Text(frame.to_string().into())).await.is_err() {
                        break;
                    }
                }
                Ok(_) => {}
                // The client fell behind; it should refetch its state (getDifference, P8.4).
                Err(broadcast::error::RecvError::Lagged(missed)) => {
                    tracing::warn!(account = account.0, missed, "session lagged behind updates");
                    let _ = out.send(json!({ "lagged": missed }));
                }
                Err(broadcast::error::RecvError::Closed) => break,
            },
        }
    }
}

/// Start one call; its answer goes out through `out` when it completes.
fn handle_frame(
    sandbox: &Arc<Sandbox>,
    caller: Caller,
    text: &str,
    out: &mpsc::UnboundedSender<Value>,
) {
    let Ok(Value::Object(mut frame)) = serde_json::from_str::<Value>(text) else {
        let _ = out.send(json!({ "error": { "code": 400, "message": "INPUT_FRAME_INVALID" } }));
        return;
    };
    let id = frame.remove("id").unwrap_or(Value::Null);
    let call = frame.remove("call").unwrap_or(Value::Null);
    let function: Function = match serde_json::from_value(call) {
        Ok(function) => function,
        Err(error) => {
            let error = RpcError::bad_request(format!("INPUT_METHOD_INVALID: {error}"));
            let _ = out.send(answer(id, Err(error)));
            return;
        }
    };
    let sandbox = sandbox.clone();
    let out = out.clone();
    tokio::spawn(async move {
        let result = sandbox
            .invoke(caller, function)
            .await
            .map(|reply| reply.to_json());
        let _ = out.send(answer(id, result));
    });
}

fn answer(id: Value, result: Result<Value, RpcError>) -> Value {
    match result {
        Ok(result) => json!({ "id": id, "result": result }),
        Err(error) => {
            json!({ "id": id, "error": { "code": error.code, "message": error.message } })
        }
    }
}
