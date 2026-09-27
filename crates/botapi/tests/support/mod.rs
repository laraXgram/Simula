//! A sandbox with one user and one bot, and the Bot API over HTTP in process: what a bot
//! library would see.

#![allow(dead_code, reason = "each test file uses a different subset")]

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use simula_botapi::Gateway;
use simula_core::services::accounts::{self, NewBot, NewUser};
use simula_core::{Account, Caller, Peer, Sandbox, SandboxConfig, UserId};
use simula_tl::{enums, functions, types};
use tower::ServiceExt;

pub struct World {
    pub app: Router,
    pub sandbox: Arc<Sandbox>,
    pub user: Account,
    pub bot_id: UserId,
    pub token: String,
}

pub async fn world() -> World {
    let sandbox = Sandbox::open(SandboxConfig {
        id: "test".into(),
        path: None,
    })
    .unwrap();
    let user = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: "Alice".into(),
                    lang_code: Some("en".into()),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    let bot = sandbox
        .admin(|ctx| {
            accounts::create_bot(
                ctx,
                NewBot {
                    first_name: "Echo".into(),
                    username: "echo_bot".into(),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    let app = simula_botapi::router(Gateway::new(sandbox.clone()));
    World {
        app,
        sandbox,
        user,
        bot_id: bot.id,
        token: bot.bot_token.unwrap(),
    }
}

pub async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// A JSON request.
pub async fn post(w: &World, method: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::post(format!("/bot{}/{method}", w.token))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    send(&w.app, request).await
}

/// An urlencoded form request, as many libraries send.
pub async fn post_form(w: &World, method: &str, form: &[(&str, &str)]) -> (StatusCode, Value) {
    let body = serde_urlencoded::to_string(form).unwrap();
    let request = Request::post(format!("/bot{}/{method}", w.token))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(body))
        .unwrap();
    send(&w.app, request).await
}

/// The `result` of a call that must succeed.
pub async fn ok(w: &World, method: &str, body: Value) -> Value {
    let (status, body) = post(w, method, body).await;
    assert_eq!(status, StatusCode::OK, "{method}: {body}");
    body["result"].clone()
}

/// The `description` of a call that must fail.
pub async fn fails(w: &World, method: &str, body: Value) -> String {
    let (status, body) = post(w, method, body).await;
    assert_ne!(status, StatusCode::OK, "{method} should fail: {body}");
    body["description"].as_str().unwrap().to_owned()
}

/// The user writes to the bot over TL; returns the id in the user's own box.
pub async fn user_writes(w: &World, text: &str) -> i32 {
    user_sends(w, text, None).await
}

/// The user replies to their own-box message `reply_to` over TL.
pub async fn user_sends(w: &World, text: &str, reply_to: Option<i32>) -> i32 {
    let peer = w.sandbox.input_peer(w.user.id, Peer::User(w.bot_id));
    let updates = w
        .sandbox
        .call(
            Caller {
                account: w.user.id,
                session: None,
            },
            functions::messages::SendMessage {
                peer,
                message: text.into(),
                random_id: rand::random(),
                reply_to: reply_to.map(|id| {
                    types::InputReplyToMessage {
                        reply_to_msg_id: id,
                        ..Default::default()
                    }
                    .into()
                }),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::Updates::Updates(batch) = updates else {
        panic!("updates expected");
    };
    batch
        .updates
        .iter()
        .find_map(|u| match u {
            enums::Update::MessageID(u) => Some(u.id),
            _ => None,
        })
        .unwrap()
}

/// The next `getUpdates` result, confirming everything before it.
pub async fn next_updates(w: &World, offset: &mut i64) -> Vec<Value> {
    let result = ok(w, "getUpdates", serde_json::json!({ "offset": *offset })).await;
    let updates = result.as_array().unwrap().clone();
    if let Some(last) = updates.last() {
        *offset = last["update_id"].as_i64().unwrap() + 1;
    }
    updates
}
