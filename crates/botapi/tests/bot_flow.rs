//! The Bot API over HTTP, in process: what a bot library would see.

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::{Value, json};
use simula_botapi::Gateway;
use simula_core::services::accounts::{self, NewBot, NewUser};
use simula_core::{Account, Caller, Sandbox, SandboxConfig};
use simula_tl::functions;
use tower::ServiceExt;

struct World {
    app: Router,
    sandbox: Arc<Sandbox>,
    user: Account,
    token: String,
}

async fn world() -> World {
    let sandbox = Sandbox::open(SandboxConfig { id: "test".into(), path: None }).unwrap();
    let user = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser { first_name: "Alice".into(), lang_code: Some("en".into()), ..Default::default() },
            )
        })
        .await
        .unwrap();
    let bot = sandbox
        .admin(|ctx| {
            accounts::create_bot(ctx, NewBot { first_name: "Echo".into(), username: "echo_bot".into(), ..Default::default() })
        })
        .await
        .unwrap();
    let app = simula_botapi::router(Gateway::new(sandbox.clone()));
    World { app, sandbox, user, token: bot.bot_token.unwrap() }
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Value) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap())
}

async fn post(w: &World, method: &str, body: Value) -> (StatusCode, Value) {
    let request = Request::post(format!("/bot{}/{method}", w.token))
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    send(&w.app, request).await
}

async fn user_writes(w: &World, text: &str) {
    let bot_id = w.token.split(':').next().unwrap().parse().unwrap();
    let peer = w.sandbox.input_peer(w.user.id, simula_core::Peer::User(simula_core::UserId(bot_id)));
    w.sandbox
        .call(
            Caller { account: w.user.id, session: None },
            functions::messages::SendMessage { peer, message: text.into(), random_id: rand::random(), ..Default::default() },
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn get_me_describes_the_bot() {
    let w = world().await;
    let (status, body) = post(&w, "getMe", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    let me = &body["result"];
    assert_eq!(me["is_bot"], true);
    assert_eq!(me["username"], "echo_bot");
    assert_eq!(me["can_join_groups"], true);
    assert_eq!(me["can_read_all_group_messages"], false, "privacy mode is on by default");
    assert_eq!(me["supports_inline_queries"], false);
}

#[tokio::test]
async fn conversation_round_trip() {
    let w = world().await;
    let user_id = w.user.id.0;

    let (status, body) = post(&w, "sendMessage", json!({ "chat_id": user_id, "text": "hi" })).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(body["description"], "Forbidden: bot can't initiate conversation with a user");

    user_writes(&w, "/start").await;

    let (_, body) = post(&w, "getUpdates", json!({})).await;
    let updates = body["result"].as_array().unwrap();
    assert_eq!(updates.len(), 1);
    let message = &updates[0]["message"];
    assert_eq!(message["text"], "/start");
    assert_eq!(message["message_id"], 1);
    assert_eq!(message["chat"], json!({ "id": user_id, "type": "private", "first_name": "Alice" }));
    assert_eq!(message["from"], json!({ "id": user_id, "is_bot": false, "first_name": "Alice", "language_code": "en" }));

    let (status, body) = post(&w, "sendMessage", json!({ "chat_id": user_id, "text": "Hello, Alice!" })).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["result"]["message_id"], 2, "the bot's box: 1 = incoming, 2 = this reply");
    assert_eq!(body["result"]["from"]["is_bot"], true);
    assert_eq!(body["result"]["chat"]["id"], user_id);

    // Confirming with offset empties the queue; the bot's own reply is never echoed.
    let offset = updates[0]["update_id"].as_i64().unwrap() + 1;
    let (_, body) = post(&w, "getUpdates", json!({ "offset": offset })).await;
    assert_eq!(body["result"], json!([]));
}

#[tokio::test]
async fn form_and_query_encodings_are_coerced() {
    let w = world().await;
    user_writes(&w, "hello").await;

    let request = Request::get(format!("/bot{}/getUpdates?timeout=0&limit=1", w.token)).body(Body::empty()).unwrap();
    let (_, body) = send(&w.app, request).await;
    assert_eq!(body["result"][0]["message"]["text"], "hello");

    let form = format!("chat_id={}&text=from+a+form&disable_notification=true", w.user.id.0);
    let request = Request::post(format!("/bot{}/sendMessage", w.token))
        .header("content-type", "application/x-www-form-urlencoded")
        .body(Body::from(form))
        .unwrap();
    let (status, body) = send(&w.app, request).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["result"]["text"], "from a form");
}

#[tokio::test]
async fn telegram_error_texts() {
    let w = world().await;
    let cases = [
        (json!({ "chat_id": 123456 }), 400, "Bad Request: message text is empty"),
        (json!({ "chat_id": 123456, "text": "x" }), 400, "Bad Request: chat not found"),
        (json!({ "chat_id": "@nobody_here", "text": "x" }), 400, "Bad Request: chat not found"),
    ];
    for (params, code, description) in cases {
        let (status, body) = post(&w, "sendMessage", params).await;
        assert_eq!((status.as_u16(), body["description"].as_str().unwrap()), (code, description));
        assert_eq!(body["ok"], false);
        assert_eq!(body["error_code"], code);
    }

    let (status, body) = post(&w, "getMee", json!({})).await;
    assert_eq!((status, body["description"].as_str().unwrap()), (StatusCode::NOT_FOUND, "Not Found"));
    let (status, _) = post(&w, "sendPhoto", json!({})).await;
    assert_eq!(status, StatusCode::NOT_IMPLEMENTED);

    let wrong = Request::post("/bot123:wrong/getMe").body(Body::empty()).unwrap();
    assert_eq!(send(&w.app, wrong).await.1["description"], "Unauthorized");
    let malformed = Request::post("/botnonsense/getMe").body(Body::empty()).unwrap();
    assert_eq!(send(&w.app, malformed).await.1["description"], "Not Found");
}

#[tokio::test]
async fn long_polling_wakes_up_and_newer_poll_wins() {
    let w = Arc::new(world().await);

    let poller = {
        let w = w.clone();
        tokio::spawn(async move { post(&w, "getUpdates", json!({ "timeout": 10 })).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    user_writes(&w, "wake up").await;
    let (_, body) = tokio::time::timeout(Duration::from_secs(2), poller).await.unwrap().unwrap();
    assert_eq!(body["result"][0]["message"]["text"], "wake up");

    let offset = body["result"][0]["update_id"].as_i64().unwrap() + 1;
    let older = {
        let w = w.clone();
        tokio::spawn(async move { post(&w, "getUpdates", json!({ "offset": offset, "timeout": 10 })).await })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    let newer = {
        let w = w.clone();
        tokio::spawn(async move { post(&w, "getUpdates", json!({ "offset": offset, "timeout": 1 })).await })
    };
    let (status, body) = tokio::time::timeout(Duration::from_secs(2), older).await.unwrap().unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["description"],
        "Conflict: terminated by other getUpdates request; make sure that only one bot instance is running"
    );
    let (status, _) = newer.await.unwrap();
    assert_eq!(status, StatusCode::OK);
}
