//! Webhooks end to end, with a real HTTP receiver on a local port: delivery, the secret
//! token header, a method call in the response, errors and retries, and the conflicts
//! with `getUpdates`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::support::{World, fails, ok, post as call, user_copy, user_writes, world};
use axum::Router;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::post;
use serde_json::{Value, json};

/// What the receiver saw, and how it answers.
#[derive(Default)]
struct Receiver {
    seen: Mutex<Vec<(HeaderMap, Value)>>,
    /// Answers to give, in order; afterwards `200` with an empty body.
    script: Mutex<Vec<(StatusCode, String)>>,
}

async fn receiver() -> (Arc<Receiver>, String) {
    let receiver = Arc::new(Receiver::default());
    let app = Router::new().route(
        "/hook",
        post({
            let receiver = receiver.clone();
            move |headers: HeaderMap, body: String| {
                let receiver = receiver.clone();
                async move {
                    let update = serde_json::from_str(&body).unwrap_or(Value::Null);
                    receiver.seen.lock().unwrap().push((headers, update));
                    let mut script = receiver.script.lock().unwrap();
                    let (status, body) = if script.is_empty() {
                        (StatusCode::OK, String::new())
                    } else {
                        script.remove(0)
                    };
                    (status, [("content-type", "application/json")], body).into_response()
                }
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/hook", listener.local_addr().unwrap());
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (receiver, url)
}

/// Poll `getWebhookInfo` until `done` holds (deliveries are asynchronous).
async fn info_until(w: &World, done: impl Fn(&Value) -> bool) -> Value {
    for _ in 0..100 {
        let info = ok(w, "getWebhookInfo", json!({})).await;
        if done(&info) {
            return info;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!(
        "webhook state never settled: {}",
        ok(w, "getWebhookInfo", json!({})).await
    );
}

async fn set_webhook(w: &World, params: Value) -> Value {
    let (status, body) = call(w, "setWebhook", params).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    body
}

#[tokio::test]
async fn updates_are_posted_and_replies_in_the_response_run() {
    let w = world().await;
    let (receiver, url) = receiver().await;
    let chat_id = w.user.id.0;
    receiver.script.lock().unwrap().push((
        StatusCode::OK,
        json!({ "method": "sendMessage", "chat_id": chat_id, "text": "echo: hi" }).to_string(),
    ));

    let body = set_webhook(&w, json!({ "url": url, "secret_token": "s3cr3t_-" })).await;
    assert_eq!(
        body,
        json!({ "ok": true, "result": true, "description": "Webhook was set" })
    );

    user_writes(&w, "hi").await;
    let info = info_until(&w, |i| {
        i["pending_update_count"] == 0 && !receiver.seen.lock().unwrap().is_empty()
    })
    .await;
    assert_eq!(info["url"], url);
    assert_eq!(info["max_connections"], 40);
    assert_eq!(info["ip_address"], "127.0.0.1");
    assert!(info.get("last_error_message").is_none());

    let (headers, update) = receiver.seen.lock().unwrap()[0].clone();
    assert_eq!(headers["x-telegram-bot-api-secret-token"], "s3cr3t_-");
    assert_eq!(update["message"]["text"], "hi");
    assert!(update["update_id"].is_number());

    // The method in the response was sent as the bot.
    for _ in 0..100 {
        if user_copy(&w, 2).await.is_some() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(user_copy(&w, 2).await.unwrap().message, "echo: hi");

    assert_eq!(
        fails(&w, "getUpdates", json!({})).await,
        "Conflict: can't use getUpdates method while webhook is active; use deleteWebhook to delete the webhook first"
    );
}

#[tokio::test]
async fn failed_deliveries_are_reported_and_retried() {
    let w = world().await;
    let (receiver, url) = receiver().await;
    receiver
        .script
        .lock()
        .unwrap()
        .push((StatusCode::INTERNAL_SERVER_ERROR, String::new()));
    set_webhook(&w, json!({ "url": url })).await;
    user_writes(&w, "retry me").await;

    // The first retry is immediate and succeeds; the error stays on record.
    let info = info_until(&w, |i| i["pending_update_count"] == 0).await;
    assert_eq!(
        info["last_error_message"],
        "Wrong response from the webhook: 500 Internal Server Error"
    );
    assert!(info["last_error_date"].is_number());
    let seen = receiver.seen.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].1["update_id"], seen[1].1["update_id"]);
}

#[tokio::test]
async fn unreachable_webhooks() {
    let w = world().await;
    // A free port with nothing listening.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    set_webhook(
        &w,
        json!({ "url": format!("http://127.0.0.1:{port}/hook") }),
    )
    .await;
    user_writes(&w, "anyone?").await;
    let info = info_until(&w, |i| i.get("last_error_message").is_some()).await;
    assert_eq!(info["last_error_message"], "Connection refused");
    assert_eq!(info["pending_update_count"], 1);
}

#[tokio::test]
async fn setting_and_deleting() {
    let w = world().await;
    let (_receiver, url) = receiver().await;
    let description = |body: Value| body["description"].as_str().unwrap().to_owned();

    // A pending long poll ends when a webhook is set.
    let poll = {
        let (app, token) = (w.app.clone(), w.token.clone());
        tokio::spawn(async move {
            let request = axum::http::Request::post(format!("/bot{token}/getUpdates"))
                .header("content-type", "application/json")
                .body(axum::body::Body::from(r#"{"timeout": 10}"#))
                .unwrap();
            crate::support::send(&app, request).await
        })
    };
    tokio::time::sleep(Duration::from_millis(100)).await;
    set_webhook(&w, json!({ "url": url })).await;
    let (status, body) = poll.await.unwrap();
    assert_eq!(status, StatusCode::CONFLICT);
    assert_eq!(
        body["description"],
        "Conflict: terminated by setWebhook request"
    );

    // One setWebhook with a URL per second.
    let (status, body) = call(&w, "setWebhook", json!({ "url": url })).await;
    assert_eq!(
        (status, body["parameters"]["retry_after"].clone()),
        (StatusCode::TOO_MANY_REQUESTS, json!(1))
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        description(set_webhook(&w, json!({ "url": url })).await),
        "Webhook is already set"
    );
    assert_eq!(
        description(set_webhook(&w, json!({ "url": "" })).await),
        "Webhook was deleted"
    );
    let (_, body) = call(&w, "deleteWebhook", json!({})).await;
    assert_eq!(description(body), "Webhook is already deleted");
    assert_eq!(ok(&w, "getWebhookInfo", json!({})).await["url"], "");

    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_eq!(
        fails(
            &w,
            "setWebhook",
            json!({ "url": url, "secret_token": "no spaces" })
        )
        .await,
        "Bad Request: secret token contains illegal characters"
    );
}

#[tokio::test]
async fn https_urls_are_delivered_over_plain_http() {
    // Frameworks register https:// and serve HTTP behind a TLS terminator (Telegraf).
    let w = world().await;
    let (receiver, url) = receiver().await;
    let https = url.replace("http://", "https://");
    set_webhook(&w, json!({ "url": https })).await;
    user_writes(&w, "hi").await;
    info_until(&w, |i| i["pending_update_count"] == 0).await;
    assert_eq!(receiver.seen.lock().unwrap().len(), 1);
}
