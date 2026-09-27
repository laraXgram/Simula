//! Callback buttons end to end: a user presses, the bot gets `callback_query`, answers
//! with `answerCallbackQuery`, and the user sees the answer.

use std::time::Duration;

use crate::support::{fails, next_updates, ok, user_presses, user_writes, world};
use serde_json::json;
use simula_tl::enums;

#[tokio::test]
async fn press_and_answer() {
    let w = world().await;
    let chat_id = w.user.id.0;
    let mut offset = 0;
    user_writes(&w, "/start").await;
    next_updates(&w, &mut offset).await;
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "Pick", "reply_parameters": { "message_id": 1 },
                "reply_markup": { "inline_keyboard": [[{ "text": "Go", "callback_data": "go" }]] } }),
    )
    .await;

    let pressing = user_presses(&w, 2, "go");
    let updates = next_updates(&w, &mut offset).await;
    let query = &updates[0]["callback_query"];
    assert_eq!(query["from"]["id"], chat_id);
    assert_eq!(query["data"], "go");
    assert_eq!(query["message"]["message_id"], sent["message_id"]);
    assert_eq!(query["message"]["text"], "Pick");
    assert_eq!(query["message"]["reply_markup"], sent["reply_markup"]);
    assert_eq!(query["message"]["reply_to_message"]["text"], "/start");
    assert!(
        query["chat_instance"]
            .as_str()
            .unwrap()
            .parse::<i64>()
            .is_ok()
    );

    assert_eq!(
        ok(
            &w,
            "answerCallbackQuery",
            json!({ "callback_query_id": query["id"], "text": "Done!", "show_alert": true })
        )
        .await,
        true
    );
    let enums::messages::BotCallbackAnswer::BotCallbackAnswer(answer) =
        pressing.await.unwrap().unwrap();
    assert!(answer.alert);
    assert_eq!(answer.message.as_deref(), Some("Done!"));

    assert_eq!(
        fails(
            &w,
            "answerCallbackQuery",
            json!({ "callback_query_id": query["id"] })
        )
        .await,
        "Bad Request: query is too old and response timeout expired or query ID is invalid"
    );
}

#[tokio::test]
async fn answer_errors_and_timeouts() {
    let w = world().await;
    let chat_id = w.user.id.0;
    let mut offset = 0;
    user_writes(&w, "/start").await;
    ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "Pick",
        "reply_markup": { "inline_keyboard": [[{ "text": "Go", "callback_data": "go" }]] } }),
    )
    .await;
    next_updates(&w, &mut offset).await;

    let pressing = user_presses(&w, 2, "go");
    let id = next_updates(&w, &mut offset).await[0]["callback_query"]["id"].clone();
    assert_eq!(
        fails(
            &w,
            "answerCallbackQuery",
            json!({ "callback_query_id": id, "text": "x".repeat(201) })
        )
        .await,
        "Bad Request: MESSAGE_TOO_LONG"
    );
    assert_eq!(
        fails(
            &w,
            "answerCallbackQuery",
            json!({ "callback_query_id": id, "url": "https://example.com" })
        )
        .await,
        "Bad Request: URL_INVALID"
    );
    assert_eq!(
        ok(
            &w,
            "answerCallbackQuery",
            json!({ "callback_query_id": id, "url": "https://t.me/echo_bot?start=pay" })
        )
        .await,
        true
    );
    let enums::messages::BotCallbackAnswer::BotCallbackAnswer(answer) =
        pressing.await.unwrap().unwrap();
    assert_eq!(
        answer.url.as_deref(),
        Some("https://t.me/echo_bot?start=pay")
    );

    // A bot that never answers leaves the user with BOT_RESPONSE_TIMEOUT.
    w.sandbox.set_answer_timeout(Duration::from_millis(50));
    let err = user_presses(&w, 2, "go").await.unwrap().unwrap_err();
    assert_eq!(err.message, "BOT_RESPONSE_TIMEOUT");
}
