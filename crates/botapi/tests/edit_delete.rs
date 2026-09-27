//! Editing and deleting messages through the Bot API, and users' edits reaching bots.

mod support;

use serde_json::json;
use support::{fails, next_updates, ok, user_copy, user_edits, user_writes, world};

fn keyboard(data: &str) -> serde_json::Value {
    json!({ "inline_keyboard": [[{ "text": "Go", "callback_data": data }]] })
}

#[tokio::test]
async fn editing_text_and_keyboards() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let chat_id = w.user.id.0;
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "v1", "reply_markup": keyboard("a") }),
    )
    .await;
    let id = sent["message_id"].clone();

    // Editing the text without a keyboard removes it, as on Telegram.
    let edited = ok(
        &w,
        "editMessageText",
        json!({ "chat_id": chat_id, "message_id": id, "text": "<i>v2</i>", "parse_mode": "HTML" }),
    )
    .await;
    assert_eq!(edited["text"], "v2");
    assert_eq!(edited["entities"][0]["type"], "italic");
    assert!(edited["edit_date"].is_number());
    assert!(edited.get("reply_markup").is_none());
    let copy = user_copy(&w, 2).await.unwrap();
    assert_eq!((copy.message.as_str(), copy.reply_markup), ("v2", None));

    let with_keyboard = ok(
        &w,
        "editMessageReplyMarkup",
        json!({ "chat_id": chat_id, "message_id": id, "reply_markup": keyboard("b") }),
    )
    .await;
    assert_eq!(with_keyboard["text"], "v2");
    assert_eq!(with_keyboard["reply_markup"], keyboard("b"));

    assert_eq!(
        fails(
            &w,
            "editMessageReplyMarkup",
            json!({ "chat_id": chat_id, "message_id": id, "reply_markup": keyboard("b") })
        )
        .await,
        "Bad Request: message is not modified: specified new message content and reply markup are exactly the same as a current content and reply markup of the message"
    );
}

#[tokio::test]
async fn edit_errors() {
    let w = world().await;
    user_writes(&w, "hi").await;
    let chat_id = w.user.id.0;
    let cases = [
        (
            json!({ "chat_id": chat_id, "message_id": 1, "text": "x" }),
            "Bad Request: message can't be edited",
        ),
        (
            json!({ "chat_id": chat_id, "message_id": 50, "text": "x" }),
            "Bad Request: message to edit not found",
        ),
        (
            json!({ "text": "x" }),
            "Bad Request: message identifier is not specified",
        ),
        (
            json!({ "chat_id": chat_id, "message_id": 1 }),
            "Bad Request: message text is empty",
        ),
    ];
    for (params, description) in cases {
        assert_eq!(fails(&w, "editMessageText", params).await, description);
    }
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "Pick",
        "reply_markup": { "keyboard": [["A"]] } }),
    )
    .await;
    assert_eq!(
        fails(
            &w,
            "editMessageText",
            json!({ "chat_id": chat_id, "message_id": sent["message_id"], "text": "x" })
        )
        .await,
        "Bad Request: message can't be edited",
        "messages with a reply keyboard can't be edited"
    );
}

#[tokio::test]
async fn users_edits_reach_the_bot() {
    let w = world().await;
    let mut offset = 0;
    let id = user_writes(&w, "helo").await;
    next_updates(&w, &mut offset).await;
    user_edits(&w, id, "hello").await;
    let updates = next_updates(&w, &mut offset).await;
    let edited = &updates[0]["edited_message"];
    assert_eq!(edited["text"], "hello");
    assert_eq!(edited["message_id"], 1);
    assert!(edited["edit_date"].is_number());
    assert!(updates[0].get("message").is_none());
}

#[tokio::test]
async fn deleting_messages() {
    let w = world().await;
    user_writes(&w, "one").await;
    let chat_id = w.user.id.0;
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "two" }),
    )
    .await;

    assert_eq!(
        ok(
            &w,
            "deleteMessage",
            json!({ "chat_id": chat_id, "message_id": sent["message_id"] })
        )
        .await,
        true
    );
    assert!(user_copy(&w, 2).await.is_none(), "deleted for the user too");
    assert_eq!(
        fails(
            &w,
            "deleteMessage",
            json!({ "chat_id": chat_id, "message_id": sent["message_id"] })
        )
        .await,
        "Bad Request: message to delete not found"
    );

    // Missing ids are skipped; the user's own message goes too.
    assert_eq!(
        ok(
            &w,
            "deleteMessages",
            json!({ "chat_id": chat_id, "message_ids": [1, 77] })
        )
        .await,
        true
    );
    assert!(user_copy(&w, 1).await.is_none());
    let too_many: Vec<i64> = (1..=101).collect();
    assert_eq!(
        fails(
            &w,
            "deleteMessages",
            json!({ "chat_id": chat_id, "message_ids": too_many })
        )
        .await,
        "Bad Request: too many message identifiers specified"
    );
}

#[tokio::test]
async fn old_messages_cant_be_deleted_by_bots() {
    let w = world().await;
    user_writes(&w, "old").await;
    w.sandbox.clock().advance(49 * 3600 * 1000);
    assert_eq!(
        fails(
            &w,
            "deleteMessage",
            json!({ "chat_id": w.user.id.0, "message_id": 1 })
        )
        .await,
        "Bad Request: message can't be deleted for everyone"
    );
}
