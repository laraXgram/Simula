//! `sendMessage` through the shared send pipeline: formatting, keyboards, replies,
//! options, legacy parameters and Telegram's error texts.

mod support;

use serde_json::json;
use support::{fails, next_updates, ok, post_form, user_sends, user_writes, world};

#[tokio::test]
async fn formatting_with_any_parse_mode_casing() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let chat_id = w.user.id.0;

    for mode in ["HTML", "html"] {
        let sent = ok(
            &w,
            "sendMessage",
            json!({ "chat_id": chat_id, "text": "  <b>Hi</b> <i>there</i>\n", "parse_mode": mode }),
        )
        .await;
        assert_eq!(sent["text"], "Hi there");
        assert_eq!(
            sent["entities"],
            json!([
                { "type": "bold", "offset": 0, "length": 2 },
                { "type": "italic", "offset": 3, "length": 5 }
            ])
        );
    }

    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "*bold* [me](tg://user?id=USER)".replace("USER", &chat_id.to_string()), "parse_mode": "MarkdownV2" }),
    )
    .await;
    assert_eq!(sent["entities"][1]["type"], "text_mention");
    assert_eq!(sent["entities"][1]["user"]["first_name"], "Alice");

    let explicit = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "code", "entities": [{ "type": "code", "offset": 0, "length": 4 }] }),
    )
    .await;
    assert_eq!(explicit["entities"][0]["type"], "code");
}

#[tokio::test]
async fn formatting_errors_come_before_the_chat_is_looked_at() {
    let w = world().await;
    let unknown_chat = 42;
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": unknown_chat, "text": "<b>x", "parse_mode": "HTML" })
        )
        .await,
        "Bad Request: can't parse entities: Can't find end tag corresponding to start tag \"b\""
    );
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": unknown_chat, "text": "x", "parse_mode": "BBCode" })
        )
        .await,
        "Bad Request: unsupported parse_mode"
    );
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": unknown_chat, "text": " " })
        )
        .await,
        "Bad Request: chat not found",
        "whitespace is tdlib's check, after the chat"
    );
}

#[tokio::test]
async fn inline_keyboards_round_trip() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let keyboard = json!({ "inline_keyboard": [
        [{ "text": "Yes", "callback_data": "yes", "style": "success" }, { "text": "Site", "url": "https://example.com" }],
        [{ "text": "Share", "switch_inline_query": "" }, { "text": "Copy", "copy_text": { "text": "42" } }]
    ] });
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": w.user.id.0, "text": "Pick", "reply_markup": keyboard }),
    )
    .await;
    assert_eq!(
        sent["reply_markup"],
        json!({ "inline_keyboard": [
            [{ "text": "Yes", "callback_data": "yes", "style": "success" }, { "text": "Site", "url": "https://example.com/" }],
            [{ "text": "Share", "switch_inline_query": "" }, { "text": "Copy", "copy_text": { "text": "42" } }]
        ] })
    );

    let too_long = "x".repeat(65);
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": w.user.id.0, "text": "x",
            "reply_markup": { "inline_keyboard": [[{ "text": "a", "callback_data": too_long }]] } })
        )
        .await,
        "Bad Request: BUTTON_DATA_INVALID"
    );
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": w.user.id.0, "text": "x",
            "reply_markup": { "inline_keyboard": [[{ "text": "plain" }]] } })
        )
        .await,
        "Bad Request: text buttons are not allowed in the inline keyboard"
    );
}

#[tokio::test]
async fn reply_keyboards_accept_bare_strings_from_forms() {
    let w = world().await;
    user_writes(&w, "/start").await;
    // What Telegraf's `Markup.keyboard([["Yes", "No"]])` sends.
    let (status, body) = post_form(
        &w,
        "sendMessage",
        &[
            ("chat_id", &w.user.id.0.to_string()),
            ("text", "Sure?"),
            (
                "reply_markup",
                r#"{"keyboard":[["Yes","No"]],"resize_keyboard":true}"#,
            ),
        ],
    )
    .await;
    assert_eq!(status.as_u16(), 200, "{body}");
    // Bots never see reply keyboards on messages.
    assert!(body["result"].get("reply_markup").is_none());
}

#[tokio::test]
async fn replies_in_both_directions() {
    let w = world().await;
    let chat_id = w.user.id.0;
    let mut offset = 0;
    user_writes(&w, "What is the answer to everything?").await;
    let question = next_updates(&w, &mut offset).await[0]["message"].clone();

    let answer = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "42",
                "reply_parameters": { "message_id": question["message_id"], "quote": "the answer" } }),
    )
    .await;
    assert_eq!(answer["reply_to_message"]["text"], question["text"]);
    assert!(answer["reply_to_message"].get("reply_to_message").is_none());
    assert_eq!(
        answer["quote"],
        json!({ "text": "the answer", "position": 8, "is_manual": true })
    );

    // The user replies to the bot's answer; the bot gets the answer back as context.
    let answer_in_user_box = 2;
    user_sends(&w, "thanks", Some(answer_in_user_box)).await;
    let thanks = next_updates(&w, &mut offset).await[0]["message"].clone();
    assert_eq!(
        thanks["reply_to_message"]["message_id"],
        answer["message_id"]
    );
    assert_eq!(thanks["reply_to_message"]["text"], "42");
    assert_eq!(thanks["reply_to_message"]["from"]["is_bot"], true);
}

#[tokio::test]
async fn missing_reply_targets() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let chat_id = w.user.id.0;
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": chat_id, "text": "x", "reply_parameters": { "message_id": 99 } })
        )
        .await,
        "Bad Request: message to be replied not found"
    );
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "x",
                "reply_parameters": { "message_id": 99, "allow_sending_without_reply": true } }),
    )
    .await;
    assert!(sent.get("reply_to_message").is_none());
}

#[tokio::test]
async fn legacy_parameters_still_work() {
    let w = world().await;
    user_writes(&w, "hello").await;
    let (_, body) = post_form(
        &w,
        "sendMessage",
        &[
            ("chat_id", &w.user.id.0.to_string()),
            ("text", "see https://example.com/page"),
            ("reply_to_message_id", "1"),
            ("disable_web_page_preview", "true"),
        ],
    )
    .await;
    assert_eq!(body["result"]["reply_to_message"]["text"], "hello");
    assert_eq!(
        body["result"]["link_preview_options"],
        json!({ "is_disabled": true })
    );
}

#[tokio::test]
async fn message_options() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": w.user.id.0, "text": "secret https://example.com/x",
                "protect_content": true, "message_effect_id": "5104841245755180586",
                "link_preview_options": { "show_above_text": true } }),
    )
    .await;
    assert_eq!(sent["has_protected_content"], true);
    assert_eq!(sent["effect_id"], "5104841245755180586");
    // Simula never builds previews, so a link reads as "preview disabled".
    assert_eq!(
        sent["link_preview_options"],
        json!({ "is_disabled": true, "show_above_text": true })
    );
}
