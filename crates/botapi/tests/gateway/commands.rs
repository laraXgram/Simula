//! `setMyCommands` / `getMyCommands` / `deleteMyCommands`: scopes, languages, and the
//! checks of tdlib (lengths, trimming) and of the server (characters, count).

use serde_json::json;

use crate::support::{fails, ok, user_writes, world};

#[tokio::test]
async fn commands_per_scope_and_language() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let set = json!({ "commands": [
        { "command": " /start ", "description": " Start the bot " },
        { "command": "help", "description": "Help", "is_ephemeral": true }
    ] });
    assert_eq!(ok(&w, "setMyCommands", set).await, true);
    assert_eq!(
        ok(&w, "getMyCommands", json!({})).await,
        json!([
            { "command": "start", "description": "Start the bot" },
            { "command": "help", "description": "Help", "is_ephemeral": true }
        ])
    );

    // Exactly the scope and language asked for; no fallback.
    ok(&w, "setMyCommands", json!({ "commands": [{ "command": "aghaz", "description": "شروع" }], "language_code": "fa" })).await;
    let persian = ok(&w, "getMyCommands", json!({ "language_code": "fa" })).await;
    assert_eq!(persian[0]["description"], "شروع");
    assert_eq!(
        ok(&w, "getMyCommands", json!({ "language_code": "de" })).await,
        json!([])
    );

    let chat = json!({ "type": "chat", "chat_id": w.user.id.0 });
    ok(
        &w,
        "setMyCommands",
        json!({ "commands": [{ "command": "vip", "description": "Only here" }], "scope": chat }),
    )
    .await;
    assert_eq!(
        ok(&w, "getMyCommands", json!({ "scope": chat })).await[0]["command"],
        "vip"
    );
    assert_eq!(
        ok(
            &w,
            "getMyCommands",
            json!({ "scope": { "type": "all_private_chats" } })
        )
        .await,
        json!([])
    );

    assert_eq!(ok(&w, "deleteMyCommands", json!({})).await, true);
    assert_eq!(ok(&w, "getMyCommands", json!({})).await, json!([]));
}

#[tokio::test]
async fn command_errors() {
    let w = world().await;
    user_writes(&w, "/start").await;
    let one = |command: &str, description: &str| json!({ "commands": [{ "command": command, "description": description }] });
    let cases = [
        (one("Hello", "x"), "Bad Request: BOT_COMMAND_INVALID"),
        (one("/", "x"), "Bad Request: command must be non-empty"),
        (
            one(&"a".repeat(33), "x"),
            "Bad Request: command length must not exceed 32",
        ),
        (
            one("ok", "  "),
            "Bad Request: command description must be non-empty",
        ),
        (
            one("ok", &"d".repeat(257)),
            "Bad Request: command description length must not exceed 256",
        ),
        (
            json!({ "commands": [], "language_code": "EN" }),
            "Bad Request: invalid language code specified",
        ),
        (
            json!({ "commands": [], "scope": { "type": "chat_administrators", "chat_id": w.user.id.0 } }),
            "Bad Request: can't use specified scope in private chats",
        ),
        (
            json!({ "commands": [], "scope": { "type": "chat", "chat_id": 42 } }),
            "Bad Request: chat not found",
        ),
    ];
    for (params, description) in cases {
        assert_eq!(fails(&w, "setMyCommands", params).await, description);
    }
    let many: Vec<_> = (0..101)
        .map(|i| json!({ "command": format!("c{i}"), "description": "x" }))
        .collect();
    assert_eq!(
        fails(&w, "setMyCommands", json!({ "commands": many })).await,
        "Bad Request: BOT_COMMANDS_TOO_MUCH"
    );
}
