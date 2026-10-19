//! Basic groups as a bot sees them: being added, privacy mode, service messages,
//! sending, and being removed.

use serde_json::json;
use simula_tl::functions;

use crate::support::{
    as_user, create_group, fails, next_updates, ok, post, user_removes, user_says_in_group, world,
};

#[tokio::test]
async fn joining_talking_and_being_removed() {
    let w = world().await;
    let mut offset = 0;
    let group = create_group(&w, "Team", &[]).await;

    let updates = next_updates(&w, &mut offset).await;
    let joined = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    assert_eq!(
        joined["chat"],
        json!({ "id": group, "type": "group", "title": "Team" })
    );
    assert_eq!(joined["from"]["id"], w.user.id.0);
    assert_eq!(joined["old_chat_member"]["status"], "left");
    assert_eq!(joined["new_chat_member"]["status"], "member");
    let created = updates
        .iter()
        .find_map(|u| u.get("message"))
        .expect("service message");
    assert_eq!(created["group_chat_created"], true);

    // Privacy mode: small talk stays with the people, commands reach the bot.
    user_says_in_group(&w, group, "hi all").await;
    user_says_in_group(&w, group, "/start").await;
    let updates = next_updates(&w, &mut offset).await;
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0]["message"]["text"], "/start");
    assert_eq!(updates[0]["message"]["chat"]["type"], "group");

    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": group, "text": "Hello, team" }),
    )
    .await;
    assert_eq!(sent["chat"]["id"], group);

    user_removes(&w, group, w.bot_id).await;
    let updates = next_updates(&w, &mut offset).await;
    let removed = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    assert_eq!(removed["old_chat_member"]["status"], "member");
    assert_eq!(removed["new_chat_member"]["status"], "left");
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": group, "text": "still here?" })
        )
        .await,
        "Forbidden: bot was kicked from the group chat"
    );
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": -4999999999i64, "text": "x" })
        )
        .await,
        "Bad Request: chat not found"
    );
}

#[tokio::test]
async fn chat_and_member_methods() {
    let w = world().await;
    let mut offset = 0;
    let group = create_group(&w, "Team", &[]).await;
    next_updates(&w, &mut offset).await;

    let chat = ok(&w, "getChat", json!({ "chat_id": group })).await;
    assert_eq!(
        (chat["type"].clone(), chat["title"].clone()),
        (json!("group"), json!("Team"))
    );
    assert_eq!(chat["permissions"]["can_send_messages"], true);
    assert_eq!(
        ok(&w, "getChatMemberCount", json!({ "chat_id": group })).await,
        2
    );
    let owner = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": group, "user_id": w.user.id.0 }),
    )
    .await;
    assert_eq!(
        (owner["status"].clone(), owner["user"]["first_name"].clone()),
        (json!("creator"), json!("Alice"))
    );
    let me = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": group, "user_id": w.bot_id.0 }),
    )
    .await;
    assert_eq!(me["status"], "member");
    let stranger = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": group, "user_id": 1 }),
    )
    .await;
    assert_eq!(stranger["status"], "left");
    let admins = ok(&w, "getChatAdministrators", json!({ "chat_id": group })).await;
    assert_eq!(admins.as_array().unwrap().len(), 1);

    assert_eq!(
        ok(
            &w,
            "setChatTitle",
            json!({ "chat_id": group, "title": "Team 2" })
        )
        .await,
        true
    );
    assert_eq!(
        ok(&w, "getChat", json!({ "chat_id": group })).await["title"],
        "Team 2"
    );

    assert_eq!(ok(&w, "leaveChat", json!({ "chat_id": group })).await, true);
    let left = next_updates(&w, &mut offset).await;
    let update = left
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .expect("my_chat_member");
    assert_eq!(update["new_chat_member"]["status"], "left");
    assert_eq!(
        fails(&w, "sendMessage", json!({ "chat_id": group, "text": "x" })).await,
        "Forbidden: bot is not a member of the group chat"
    );
    assert_eq!(
        fails(&w, "leaveChat", json!({ "chat_id": w.user.id.0 })).await,
        "Bad Request: can't leave private chats"
    );
    let private = ok(&w, "getChat", json!({ "chat_id": w.user.id.0 })).await;
    assert_eq!(
        (private["type"].clone(), private["first_name"].clone()),
        (json!("private"), json!("Alice"))
    );
}

#[tokio::test]
async fn upgraded_to_a_supergroup() {
    let w = world().await;
    let mut offset = 0;
    let group = create_group(&w, "Team", &[]).await;
    next_updates(&w, &mut offset).await;
    let chat_id = -group;
    as_user(&w, w.user.id, functions::messages::MigrateChat { chat_id })
        .await
        .unwrap();

    let updates = next_updates(&w, &mut offset).await;
    let messages: Vec<_> = updates.iter().filter_map(|u| u.get("message")).collect();
    assert_eq!(messages.len(), 2, "{updates:?}");
    let supergroup = messages[0]["migrate_to_chat_id"].as_i64().unwrap();
    assert!(supergroup < -1_000_000_000_000);
    assert_eq!(messages[0]["chat"]["id"], group);
    assert_eq!(messages[0]["from"]["id"], w.user.id.0);
    assert_eq!(messages[1]["migrate_from_chat_id"], group);
    assert_eq!(messages[1]["message_id"], 1);
    assert_eq!(
        messages[1]["chat"],
        json!({ "id": supergroup, "type": "supergroup", "title": "Team" })
    );

    // Writing to the old group says where it went.
    let (status, body) = post(
        &w,
        "sendMessage",
        json!({ "chat_id": group, "text": "hello?" }),
    )
    .await;
    assert_eq!(status, 400);
    assert_eq!(
        body,
        json!({
            "ok": false,
            "error_code": 400,
            "description": "Bad Request: group chat was upgraded to a supergroup chat",
            "parameters": { "migrate_to_chat_id": supergroup },
        })
    );
    // Reading it still works; the supergroup takes messages.
    assert_eq!(
        ok(&w, "getChat", json!({ "chat_id": group })).await["type"],
        "group"
    );
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": supergroup, "text": "here now" }),
    )
    .await;
    assert_eq!(sent["chat"]["type"], "supergroup");
}
