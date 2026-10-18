//! Basic groups as a bot sees them: being added, privacy mode, service messages,
//! sending, and being removed.

use serde_json::json;

use crate::support::{
    create_group, fails, next_updates, ok, user_removes, user_says_in_group, world,
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
