//! Pinned messages from the bot's side: pinning, the notice the bot gets when the user
//! pins, getChat's pinned_message, unpinning, and the right it takes in supergroups.

use serde_json::json;
use simula_core::Peer;
use simula_tl::functions;

use crate::support::{
    as_user, create_channel, fails, input_channel, input_user, next_updates, ok, user_writes, world,
};

#[tokio::test]
async fn pinning_and_unpinning() {
    let w = world().await;
    let mut offset = 0;
    user_writes(&w, "hello").await;
    let first = next_updates(&w, &mut offset).await[0]["message"]["message_id"].clone();
    ok(
        &w,
        "pinChatMessage",
        json!({ "chat_id": w.user.id.0, "message_id": first }),
    )
    .await;
    let chat = ok(&w, "getChat", json!({ "chat_id": w.user.id.0 })).await;
    assert_eq!(chat["pinned_message"]["text"], "hello");

    // When the user pins, the bot gets the notice with the pinned message.
    let mine = user_writes(&w, "important").await;
    let second = next_updates(&w, &mut offset).await[0]["message"]["message_id"].clone();
    as_user(
        &w,
        w.user.id,
        functions::messages::UpdatePinnedMessage {
            peer: w.sandbox.input_peer(w.user.id, Peer::User(w.bot_id)),
            id: mine,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let notice = &updates[0]["message"];
    assert_eq!(notice["pinned_message"]["text"], "important");
    assert_eq!(notice["pinned_message"]["message_id"], second);

    // Without an id, the newest pinned message goes.
    ok(&w, "unpinChatMessage", json!({ "chat_id": w.user.id.0 })).await;
    let chat = ok(&w, "getChat", json!({ "chat_id": w.user.id.0 })).await;
    assert_eq!(chat["pinned_message"]["text"], "hello");
    ok(
        &w,
        "unpinAllChatMessages",
        json!({ "chat_id": w.user.id.0 }),
    )
    .await;
    let chat = ok(&w, "getChat", json!({ "chat_id": w.user.id.0 })).await;
    assert!(chat.get("pinned_message").is_none());
    assert_eq!(
        fails(&w, "unpinChatMessage", json!({ "chat_id": w.user.id.0 })).await,
        "Bad Request: message to unpin not found"
    );
}

#[tokio::test]
async fn pinning_in_a_supergroup_takes_the_right() {
    let w = world().await;
    let dev = create_channel(&w, false, "Dev").await;
    let chat_id = Peer::Channel(dev).bot_api_id();
    as_user(
        &w,
        w.user.id,
        functions::channels::InviteToChannel {
            channel: input_channel(&w, w.user.id, dev),
            users: vec![input_user(&w, w.user.id, w.bot_id)],
        },
    )
    .await
    .unwrap();
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "rules" }),
    )
    .await;
    assert_eq!(
        fails(
            &w,
            "pinChatMessage",
            json!({ "chat_id": chat_id, "message_id": sent["message_id"] })
        )
        .await,
        "Bad Request: not enough rights to manage pinned messages in the chat"
    );
}
