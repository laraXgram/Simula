//! Replies to messages in other chats: what the bot sees (`external_reply` and an
//! automatic `quote`) and `reply_parameters.chat_id` when it replies itself.

use serde_json::json;
use simula_core::{ChannelId, Peer};
use simula_tl::{functions, types};

use crate::support::{
    World, as_user, create_channel, fails, input_channel, input_user, next_updates, ok,
    says_in_channel, user_writes, world,
};

async fn make_admin(w: &World, channel: ChannelId) {
    as_user(
        w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(w, w.user.id, channel),
            user_id: input_user(w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                post_messages: true,
                delete_messages: true,
                other: true,
                ..Default::default()
            }
            .into(),
            rank: None,
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn replies_across_chats() {
    let w = world().await;
    let mut offset = 0;
    let news = create_channel(&w, true, "News").await;
    let dev = create_channel(&w, false, "Dev").await;
    make_admin(&w, news).await;
    make_admin(&w, dev).await;
    let news_id = Peer::Channel(news).bot_api_id();
    let post = says_in_channel(&w, w.user.id, news, "Big news today")
        .await
        .unwrap();
    next_updates(&w, &mut offset).await;

    // The user replies in the supergroup to the channel's post.
    as_user(
        &w,
        w.user.id,
        functions::messages::SendMessage {
            peer: w.sandbox.input_peer(w.user.id, Peer::Channel(dev)),
            message: "seen this?".into(),
            random_id: 7,
            reply_to: Some(
                types::InputReplyToMessage {
                    reply_to_msg_id: post,
                    reply_to_peer_id: Some(w.sandbox.input_peer(w.user.id, Peer::Channel(news))),
                    ..Default::default()
                }
                .into(),
            ),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let message = &updates[0]["message"];
    assert!(message.get("reply_to_message").is_none());
    let news_chat = json!({ "id": news_id, "type": "channel", "title": "News" });
    let external = &message["external_reply"];
    assert_eq!(external["origin"]["type"], "channel");
    assert_eq!(external["origin"]["chat"], news_chat);
    assert_eq!(external["origin"]["message_id"], post);
    assert!(external["origin"]["date"].as_i64().unwrap() > 0);
    assert_eq!(external["chat"], news_chat);
    assert_eq!(external["message_id"], post);
    assert_eq!(
        message["quote"],
        json!({ "text": "Big news today", "position": 0 })
    );

    // The bot replies to the post from the private chat, quoting part of it.
    user_writes(&w, "hello").await;
    let sent = ok(
        &w,
        "sendMessage",
        json!({
            "chat_id": w.user.id.0,
            "text": "about that",
            "reply_parameters": { "chat_id": news_id, "message_id": post, "quote": "news" },
        }),
    )
    .await;
    assert_eq!(sent["external_reply"]["origin"]["chat"]["id"], news_id);
    assert_eq!(
        sent["quote"],
        json!({ "text": "news", "position": 4, "is_manual": true })
    );
    // The same chat named explicitly is an ordinary reply.
    let same = ok(
        &w,
        "sendMessage",
        json!({
            "chat_id": news_id,
            "text": "follow-up",
            "reply_parameters": { "chat_id": news_id, "message_id": post },
        }),
    )
    .await;
    assert_eq!(same["reply_to_message"]["message_id"], post);
    assert!(same.get("external_reply").is_none());

    // Protected posts and missing messages: sent without a reply, or refused.
    let protected = as_user(
        &w,
        w.user.id,
        functions::messages::SendMessage {
            peer: w.sandbox.input_peer(w.user.id, Peer::Channel(news)),
            message: "members only".into(),
            random_id: 8,
            noforwards: true,
            ..Default::default()
        },
    )
    .await;
    assert!(protected.is_ok());
    let sent = ok(
        &w,
        "sendMessage",
        json!({
            "chat_id": w.user.id.0,
            "text": "can't quote that",
            "reply_parameters": { "chat_id": news_id, "message_id": post + 2 },
        }),
    )
    .await;
    assert!(sent.get("external_reply").is_none() && sent.get("quote").is_none());
    let reply_to_missing = json!({
        "chat_id": w.user.id.0,
        "text": "x",
        "reply_parameters": { "chat_id": news_id, "message_id": 999 },
    });
    assert_eq!(
        fails(&w, "sendMessage", reply_to_missing.clone()).await,
        "Bad Request: message to be replied not found"
    );
    let mut lenient = reply_to_missing;
    lenient["reply_parameters"]["allow_sending_without_reply"] = json!(true);
    assert!(
        ok(&w, "sendMessage", lenient)
            .await
            .get("external_reply")
            .is_none()
    );

    // Chats the bot isn't in: private ones aren't readable, public ones' messages not.
    let other = create_channel(&w, true, "Other").await;
    let other_id = Peer::Channel(other).bot_api_id();
    let reply_in_other = json!({
        "chat_id": w.user.id.0,
        "text": "x",
        "reply_parameters": { "chat_id": other_id, "message_id": 2 },
    });
    assert_eq!(
        fails(&w, "sendMessage", reply_in_other.clone()).await,
        "Forbidden: bot is not a member of the channel chat"
    );
    as_user(
        &w,
        w.user.id,
        functions::channels::UpdateUsername {
            channel: input_channel(&w, w.user.id, other),
            username: "simula_other".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        fails(&w, "sendMessage", reply_in_other).await,
        "Bad Request: message to be replied not found"
    );
}
