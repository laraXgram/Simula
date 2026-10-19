//! Forums as a bot sees them: topics it creates and manages, messages in topics
//! (`message_thread_id`, `is_topic_message`, the topic's creation message as the reply),
//! tdlib's rights checks, and reply threads in other supergroups.

use serde_json::{Value, json};
use simula_core::{ChannelId, Peer};
use simula_tl::{enums, functions, types};

use crate::support::{World, as_user, fails, input_channel, input_user, next_updates, ok, world};

async fn supergroup(w: &World, forum: bool) -> ChannelId {
    let enums::Updates::Updates(created) = as_user(
        w,
        w.user.id,
        functions::channels::CreateChannel {
            megagroup: true,
            forum,
            title: "Hub".into(),
            about: String::new(),
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    created
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Channel(c) => Some(ChannelId(c.id)),
            _ => None,
        })
        .unwrap()
}

async fn promote(w: &World, channel: ChannelId, manage_topics: bool) {
    as_user(
        w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(w, w.user.id, channel),
            user_id: input_user(w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                manage_topics,
                delete_messages: true,
                pin_messages: true,
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

/// The user writes in the supergroup, optionally replying (`reply_to`, `top`).
async fn user_says(
    w: &World,
    channel: ChannelId,
    text: &str,
    reply_to: Option<(i32, Option<i32>)>,
) -> i32 {
    let enums::Updates::Updates(sent) = as_user(
        w,
        w.user.id,
        functions::messages::SendMessage {
            peer: w.sandbox.input_peer(w.user.id, Peer::Channel(channel)),
            message: text.into(),
            random_id: rand::random(),
            reply_to: reply_to.map(|(id, top)| {
                types::InputReplyToMessage {
                    reply_to_msg_id: id,
                    top_msg_id: top,
                    ..Default::default()
                }
                .into()
            }),
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    sent.updates
        .iter()
        .find_map(|u| match u {
            enums::Update::MessageID(u) => Some(u.id),
            _ => None,
        })
        .unwrap()
}

async fn user_creates_topic(w: &World, channel: ChannelId, title: &str) -> i32 {
    let enums::Updates::Updates(created) = as_user(
        w,
        w.user.id,
        functions::messages::CreateForumTopic {
            peer: w.sandbox.input_peer(w.user.id, Peer::Channel(channel)),
            title: title.into(),
            random_id: rand::random(),
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    created
        .updates
        .iter()
        .find_map(|u| match u {
            enums::Update::NewChannelMessage(u) => match &u.message {
                enums::Message::Service(s) => Some(s.id),
                _ => None,
            },
            _ => None,
        })
        .unwrap()
}

fn messages(updates: &[Value]) -> Vec<&Value> {
    updates.iter().filter_map(|u| u.get("message")).collect()
}

#[tokio::test]
async fn topics_in_a_forum() {
    let w = world().await;
    let mut offset = 0;
    let hub = supergroup(&w, true).await;
    let chat_id = Peer::Channel(hub).bot_api_id();
    promote(&w, hub, true).await;
    let updates = next_updates(&w, &mut offset).await;
    let joined = updates
        .iter()
        .find_map(|u| u.get("my_chat_member"))
        .unwrap();
    assert_eq!(joined["chat"]["is_forum"], true);
    assert_eq!(
        ok(&w, "getChat", json!({ "chat_id": chat_id })).await["is_forum"],
        true
    );

    // The bot opens a topic; the user opens another and writes in it.
    let bugs = ok(
        &w,
        "createForumTopic",
        json!({ "chat_id": chat_id, "name": "  Bugs  ", "icon_color": 0xFFD67E }),
    )
    .await;
    assert_eq!(bugs["name"], "Bugs");
    assert_eq!(bugs["icon_color"], 0xFFD67E);
    let bugs = bugs["message_thread_id"].as_i64().unwrap() as i32;
    let ideas = user_creates_topic(&w, hub, "Ideas").await;
    let asked = user_says(&w, hub, "dark mode?", Some((ideas, Some(ideas)))).await;
    let updates = next_updates(&w, &mut offset).await;
    let got = messages(&updates);
    assert_eq!(
        got[0]["forum_topic_created"],
        json!({ "name": "Ideas", "icon_color": 0x6FB9F0 })
    );
    assert_eq!(got[0]["message_thread_id"], ideas);
    assert_eq!(got[0]["is_topic_message"], true);
    assert_eq!(got[1]["text"], "dark mode?");
    assert_eq!(got[1]["message_thread_id"], ideas);
    assert_eq!(got[1]["is_topic_message"], true);
    assert_eq!(got[1]["reply_to_message"]["message_id"], ideas);
    assert_eq!(
        got[1]["reply_to_message"]["forum_topic_created"]["name"],
        "Ideas"
    );
    assert_eq!(got[1]["chat"]["is_forum"], true);

    // Sending into topics: replies stay within one topic.
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "message_thread_id": ideas, "text": "soon", "reply_parameters": { "message_id": asked } }),
    )
    .await;
    assert_eq!(sent["message_thread_id"], ideas);
    assert_eq!(sent["reply_to_message"]["message_id"], asked);
    let moved = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "message_thread_id": bugs, "text": "elsewhere", "reply_parameters": { "message_id": asked } }),
    )
    .await;
    assert_eq!(moved["message_thread_id"], bugs);
    assert_eq!(
        moved["reply_to_message"]["message_id"], bugs,
        "a reply from another topic is dropped"
    );
    let general = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "text": "hi all" }),
    )
    .await;
    assert!(
        general.get("message_thread_id").is_none() && general.get("is_topic_message").is_none()
    );
    for thread in [1, 999] {
        assert_eq!(
            fails(
                &w,
                "sendMessage",
                json!({ "chat_id": chat_id, "message_thread_id": thread, "text": "x" })
            )
            .await,
            "Bad Request: message thread not found"
        );
    }

    // Managing topics; the user's reopening reaches the bot as a service message.
    assert!(
        ok(
            &w,
            "closeForumTopic",
            json!({ "chat_id": chat_id, "message_thread_id": ideas })
        )
        .await
            == true
    );
    as_user(
        &w,
        w.user.id,
        functions::messages::EditForumTopic {
            peer: w.sandbox.input_peer(w.user.id, Peer::Channel(hub)),
            topic_id: ideas,
            closed: Some(false),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let updates = next_updates(&w, &mut offset).await;
    let reopened = messages(&updates)
        .into_iter()
        .find(|m| m.get("forum_topic_reopened").is_some())
        .unwrap();
    assert_eq!(reopened["message_thread_id"], ideas);
    assert_eq!(
        reopened["reply_to_message"]["forum_topic_created"]["name"],
        "Ideas"
    );
    ok(
        &w,
        "editForumTopic",
        json!({ "chat_id": chat_id, "message_thread_id": bugs, "name": "Issues" }),
    )
    .await;
    ok(
        &w,
        "editForumTopic",
        json!({ "chat_id": chat_id, "message_thread_id": bugs }),
    )
    .await;
    assert_eq!(
        fails(
            &w,
            "editForumTopic",
            json!({ "chat_id": chat_id, "message_thread_id": bugs, "name": "Issues" })
        )
        .await,
        "Bad Request: TOPIC_NOT_MODIFIED"
    );
    ok(
        &w,
        "editGeneralForumTopic",
        json!({ "chat_id": chat_id, "name": "Lobby" }),
    )
    .await;
    ok(&w, "hideGeneralForumTopic", json!({ "chat_id": chat_id })).await;
    ok(&w, "unhideGeneralForumTopic", json!({ "chat_id": chat_id })).await;
    ok(&w, "closeGeneralForumTopic", json!({ "chat_id": chat_id })).await;
    ok(&w, "reopenGeneralForumTopic", json!({ "chat_id": chat_id })).await;
    ok(
        &w,
        "unpinAllForumTopicMessages",
        json!({ "chat_id": chat_id, "message_thread_id": ideas }),
    )
    .await;
    ok(
        &w,
        "unpinAllGeneralForumTopicMessages",
        json!({ "chat_id": chat_id }),
    )
    .await;
    ok(
        &w,
        "deleteForumTopic",
        json!({ "chat_id": chat_id, "message_thread_id": bugs }),
    )
    .await;
    assert_eq!(
        fails(
            &w,
            "sendMessage",
            json!({ "chat_id": chat_id, "message_thread_id": bugs, "text": "x" })
        )
        .await,
        "Bad Request: message thread not found"
    );
    assert_eq!(
        fails(
            &w,
            "closeForumTopic",
            json!({ "chat_id": chat_id, "message_thread_id": 0 })
        )
        .await,
        "Bad Request: invalid forum topic identifier specified"
    );

    // Without can_manage_topics: no new topics (members may not either), and only its
    // own topics to close.
    promote(&w, hub, false).await;
    assert_eq!(
        fails(
            &w,
            "createForumTopic",
            json!({ "chat_id": chat_id, "name": "x" })
        )
        .await,
        "Bad Request: not enough rights to create a topic"
    );
    assert_eq!(
        fails(
            &w,
            "closeForumTopic",
            json!({ "chat_id": chat_id, "message_thread_id": ideas })
        )
        .await,
        "Bad Request: not enough rights to close or open the topic"
    );
    assert_eq!(
        fails(&w, "hideGeneralForumTopic", json!({ "chat_id": chat_id })).await,
        "Bad Request: not enough rights to close or open the topic"
    );
}

#[tokio::test]
async fn reply_threads_outside_forums() {
    let w = world().await;
    let mut offset = 0;
    let hub = supergroup(&w, false).await;
    let chat_id = Peer::Channel(hub).bot_api_id();
    promote(&w, hub, false).await;
    next_updates(&w, &mut offset).await;
    assert_eq!(
        fails(
            &w,
            "createForumTopic",
            json!({ "chat_id": chat_id, "name": "x" })
        )
        .await,
        "Bad Request: the chat is not a forum"
    );
    let root = user_says(&w, hub, "plan?", None).await;
    let first = user_says(&w, hub, "A", Some((root, None))).await;
    user_says(&w, hub, "why A?", Some((first, None))).await;
    let updates = next_updates(&w, &mut offset).await;
    let got = messages(&updates);
    assert!(got[0].get("message_thread_id").is_none());
    assert_eq!(got[1]["message_thread_id"], root);
    assert_eq!(got[2]["message_thread_id"], root);
    assert!(got[2].get("is_topic_message").is_none());
    // message_thread_id is ignored outside forums.
    let sent = ok(
        &w,
        "sendMessage",
        json!({ "chat_id": chat_id, "message_thread_id": root, "text": "ok" }),
    )
    .await;
    assert!(sent.get("message_thread_id").is_none());
}
