//! Forums and threads at the MTProto level: topics and their messages, rights to
//! create, edit, close, pin and delete them, and reply threads in other supergroups.

use std::sync::Arc;

use simula_core::services::accounts::{self, NewUser};
use simula_core::{Account, AccountUpdates, ChannelId, Peer, RpcError, Sandbox, UserId};
use simula_tl::{enums, functions, types};
use tokio::sync::broadcast::Receiver;

use crate::common::{as_account, setup};

struct World {
    sandbox: Arc<Sandbox>,
    alice: Account,
    bob: Account,
}

async fn world() -> World {
    let (sandbox, alice, _bot) = setup().await;
    let bob = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: "Bob".into(),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    World {
        sandbox,
        alice,
        bob,
    }
}

fn peer(w: &World, viewer: UserId, channel: ChannelId) -> enums::InputPeer {
    w.sandbox.input_peer(viewer, Peer::Channel(channel))
}

async fn call<F>(w: &World, account: UserId, f: F) -> Result<F::Return, RpcError>
where
    F: simula_tl::RemoteCall + Into<simula_tl::Function>,
    F::Return: 'static,
{
    w.sandbox.call(as_account(account), f).await
}

/// Alice creates a supergroup (a forum with `forum`) with Bob in it.
async fn group(w: &World, forum: bool) -> ChannelId {
    let enums::Updates::Updates(created) = call(
        w,
        w.alice.id,
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
    let channel = created
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Channel(c) => Some(c.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(channel.forum, forum);
    let id = ChannelId(channel.id);
    let enums::InputPeer::Channel(input) = peer(w, w.alice.id, id) else {
        unreachable!()
    };
    let enums::InputPeer::User(bob) = w.sandbox.input_peer(w.alice.id, w.bob.id.into()) else {
        unreachable!()
    };
    call(
        w,
        w.alice.id,
        functions::channels::InviteToChannel {
            channel: types::InputChannel {
                channel_id: input.channel_id,
                access_hash: input.access_hash,
            }
            .into(),
            users: vec![
                types::InputUser {
                    user_id: bob.user_id,
                    access_hash: bob.access_hash,
                }
                .into(),
            ],
        },
    )
    .await
    .unwrap();
    id
}

/// The new channel message in a call's result.
fn new_message(updates: &enums::Updates) -> enums::Message {
    let enums::Updates::Updates(u) = updates else {
        panic!()
    };
    u.updates
        .iter()
        .find_map(|u| match u {
            enums::Update::NewChannelMessage(n) => Some(n.message.clone()),
            _ => None,
        })
        .expect("a new message")
}

fn header(message: &enums::Message) -> Option<types::MessageReplyHeader> {
    let reply_to = match message {
        enums::Message::Message(m) => &m.reply_to,
        enums::Message::Service(s) => &s.reply_to,
        _ => return None,
    };
    match reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) => Some((**h).clone()),
        _ => None,
    }
}

fn id_of(message: &enums::Message) -> i32 {
    match message {
        enums::Message::Message(m) => m.id,
        enums::Message::Service(s) => s.id,
        enums::Message::Empty(e) => e.id,
    }
}

async fn say(
    w: &World,
    from: UserId,
    channel: ChannelId,
    text: &str,
    reply_to: Option<(i32, Option<i32>)>,
) -> Result<enums::Message, RpcError> {
    call(
        w,
        from,
        functions::messages::SendMessage {
            peer: peer(w, from, channel),
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
    .map(|u| new_message(&u))
}

async fn create_topic(
    w: &World,
    from: UserId,
    channel: ChannelId,
    title: &str,
) -> Result<i32, RpcError> {
    call(
        w,
        from,
        functions::messages::CreateForumTopic {
            peer: peer(w, from, channel),
            title: title.into(),
            icon_color: Some(0xFFD67E),
            random_id: rand::random(),
            ..Default::default()
        },
    )
    .await
    .map(|u| id_of(&new_message(&u)))
}

async fn edit_topic(
    w: &World,
    from: UserId,
    channel: ChannelId,
    topic: i32,
    edit: functions::messages::EditForumTopic,
) -> Result<enums::Updates, RpcError> {
    call(
        w,
        from,
        functions::messages::EditForumTopic {
            peer: peer(w, from, channel),
            topic_id: topic,
            ..edit
        },
    )
    .await
}

async fn thread(w: &World, viewer: UserId, channel: ChannelId, top: i32) -> Vec<i32> {
    let enums::messages::Messages::ChannelMessages(found) = call(
        w,
        viewer,
        functions::messages::GetReplies {
            peer: peer(w, viewer, channel),
            msg_id: top,
            limit: 50,
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    found.messages.iter().map(id_of).collect()
}

async fn topics(w: &World, viewer: UserId, channel: ChannelId) -> Vec<types::ForumTopic> {
    let enums::messages::ForumTopics::ForumTopics(found) = call(
        w,
        viewer,
        functions::messages::GetForumTopics {
            peer: peer(w, viewer, channel),
            limit: 20,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    found
        .topics
        .into_iter()
        .filter_map(|t| match t {
            enums::ForumTopic::ForumTopic(t) => Some(*t),
            _ => None,
        })
        .collect()
}

fn code(err: RpcError) -> String {
    err.message
}

fn received(rx: &mut Receiver<Arc<AccountUpdates>>, account: UserId) -> Vec<enums::Update> {
    let mut out = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        if batch.account == account
            && let enums::Updates::Updates(u) = &batch.updates
        {
            out.extend(u.updates.iter().cloned());
        }
    }
    out
}

#[tokio::test]
async fn topics_and_their_messages() {
    let w = world().await;
    let hub = group(&w, true).await;
    let (alice, bob) = (w.alice.id, w.bob.id);

    // New supergroups keep topic creation to admins.
    assert_eq!(
        code(create_topic(&w, bob, hub, "Mine").await.unwrap_err()),
        "CHAT_ADMIN_REQUIRED"
    );
    assert_eq!(
        code(create_topic(&w, alice, hub, "  ").await.unwrap_err()),
        "TOPIC_TITLE_EMPTY"
    );
    let bugs = create_topic(&w, alice, hub, "Bugs").await.unwrap();

    // Writing into the topic, in General, and replying inside the topic.
    let in_topic = say(&w, bob, hub, "it crashes", Some((bugs, Some(bugs))))
        .await
        .unwrap();
    let h = header(&in_topic).unwrap();
    assert!(h.forum_topic);
    assert_eq!((h.reply_to_msg_id, h.reply_to_top_id), (Some(bugs), None));
    let general = say(&w, bob, hub, "hello all", None).await.unwrap();
    assert!(header(&general).is_none());
    let answer = say(&w, alice, hub, "fixed", Some((id_of(&in_topic), None)))
        .await
        .unwrap();
    let h = header(&answer).unwrap();
    assert!(h.forum_topic);
    assert_eq!(
        (h.reply_to_msg_id, h.reply_to_top_id),
        (Some(id_of(&in_topic)), Some(bugs))
    );

    let in_bugs = thread(&w, bob, hub, bugs).await;
    assert_eq!(in_bugs, vec![id_of(&answer), id_of(&in_topic), bugs]);
    let in_general = thread(&w, bob, hub, 1).await;
    assert!(in_general.contains(&id_of(&general)) && in_general.contains(&1));
    assert!(!in_general.contains(&bugs));

    let listed = topics(&w, bob, hub).await;
    assert_eq!(
        listed[0].id, bugs,
        "the topic with the newest message first"
    );
    assert_eq!(listed[0].title, "Bugs");
    assert_eq!(listed[0].icon_color, 0xFFD67E);
    assert_eq!(listed[0].top_message, id_of(&answer));
    assert!(!listed[0].my);
    assert_eq!(listed[1].id, 1);
    assert_eq!(listed[1].title, "General");

    // Closing: only the creator and admins with manage_topics post there.
    let close = functions::messages::EditForumTopic {
        closed: Some(true),
        ..Default::default()
    };
    assert_eq!(
        code(
            edit_topic(&w, bob, hub, bugs, close.clone())
                .await
                .unwrap_err()
        ),
        "CHAT_ADMIN_REQUIRED"
    );
    let closed = edit_topic(&w, alice, hub, bugs, close.clone())
        .await
        .unwrap();
    let notice = new_message(&closed);
    let enums::Message::Service(notice) = &notice else {
        panic!()
    };
    assert!(matches!(
        &notice.action,
        enums::MessageAction::TopicEdit(e) if e.closed == Some(true) && e.title.is_none()
    ));
    assert_eq!(
        header(&enums::Message::Service(notice.clone()))
            .unwrap()
            .reply_to_msg_id,
        Some(bugs)
    );
    assert_eq!(
        code(
            say(&w, bob, hub, "x", Some((bugs, Some(bugs))))
                .await
                .unwrap_err()
        ),
        "TOPIC_CLOSED"
    );
    say(&w, alice, hub, "still here", Some((bugs, Some(bugs))))
        .await
        .unwrap();
    assert_eq!(
        code(edit_topic(&w, alice, hub, bugs, close).await.unwrap_err()),
        "TOPIC_NOT_MODIFIED"
    );
    let both = functions::messages::EditForumTopic {
        closed: Some(false),
        title: Some("Issues".into()),
        ..Default::default()
    };
    assert_eq!(
        code(edit_topic(&w, alice, hub, bugs, both).await.unwrap_err()),
        "TOPIC_CLOSE_SEPARATELY"
    );
    let general_icon = functions::messages::EditForumTopic {
        icon_emoji_id: Some(5),
        ..Default::default()
    };
    assert_eq!(
        code(
            edit_topic(&w, alice, hub, 1, general_icon)
                .await
                .unwrap_err()
        ),
        "GENERAL_MODIFY_ICON_FORBIDDEN"
    );
    let rename = functions::messages::EditForumTopic {
        title: Some("Issues".into()),
        ..Default::default()
    };
    edit_topic(&w, alice, hub, bugs, rename).await.unwrap();

    // Pinning tells every member; pinned topics come first.
    let mut rx = w.sandbox.subscribe_updates();
    let ideas = create_topic(&w, alice, hub, "Ideas").await.unwrap();
    call(
        &w,
        alice,
        functions::messages::UpdatePinnedForumTopic {
            peer: peer(&w, alice, hub),
            topic_id: bugs,
            pinned: true,
        },
    )
    .await
    .unwrap();
    assert!(received(&mut rx, bob).iter().any(|u| matches!(
        u,
        enums::Update::PinnedForumTopic(p) if p.pinned && p.topic_id == bugs
    )));
    let listed = topics(&w, bob, hub).await;
    assert_eq!(
        listed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![bugs, ideas, 1]
    );
    assert!(listed[0].pinned && listed[0].closed && listed[0].title == "Issues");

    // Deleting takes the topic and everything in it.
    let enums::messages::AffectedHistory::AffectedHistory(deleted) = call(
        &w,
        alice,
        functions::messages::DeleteTopicHistory {
            peer: peer(&w, alice, hub),
            top_msg_id: bugs,
        },
    )
    .await
    .unwrap();
    assert!(deleted.pts_count >= 5);
    assert!(thread(&w, bob, hub, bugs).await.is_empty());
    let listed = topics(&w, bob, hub).await;
    assert_eq!(
        listed.iter().map(|t| t.id).collect::<Vec<_>>(),
        vec![ideas, 1]
    );
    assert_eq!(
        code(
            say(&w, alice, hub, "x", Some((bugs, Some(bugs))))
                .await
                .unwrap_err()
        ),
        "TOPIC_DELETED"
    );
}

#[tokio::test]
async fn reply_threads_and_turning_into_a_forum() {
    let w = world().await;
    let hub = group(&w, false).await;
    let (alice, bob) = (w.alice.id, w.bob.id);
    let root = say(&w, alice, hub, "plan?", None).await.unwrap();
    let first = say(&w, bob, hub, "A", Some((id_of(&root), None)))
        .await
        .unwrap();
    let h = header(&first).unwrap();
    assert_eq!(
        (h.reply_to_msg_id, h.reply_to_top_id, h.forum_topic),
        (Some(id_of(&root)), None, false)
    );
    let second = say(&w, alice, hub, "why A?", Some((id_of(&first), None)))
        .await
        .unwrap();
    let h = header(&second).unwrap();
    assert_eq!(
        h.reply_to_top_id,
        Some(id_of(&root)),
        "replies to replies stay in the thread"
    );
    assert_eq!(
        thread(&w, bob, hub, id_of(&root)).await,
        vec![id_of(&second), id_of(&first)]
    );

    assert_eq!(
        code(create_topic(&w, alice, hub, "Topic").await.unwrap_err()),
        "CHANNEL_FORUM_MISSING"
    );
    let enums::InputPeer::Channel(input) = peer(&w, bob, hub) else {
        unreachable!()
    };
    let toggle = |enabled| functions::channels::ToggleForum {
        channel: types::InputChannel {
            channel_id: input.channel_id,
            access_hash: input.access_hash,
        }
        .into(),
        enabled,
        tabs: false,
    };
    assert_eq!(
        code(call(&w, bob, toggle(true)).await.unwrap_err()),
        "CHAT_ADMIN_REQUIRED"
    );
    let enums::InputPeer::Channel(input) = peer(&w, alice, hub) else {
        unreachable!()
    };
    let toggle = |enabled| functions::channels::ToggleForum {
        channel: types::InputChannel {
            channel_id: input.channel_id,
            access_hash: input.access_hash,
        }
        .into(),
        enabled,
        tabs: false,
    };
    call(&w, alice, toggle(true)).await.unwrap();
    assert_eq!(
        code(call(&w, alice, toggle(true)).await.unwrap_err()),
        "CHAT_NOT_MODIFIED"
    );
    let general = thread(&w, bob, hub, 1).await;
    assert!(general.contains(&id_of(&root)) && general.contains(&id_of(&second)));
    let topic = create_topic(&w, alice, hub, "Topic").await.unwrap();
    assert!(topic > id_of(&second));
}
