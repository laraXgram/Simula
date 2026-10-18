//! Supergroups and channels at the MTProto level: one box per channel with its own pts,
//! privacy mode, reading, editing, deleting, admin rights, restrictions and bans,
//! public channels, and bots as channel admins.

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
    bot: Account,
}

async fn world() -> World {
    let (sandbox, alice, bot) = setup().await;
    let bob = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: "Bob".into(),
                    username: Some("bobby".into()),
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
        bot,
    }
}

fn input_user(sandbox: &Sandbox, viewer: UserId, user: UserId) -> enums::InputUser {
    let enums::InputPeer::User(peer) = sandbox.input_peer(viewer, user.into()) else {
        unreachable!()
    };
    types::InputUser {
        user_id: peer.user_id,
        access_hash: peer.access_hash,
    }
    .into()
}

fn input_peer(sandbox: &Sandbox, viewer: UserId, channel: ChannelId) -> enums::InputPeer {
    sandbox.input_peer(viewer, Peer::Channel(channel))
}

fn input_channel(sandbox: &Sandbox, viewer: UserId, channel: ChannelId) -> enums::InputChannel {
    let enums::InputPeer::Channel(peer) = input_peer(sandbox, viewer, channel) else {
        unreachable!()
    };
    types::InputChannel {
        channel_id: peer.channel_id,
        access_hash: peer.access_hash,
    }
    .into()
}

/// Alice creates a supergroup (or channel) and returns its id.
async fn create(w: &World, broadcast: bool, title: &str) -> ChannelId {
    let updates = w
        .sandbox
        .call(
            as_account(w.alice.id),
            functions::channels::CreateChannel {
                broadcast,
                megagroup: !broadcast,
                title: title.into(),
                about: "about it".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::Updates::Updates(updates) = updates else {
        panic!("updates expected");
    };
    let channel = updates.chats.iter().find_map(|c| match c {
        enums::Chat::Channel(c) => Some(c.clone()),
        _ => None,
    });
    let channel = channel.expect("the new channel");
    assert!(channel.creator && channel.megagroup == !broadcast && channel.broadcast == broadcast);
    ChannelId(channel.id)
}

async fn invite(
    w: &World,
    actor: UserId,
    channel: ChannelId,
    users: &[UserId],
) -> Result<(), RpcError> {
    w.sandbox
        .call(
            as_account(actor),
            functions::channels::InviteToChannel {
                channel: input_channel(&w.sandbox, actor, channel),
                users: users
                    .iter()
                    .map(|&u| input_user(&w.sandbox, actor, u))
                    .collect(),
            },
        )
        .await
        .map(|_| ())
}

async fn say(w: &World, from: UserId, channel: ChannelId, text: &str) -> Result<i32, RpcError> {
    let updates = w
        .sandbox
        .call(
            as_account(from),
            functions::messages::SendMessage {
                peer: input_peer(&w.sandbox, from, channel),
                message: text.into(),
                random_id: rand::random(),
                ..Default::default()
            },
        )
        .await?;
    let enums::Updates::Updates(updates) = updates else {
        panic!("updates expected");
    };
    Ok(updates
        .updates
        .iter()
        .find_map(|u| match u {
            enums::Update::MessageID(u) => Some(u.id),
            _ => None,
        })
        .expect("updateMessageID"))
}

/// Every update `account` received so far.
fn received(rx: &mut Receiver<Arc<AccountUpdates>>, account: UserId) -> Vec<enums::Update> {
    let mut out = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        if batch.account != account {
            continue;
        }
        if let enums::Updates::Updates(u) = &batch.updates {
            out.extend(u.updates.iter().cloned());
        }
    }
    out
}

/// `(id, text, out, pts)` of the new channel messages among `updates`.
fn new_messages(updates: &[enums::Update]) -> Vec<(i32, String, bool, i32)> {
    updates
        .iter()
        .filter_map(|u| match u {
            enums::Update::NewChannelMessage(u) => match &u.message {
                enums::Message::Message(m) => Some((m.id, m.message.clone(), m.out, u.pts)),
                enums::Message::Service(m) => Some((m.id, String::new(), m.out, u.pts)),
                _ => None,
            },
            _ => None,
        })
        .collect()
}

fn code(err: RpcError) -> (i32, String) {
    (err.code, err.message)
}

#[tokio::test]
async fn one_box_for_everyone_and_privacy_mode() {
    let w = world().await;
    let dev = create(&w, false, "Dev").await;
    let mut rx = w.sandbox.subscribe_updates();
    invite(&w, w.alice.id, dev, &[w.bob.id, w.bot.id])
        .await
        .unwrap();
    // One service message (id 2 after the creation message) for everyone, with the
    // channel's pts.
    let bob = new_messages(&received(&mut rx, w.bob.id));
    assert_eq!(bob, [(2, String::new(), false, 2)]);

    let mut rx = w.sandbox.subscribe_updates();
    assert_eq!(say(&w, w.alice.id, dev, "hello").await.unwrap(), 3);
    assert_eq!(say(&w, w.bob.id, dev, "/start").await.unwrap(), 4);
    assert_eq!(say(&w, w.bob.id, dev, "/help@echo_bot").await.unwrap(), 5);
    let updates = received(&mut rx, w.bob.id);
    assert_eq!(
        new_messages(&updates),
        [
            (3, "hello".into(), false, 3),
            (4, "/start".into(), true, 4),
            (5, "/help@echo_bot".into(), true, 5)
        ]
    );
    // The bot (privacy mode, not an admin) only gets commands; the ids are the same.
    let mut rx = w.sandbox.subscribe_updates();
    say(&w, w.alice.id, dev, "small talk").await.unwrap();
    say(&w, w.alice.id, dev, "/start").await.unwrap();
    let bot: Vec<_> = new_messages(&received(&mut rx, w.bot.id))
        .into_iter()
        .map(|(id, text, _, _)| (id, text))
        .collect();
    assert_eq!(bot, [(7, "/start".into())]);

    // History is the shared box, newest first, as each viewer sees it.
    let history = w
        .sandbox
        .call(
            as_account(w.bob.id),
            functions::messages::GetHistory {
                peer: input_peer(&w.sandbox, w.bob.id, dev),
                limit: 3,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::messages::Messages::ChannelMessages(history) = history else {
        panic!("channelMessages expected");
    };
    assert_eq!((history.count, history.pts), (7, 7));
    let outs: Vec<(i32, bool)> = history
        .messages
        .iter()
        .filter_map(|m| match m {
            enums::Message::Message(m) => Some((m.id, m.out)),
            _ => None,
        })
        .collect();
    assert_eq!(outs, [(7, false), (6, false), (5, true)]);

    // The chat list has the supergroup with its pts.
    let dialogs = w
        .sandbox
        .call(
            as_account(w.bob.id),
            functions::messages::GetDialogs {
                offset_peer: enums::InputPeer::Empty,
                limit: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::messages::Dialogs::Dialogs(dialogs) = dialogs else {
        panic!("dialogs expected");
    };
    let enums::Dialog::Dialog(dialog) = &dialogs.dialogs[0] else {
        panic!("dialog expected");
    };
    assert_eq!(Peer::from_tl(&dialog.peer), Peer::Channel(dev));
    assert_eq!((dialog.top_message, dialog.pts), (7, Some(7)));
}

#[tokio::test]
async fn reading_editing_deleting() {
    let w = world().await;
    let dev = create(&w, false, "Dev").await;
    invite(&w, w.alice.id, dev, &[w.bob.id]).await.unwrap();
    let hello = say(&w, w.alice.id, dev, "hello").await.unwrap();
    let mut rx = w.sandbox.subscribe_updates();
    let read = w
        .sandbox
        .call(
            as_account(w.bob.id),
            functions::channels::ReadHistory {
                channel: input_channel(&w.sandbox, w.bob.id, dev),
                max_id: 0,
            },
        )
        .await
        .unwrap();
    assert!(read);
    let alice = received(&mut rx, w.alice.id);
    assert!(
        alice
            .iter()
            .any(|u| matches!(u, enums::Update::ReadChannelOutbox(u) if u.max_id == hello))
    );

    // Only the author edits; everyone gets the edit with the channel's next pts.
    let edit = |editor: UserId, text: &str| functions::messages::EditMessage {
        peer: input_peer(&w.sandbox, editor, dev),
        id: hello,
        message: Some(text.into()),
        ..Default::default()
    };
    let err = w
        .sandbox
        .call(as_account(w.bob.id), edit(w.bob.id, "hacked"))
        .await
        .unwrap_err();
    assert_eq!(code(err), (403, "MESSAGE_AUTHOR_REQUIRED".into()));
    let mut rx = w.sandbox.subscribe_updates();
    w.sandbox
        .call(as_account(w.alice.id), edit(w.alice.id, "hello!"))
        .await
        .unwrap();
    assert!(received(&mut rx, w.bob.id).iter().any(|u| matches!(
        u,
        enums::Update::EditChannelMessage(u)
            if matches!(&u.message, enums::Message::Message(m) if m.message == "hello!")
    )));

    // Members delete their own messages; deleting others' takes delete_messages.
    let mine = say(&w, w.bob.id, dev, "oops").await.unwrap();
    let delete = |caller: UserId, id: i32| functions::channels::DeleteMessages {
        channel: input_channel(&w.sandbox, caller, dev),
        id: vec![id],
    };
    let err = w
        .sandbox
        .call(as_account(w.bob.id), delete(w.bob.id, hello))
        .await
        .unwrap_err();
    assert_eq!(code(err), (403, "MESSAGE_DELETE_FORBIDDEN".into()));
    let enums::messages::AffectedMessages::AffectedMessages(affected) = w
        .sandbox
        .call(as_account(w.bob.id), delete(w.bob.id, mine))
        .await
        .unwrap();
    assert_eq!(affected.pts_count, 1);
    w.sandbox
        .call(as_account(w.alice.id), delete(w.alice.id, hello))
        .await
        .unwrap();
}

#[tokio::test]
async fn admins_restrictions_and_bans() {
    let w = world().await;
    let dev = create(&w, false, "Dev").await;
    invite(&w, w.alice.id, dev, &[w.bob.id, w.bot.id])
        .await
        .unwrap();
    let rename = |actor: UserId, title: &str| functions::channels::EditTitle {
        channel: input_channel(&w.sandbox, actor, dev),
        title: title.into(),
    };
    // New supergroups don't let members change the group's info.
    let err = w
        .sandbox
        .call(as_account(w.bob.id), rename(w.bob.id, "Bob's"))
        .await
        .unwrap_err();
    assert_eq!(code(err), (400, "CHAT_ADMIN_REQUIRED".into()));

    // Promoting the bot tells it (updateChannelParticipant), with its rights.
    let mut rx = w.sandbox.subscribe_updates();
    w.sandbox
        .call(
            as_account(w.alice.id),
            functions::channels::EditAdmin {
                channel: input_channel(&w.sandbox, w.alice.id, dev),
                user_id: input_user(&w.sandbox, w.alice.id, w.bot.id),
                admin_rights: types::ChatAdminRights {
                    change_info: true,
                    delete_messages: true,
                    ban_users: true,
                    ..Default::default()
                }
                .into(),
                rank: Some("helper".into()),
            },
        )
        .await
        .unwrap();
    let change = received(&mut rx, w.bot.id)
        .into_iter()
        .find_map(|u| match u {
            enums::Update::ChannelParticipant(p) => Some(p),
            _ => None,
        })
        .expect("updateChannelParticipant");
    assert!(matches!(
        change.new_participant,
        Some(enums::ChannelParticipant::Admin(ref a))
            if a.self_ && matches!(&a.admin_rights, enums::ChatAdminRights::ChatAdminRights(r) if r.change_info)
    ));
    // An admin can't grant rights it lacks.
    let err = w
        .sandbox
        .call(
            as_account(w.bot.id),
            functions::channels::EditAdmin {
                channel: input_channel(&w.sandbox, w.bot.id, dev),
                user_id: input_user(&w.sandbox, w.bot.id, w.bob.id),
                admin_rights: types::ChatAdminRights {
                    change_info: true,
                    ..Default::default()
                }
                .into(),
                rank: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(code(err), (400, "CHAT_ADMIN_REQUIRED".into()));
    w.sandbox
        .call(as_account(w.bot.id), rename(w.bot.id, "Dev team"))
        .await
        .unwrap();

    // Restricted members can't send; lifting it restores them.
    let restrict = |rights: types::ChatBannedRights| functions::channels::EditBanned {
        channel: input_channel(&w.sandbox, w.bot.id, dev),
        participant: w.sandbox.input_peer(w.bot.id, w.bob.id.into()),
        banned_rights: rights.into(),
    };
    w.sandbox
        .call(
            as_account(w.bot.id),
            restrict(types::ChatBannedRights {
                send_messages: true,
                send_plain: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let err = say(&w, w.bob.id, dev, "let me talk").await.unwrap_err();
    assert_eq!(code(err), (403, "CHAT_SEND_PLAIN_FORBIDDEN".into()));
    w.sandbox
        .call(
            as_account(w.bot.id),
            restrict(types::ChatBannedRights::default()),
        )
        .await
        .unwrap();
    say(&w, w.bob.id, dev, "thanks").await.unwrap();

    // Banned: Bob sees channelForbidden and can't write.
    let mut rx = w.sandbox.subscribe_updates();
    w.sandbox
        .call(
            as_account(w.bot.id),
            restrict(types::ChatBannedRights {
                view_messages: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
    let bob = received(&mut rx, w.bob.id);
    assert!(bob.iter().any(|u| matches!(u, enums::Update::Channel(_))));
    let err = say(&w, w.bob.id, dev, "hello?").await.unwrap_err();
    assert_eq!(code(err), (403, "CHANNEL_PRIVATE".into()));
    let chats = w
        .sandbox
        .call(
            as_account(w.bob.id),
            functions::channels::GetChannels {
                id: vec![input_channel(&w.sandbox, w.bob.id, dev)],
            },
        )
        .await
        .unwrap();
    let enums::messages::Chats::Chats(chats) = chats else {
        panic!("chats expected");
    };
    assert!(matches!(chats.chats[0], enums::Chat::ChannelForbidden(_)));
}

#[tokio::test]
async fn public_channels_and_bot_admins() {
    let w = world().await;
    let news = create(&w, true, "News").await;
    w.sandbox
        .call(
            as_account(w.alice.id),
            functions::channels::UpdateUsername {
                channel: input_channel(&w.sandbox, w.alice.id, news),
                username: "simula_news".into(),
            },
        )
        .await
        .unwrap();
    // Bob finds it by username and joins.
    let resolved = w
        .sandbox
        .call(
            as_account(w.bob.id),
            functions::contacts::ResolveUsername {
                username: "simula_news".into(),
                referer: None,
            },
        )
        .await
        .unwrap();
    let enums::contacts::ResolvedPeer::ResolvedPeer(resolved) = resolved;
    assert_eq!(Peer::from_tl(&resolved.peer), Peer::Channel(news));
    w.sandbox
        .call(
            as_account(w.bob.id),
            functions::channels::JoinChannel {
                channel: input_channel(&w.sandbox, w.bob.id, news),
            },
        )
        .await
        .unwrap();
    // Subscribers don't post.
    let err = say(&w, w.bob.id, news, "hi all").await.unwrap_err();
    assert_eq!(code(err), (403, "CHAT_ADMIN_REQUIRED".into()));
    // Bots join channels only as admins.
    let err = invite(&w, w.alice.id, news, &[w.bot.id]).await.unwrap_err();
    assert_eq!(code(err), (400, "USER_BOT".into()));
    w.sandbox
        .call(
            as_account(w.alice.id),
            functions::channels::EditAdmin {
                channel: input_channel(&w.sandbox, w.alice.id, news),
                user_id: input_user(&w.sandbox, w.alice.id, w.bot.id),
                admin_rights: types::ChatAdminRights {
                    post_messages: true,
                    edit_messages: true,
                    ..Default::default()
                }
                .into(),
                rank: None,
            },
        )
        .await
        .unwrap();
    // Posts carry no author, and reach the bot.
    let mut rx = w.sandbox.subscribe_updates();
    say(&w, w.alice.id, news, "first post").await.unwrap();
    let post = received(&mut rx, w.bot.id)
        .into_iter()
        .find_map(|u| match u {
            enums::Update::NewChannelMessage(u) => match u.message {
                enums::Message::Message(m) => Some(m),
                _ => None,
            },
            _ => None,
        })
        .expect("the post");
    assert!(post.post && post.from_id.is_none());
    // The bot posts too.
    say(&w, w.bot.id, news, "bot post").await.unwrap();
}
