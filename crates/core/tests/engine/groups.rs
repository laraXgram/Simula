//! Basic groups at the MTProto level: creation, per-member boxes, privacy mode for bots,
//! membership updates for bots, reading, permissions.

use std::sync::Arc;

use simula_core::services::accounts::{self, NewUser};
use simula_core::{Account, AccountUpdates, Sandbox, UserId};
use simula_tl::{enums, functions, types};
use tokio::sync::broadcast::Receiver;

use crate::common::{as_account, setup};

struct Group {
    sandbox: Arc<Sandbox>,
    alice: Account,
    bob: Account,
    bot: Account,
    chat: i64,
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

/// Alice creates "Team" with Bob and the bot (privacy mode on).
async fn group() -> Group {
    let (sandbox, alice, bot) = setup().await;
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
    let created = sandbox
        .call(
            as_account(alice.id),
            functions::messages::CreateChat {
                users: vec![
                    input_user(&sandbox, alice.id, bob.id),
                    input_user(&sandbox, alice.id, bot.id),
                ],
                title: "Team".into(),
                ttl_period: None,
            },
        )
        .await
        .unwrap();
    let enums::messages::InvitedUsers::InvitedUsers(created) = created;
    let enums::Updates::Updates(updates) = created.updates else {
        panic!("updates expected");
    };
    let chat = updates.chats.iter().find_map(|c| match c {
        enums::Chat::Chat(c) => Some(c.id),
        _ => None,
    });
    Group {
        sandbox,
        alice,
        bob,
        bot,
        chat: chat.expect("the new chat"),
    }
}

fn peer(chat: i64) -> enums::InputPeer {
    types::InputPeerChat { chat_id: chat }.into()
}

async fn say(g: &Group, from: UserId, text: &str) -> enums::Updates {
    g.sandbox
        .call(
            as_account(from),
            functions::messages::SendMessage {
                peer: peer(g.chat),
                message: text.into(),
                random_id: rand::random(),
                ..Default::default()
            },
        )
        .await
        .unwrap()
}

/// Texts of the new messages `account` received in the batches so far.
fn received(rx: &mut Receiver<Arc<AccountUpdates>>, account: UserId) -> Vec<String> {
    let mut texts = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        if batch.account != account {
            continue;
        }
        let enums::Updates::Updates(updates) = &batch.updates else {
            continue;
        };
        for update in &updates.updates {
            if let enums::Update::NewMessage(u) = update
                && let enums::Message::Message(m) = &u.message
            {
                texts.push(m.message.clone());
            }
        }
    }
    texts
}

#[tokio::test]
async fn members_bots_and_privacy_mode() {
    let g = group().await;
    let full = g
        .sandbox
        .call(
            as_account(g.alice.id),
            functions::messages::GetFullChat { chat_id: g.chat },
        )
        .await
        .unwrap();
    let enums::messages::ChatFull::ChatFull(full) = full;
    let enums::ChatFull::ChatFull(full) = full.full_chat else {
        panic!("basic group expected");
    };
    let enums::ChatParticipants::ChatParticipants(participants) = full.participants else {
        panic!("participants expected");
    };
    assert_eq!(participants.participants.len(), 3);

    let mut rx = g.sandbox.subscribe_updates();
    say(&g, g.alice.id, "small talk").await;
    say(&g, g.alice.id, "/start").await;
    say(&g, g.bob.id, "/help@echo_bot").await;
    say(&g, g.bob.id, "/help@other_bot").await;
    // Every member gets everything (their own messages included)…
    assert_eq!(
        received(&mut rx, g.bob.id),
        ["small talk", "/start", "/help@echo_bot", "/help@other_bot"]
    );
    // …but a bot in privacy mode only commands for it (the only bot, so general ones too).
    let mut rx = g.sandbox.subscribe_updates();
    say(&g, g.alice.id, "small talk").await;
    say(&g, g.alice.id, "/start").await;
    say(&g, g.bob.id, "/help@echo_bot").await;
    say(&g, g.bob.id, "/help@other_bot").await;
    assert_eq!(received(&mut rx, g.bot.id), ["/start", "/help@echo_bot"]);
}

#[tokio::test]
async fn bots_learn_about_their_membership() {
    let g = group().await;
    let mut rx = g.sandbox.subscribe_updates();
    g.sandbox
        .call(
            as_account(g.alice.id),
            functions::messages::DeleteChatUser {
                revoke_history: false,
                chat_id: g.chat,
                user_id: input_user(&g.sandbox, g.alice.id, g.bot.id),
            },
        )
        .await
        .unwrap();
    let mut change = None;
    while let Ok(batch) = rx.try_recv() {
        if batch.account != g.bot.id {
            continue;
        }
        if let enums::Updates::Updates(u) = &batch.updates {
            for update in &u.updates {
                if let enums::Update::ChatParticipant(p) = update {
                    change = Some((**p).clone());
                }
            }
        }
    }
    let change = change.expect("updateChatParticipant for the bot");
    assert_eq!(
        (change.user_id, change.actor_id),
        (g.bot.id.0, g.alice.id.0)
    );
    assert!(change.prev_participant.is_some() && change.new_participant.is_none());

    let err = g
        .sandbox
        .call(
            as_account(g.bot.id),
            functions::messages::SendMessage {
                peer: peer(g.chat),
                message: "still here?".into(),
                random_id: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        (err.code, err.message.as_str()),
        (403, "CHAT_WRITE_FORBIDDEN")
    );
}

#[tokio::test]
async fn reading_and_permissions() {
    let g = group().await;
    say(&g, g.alice.id, "read me").await;
    let mut rx = g.sandbox.subscribe_updates();
    g.sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::ReadHistory {
                peer: peer(g.chat),
                max_id: 0,
            },
        )
        .await
        .unwrap();
    let mut read = false;
    while let Ok(batch) = rx.try_recv() {
        if batch.account == g.alice.id
            && let enums::Updates::Updates(u) = &batch.updates
            && u.updates
                .iter()
                .any(|u| matches!(u, enums::Update::ReadHistoryOutbox(_)))
        {
            read = true;
        }
    }
    assert!(read, "Alice sees Bob read her message");

    // Bob can't delete Alice's message for everyone; Alice (the creator) can.
    let history = g
        .sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::GetHistory {
                peer: peer(g.chat),
                limit: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let id = match history {
        enums::messages::Messages::Messages(m) => m.messages[0].id(),
        enums::messages::Messages::Slice(m) => m.messages[0].id(),
        _ => panic!("history expected"),
    };
    let err = g
        .sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::DeleteMessages {
                revoke: true,
                id: vec![id],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "MESSAGE_DELETE_FORBIDDEN");
}

#[tokio::test]
async fn bot_commands_for_the_group() {
    let g = group().await;
    let command = |name: &str| -> enums::BotCommand {
        types::BotCommand {
            command: name.into(),
            description: format!("{name} it"),
            ..Default::default()
        }
        .into()
    };
    let set = |scope: enums::BotCommandScope, name: &'static str| {
        let sandbox = g.sandbox.clone();
        let bot = g.bot.id;
        let commands = vec![command(name)];
        async move {
            sandbox
                .call(
                    as_account(bot),
                    functions::bots::SetBotCommands {
                        scope,
                        lang_code: String::new(),
                        commands,
                    },
                )
                .await
                .unwrap();
        }
    };
    set(enums::BotCommandScope::Default, "everywhere").await;
    set(enums::BotCommandScope::Chats, "groups").await;
    set(enums::BotCommandScope::ChatAdmins, "admins").await;
    let commands = |viewer: UserId| {
        let sandbox = g.sandbox.clone();
        let chat = g.chat;
        let bot = g.bot.id;
        async move {
            let enums::messages::ChatFull::ChatFull(full) = sandbox
                .call(
                    as_account(viewer),
                    functions::messages::GetFullChat { chat_id: chat },
                )
                .await
                .unwrap();
            let enums::ChatFull::ChatFull(full) = full.full_chat else {
                panic!("basic group expected");
            };
            let info = full.bot_info.unwrap_or_default();
            assert_eq!(info.len(), 1, "one bot in the group");
            let enums::BotInfo::BotInfo(info) = &info[0];
            assert_eq!(info.user_id, Some(bot.0));
            info.commands
                .clone()
                .unwrap_or_default()
                .into_iter()
                .map(|enums::BotCommand::BotCommand(c)| c.command)
                .collect::<Vec<_>>()
        }
    };
    // The creator is an admin: all chat admins come before all group chats.
    assert_eq!(commands(g.alice.id).await, ["admins"]);
    assert_eq!(commands(g.bob.id).await, ["groups"]);
    // A list for this chat beats the general ones.
    set(
        types::BotCommandScopePeer { peer: peer(g.chat) }.into(),
        "here",
    )
    .await;
    assert_eq!(commands(g.bob.id).await, ["here"]);
}

/// Every update `account` received in the batches so far.
fn all_updates(rx: &mut Receiver<Arc<AccountUpdates>>, account: UserId) -> Vec<enums::Update> {
    let mut out = Vec::new();
    while let Ok(batch) = rx.try_recv() {
        if batch.account != account {
            continue;
        }
        if let enums::Updates::Updates(updates) = &batch.updates {
            out.extend(updates.updates.iter().cloned());
        }
    }
    out
}

#[tokio::test]
async fn upgrading_to_a_supergroup() {
    let g = group().await;
    say(&g, g.alice.id, "before the upgrade").await;
    let migrate = functions::messages::MigrateChat { chat_id: g.chat };
    let err = g
        .sandbox
        .call(as_account(g.bob.id), migrate.clone())
        .await
        .unwrap_err();
    assert_eq!(err.message, "CHAT_ADMIN_REQUIRED");

    let mut rx = g.sandbox.subscribe_updates();
    let enums::Updates::Updates(result) = g
        .sandbox
        .call(as_account(g.alice.id), migrate.clone())
        .await
        .unwrap()
    else {
        panic!("updates expected");
    };
    let channel = result
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Channel(c) => Some(c.clone()),
            _ => None,
        })
        .expect("the supergroup");
    assert!(channel.megagroup && channel.creator);
    assert_eq!(channel.title, "Team");
    assert_eq!(channel.participants_count, Some(3));
    let old = result
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Chat(c) => Some(c.clone()),
            _ => None,
        })
        .expect("the old group");
    assert!(old.deactivated);
    let Some(enums::InputChannel::InputChannel(to)) = &old.migrated_to else {
        panic!("migrated_to expected");
    };
    assert_eq!(to.channel_id, channel.id);
    assert_eq!(to.access_hash, channel.access_hash.unwrap());

    // The bot (privacy mode on) gets both service messages: the group's last, then the
    // supergroup's first.
    let bot_updates = all_updates(&mut rx, g.bot.id);
    let actions: Vec<_> = bot_updates
        .iter()
        .filter_map(|u| match u {
            enums::Update::NewMessage(u) => Some(&u.message),
            enums::Update::NewChannelMessage(u) => Some(&u.message),
            _ => None,
        })
        .filter_map(|m| match m {
            enums::Message::Service(s) => Some((s.id, s.action.clone())),
            _ => None,
        })
        .collect();
    assert!(matches!(
        &actions[..],
        [
            (_, enums::MessageAction::ChatMigrateTo(to)),
            (1, enums::MessageAction::ChannelMigrateFrom(from)),
        ] if to.channel_id == channel.id && from.chat_id == g.chat && from.title == "Team"
    ));

    // The group takes no more messages, nor a second upgrade.
    let err = g
        .sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::SendMessage {
                peer: peer(g.chat),
                message: "late".into(),
                random_id: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "CHAT_ID_INVALID");
    let err = g
        .sandbox
        .call(as_account(g.alice.id), migrate)
        .await
        .unwrap_err();
    assert_eq!(err.message, "CHAT_ID_INVALID");

    // Bob is a member of the supergroup and sees where it came from.
    let input = g.sandbox.input_peer(
        g.bob.id,
        simula_core::Peer::Channel(simula_core::ChannelId(channel.id)),
    );
    let enums::InputPeer::Channel(input) = input else {
        unreachable!()
    };
    let enums::messages::ChatFull::ChatFull(full) = g
        .sandbox
        .call(
            as_account(g.bob.id),
            functions::channels::GetFullChannel {
                channel: types::InputChannel {
                    channel_id: input.channel_id,
                    access_hash: input.access_hash,
                }
                .into(),
            },
        )
        .await
        .unwrap();
    let enums::ChatFull::ChannelFull(full_channel) = &full.full_chat else {
        panic!("channelFull expected");
    };
    assert_eq!(full_channel.migrated_from_chat_id, Some(g.chat));
    assert!(full_channel.migrated_from_max_id.unwrap() > 1);
    assert!(
        full.chats
            .iter()
            .any(|c| matches!(c, enums::Chat::Chat(c) if c.id == g.chat))
    );
    g.sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::SendMessage {
                peer: enums::InputPeer::Channel(input.clone()),
                message: "after the upgrade".into(),
                random_id: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    // The old history stays readable.
    let enums::messages::Messages::Messages(history) = g
        .sandbox
        .call(
            as_account(g.bob.id),
            functions::messages::GetHistory {
                peer: peer(g.chat),
                limit: 10,
                ..Default::default()
            },
        )
        .await
        .unwrap()
    else {
        panic!("all messages expected");
    };
    assert!(
        history
            .messages
            .iter()
            .any(|m| matches!(m, enums::Message::Message(m) if m.message == "before the upgrade"))
    );
}
