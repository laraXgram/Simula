//! Pinned messages: both sides of a private chat, "only for me", the notice that
//! replies to the pinned message, unpinning everything, and rights in channels.

use simula_core::Peer;
use simula_tl::{enums, functions, types};

use crate::common::{as_account, send, setup};

#[tokio::test]
async fn pinning_in_a_private_chat() {
    let (sandbox, alice, bot) = setup().await;
    let peer = sandbox.input_peer(alice.id, Peer::User(bot.id));
    sandbox
        .call(as_account(alice.id), send(peer.clone(), "pin me", 1))
        .await
        .unwrap();
    let mut rx = sandbox.subscribe_updates();
    sandbox
        .call(
            as_account(alice.id),
            functions::messages::UpdatePinnedMessage {
                peer: peer.clone(),
                id: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    // The bot's copy is pinned too, and it gets the notice replying to it.
    let mut pinned = false;
    let mut notice = None;
    while let Ok(batch) = rx.try_recv() {
        if batch.account != bot.id {
            continue;
        }
        let enums::Updates::Updates(u) = &batch.updates else {
            continue;
        };
        for update in &u.updates {
            match update {
                enums::Update::PinnedMessages(p) => pinned = p.pinned && p.messages == [1],
                enums::Update::NewMessage(m) => {
                    if let enums::Message::Service(s) = &m.message {
                        notice = Some(s.clone());
                    }
                }
                _ => {}
            }
        }
    }
    assert!(pinned);
    let notice = notice.expect("pin notice");
    assert!(matches!(notice.action, enums::MessageAction::PinMessage));
    assert!(matches!(
        notice.reply_to,
        Some(enums::MessageReplyHeader::MessageReplyHeader(ref h)) if h.reply_to_msg_id == Some(1)
    ));
    let full = sandbox
        .call(
            as_account(bot.id),
            functions::users::GetFullUser {
                id: types::InputUser {
                    user_id: alice.id.0,
                    access_hash: 0,
                }
                .into(),
            },
        )
        .await
        .unwrap();
    let enums::users::UserFull::UserFull(full) = full;
    let enums::UserFull::UserFull(info) = full.full_user;
    assert_eq!(info.pinned_msg_id, Some(1));

    let enums::messages::AffectedHistory::AffectedHistory(affected) = sandbox
        .call(
            as_account(alice.id),
            functions::messages::UnpinAllMessages {
                peer,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(affected.pts_count, 1);
}

#[tokio::test]
async fn pinning_in_a_supergroup_takes_the_right() {
    let (sandbox, alice, bot) = setup().await;
    let created = sandbox
        .call(
            as_account(alice.id),
            functions::channels::CreateChannel {
                megagroup: true,
                title: "Dev".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::Updates::Updates(created) = created else {
        panic!("updates expected");
    };
    let Some(enums::Chat::Channel(channel)) = created.chats.first() else {
        panic!("channel expected");
    };
    let peer = Peer::Channel(simula_core::ChannelId(channel.id));
    let enums::InputPeer::Channel(input) = sandbox.input_peer(alice.id, peer) else {
        unreachable!()
    };
    let enums::InputPeer::User(bot_peer) = sandbox.input_peer(alice.id, bot.id.into()) else {
        unreachable!()
    };
    sandbox
        .call(
            as_account(alice.id),
            functions::channels::InviteToChannel {
                channel: types::InputChannel {
                    channel_id: input.channel_id,
                    access_hash: input.access_hash,
                }
                .into(),
                users: vec![
                    types::InputUser {
                        user_id: bot_peer.user_id,
                        access_hash: bot_peer.access_hash,
                    }
                    .into(),
                ],
            },
        )
        .await
        .unwrap();
    sandbox
        .call(
            as_account(alice.id),
            send(sandbox.input_peer(alice.id, peer), "rules", 2),
        )
        .await
        .unwrap();
    let pin = |id: i32| functions::messages::UpdatePinnedMessage {
        peer: sandbox.input_peer(bot.id, peer),
        id,
        ..Default::default()
    };
    // New supergroups don't let members pin.
    let err = sandbox.call(as_account(bot.id), pin(3)).await.unwrap_err();
    assert_eq!(err.message, "CHAT_ADMIN_REQUIRED");
    let pin = functions::messages::UpdatePinnedMessage {
        peer: sandbox.input_peer(alice.id, peer),
        id: 3,
        ..Default::default()
    };
    let updates = sandbox.call(as_account(alice.id), pin).await.unwrap();
    let enums::Updates::Updates(updates) = updates else {
        panic!("updates expected");
    };
    assert!(
        updates
            .updates
            .iter()
            .any(|u| matches!(u, enums::Update::PinnedChannelMessages(p) if p.messages == [3]))
    );
}
