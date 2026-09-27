//! A user and a bot talk in a private chat, at the MTProto level.

mod common;

use common::{as_account, send, setup};
use simula_core::services::accounts::{self, NewBot};
use simula_core::{AccountUpdates, UserId};
use simula_tl::{enums, functions, types};
use tokio::sync::broadcast::Receiver;

/// The single `updateNewMessage` of the next batch delivered to `account`.
async fn next_message(
    rx: &mut Receiver<std::sync::Arc<AccountUpdates>>,
    account: UserId,
) -> (types::Message, i32, Vec<enums::User>) {
    loop {
        let batch = rx.recv().await.unwrap();
        if batch.account != account {
            continue;
        }
        let enums::Updates::Updates(updates) = &batch.updates else {
            panic!("expected updates")
        };
        let [enums::Update::NewMessage(new)] = updates.updates.as_slice() else {
            panic!("expected one updateNewMessage")
        };
        let enums::Message::Message(message) = &new.message else {
            panic!("expected a message")
        };
        return ((**message).clone(), new.pts, updates.users.clone());
    }
}

#[tokio::test]
async fn user_and_bot_exchange_private_messages() {
    let (sandbox, user, bot) = setup().await;
    let mut rx = sandbox.subscribe_updates();

    // A bot can't start a conversation (tdlib turns this into "Bot can't initiate conversation").
    let err = sandbox
        .call(
            as_account(bot.id),
            send(sandbox.input_peer(bot.id, user.id.into()), "hi", 1),
        )
        .await
        .unwrap_err();
    assert_eq!((err.code, err.message.as_str()), (400, "PEER_ID_INVALID"));

    // The user writes first.
    let result = sandbox
        .call(
            as_account(user.id),
            send(sandbox.input_peer(user.id, bot.id.into()), "/start", 42),
        )
        .await
        .unwrap();
    let enums::Updates::Updates(result) = result else {
        panic!("expected updates")
    };
    assert!(
        matches!(&result.updates[0], enums::Update::MessageID(u) if u.id == 1 && u.random_id == 42)
    );

    // The bot gets its own copy: id 1 in its box, incoming, from the user.
    let (message, pts, users) = next_message(&mut rx, bot.id).await;
    assert_eq!(
        (message.id, message.out, message.message.as_str(), pts),
        (1, false, "/start", 1)
    );
    assert_eq!(
        message.peer_id,
        types::PeerUser { user_id: user.id.0 }.into()
    );
    let enums::User::User(seen) = users
        .iter()
        .find(|u| matches!(u, enums::User::User(x) if x.id == user.id.0))
        .unwrap()
    else {
        unreachable!()
    };
    assert_eq!(
        seen.lang_code.as_deref(),
        Some("en"),
        "bots see users' language"
    );
    let enums::InputPeer::User(addr) = sandbox.input_peer(bot.id, user.id.into()) else {
        unreachable!()
    };
    assert_eq!(
        seen.access_hash,
        Some(addr.access_hash),
        "access hash is the bot's own"
    );

    // The bot replies: it's message 2 in the user's box, the user's pts moves to 2.
    sandbox
        .call(
            as_account(bot.id),
            send(
                sandbox.input_peer(bot.id, user.id.into()),
                "Hello, Alice!",
                7,
            ),
        )
        .await
        .unwrap();
    let (reply, pts, _) = next_message(&mut rx, user.id).await;
    assert_eq!(
        (reply.id, reply.out, reply.message.as_str(), pts),
        (2, false, "Hello, Alice!", 2)
    );

    let state = sandbox
        .call(as_account(user.id), functions::updates::GetState {})
        .await
        .unwrap();
    let enums::updates::State::State(state) = state;
    assert_eq!(state.pts, 2);
}

#[tokio::test]
async fn forged_access_hash_is_rejected() {
    let (sandbox, user, bot) = setup().await;
    let forged: enums::InputPeer = types::InputPeerUser {
        user_id: bot.id.0,
        access_hash: 12345,
    }
    .into();
    let err = sandbox
        .call(as_account(user.id), send(forged, "hi", 1))
        .await
        .unwrap_err();
    assert_eq!(err.message, "PEER_ID_INVALID");
}

#[tokio::test]
async fn unimplemented_functions_say_so() {
    let (sandbox, user, _) = setup().await;
    let err = sandbox
        .call(as_account(user.id), functions::help::GetConfig {})
        .await
        .unwrap_err();
    assert_eq!(
        (err.code, err.message.as_str()),
        (501, "NOT_IMPLEMENTED_IN_SIMULA: help.getConfig")
    );
}

#[tokio::test]
async fn bot_tokens_fix_the_bot_id() {
    let (sandbox, ..) = setup().await;
    let bot = sandbox
        .admin(|ctx| {
            accounts::create_bot(
                ctx,
                NewBot {
                    first_name: "Mine".into(),
                    username: "mine_bot".into(),
                    token: Some("123456789:AAHdqTcvCH1vGWJxfSeofSAs0K5PALDsaw".into()),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    assert_eq!(bot.id, UserId(123456789));
}
