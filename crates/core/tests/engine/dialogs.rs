//! What a client needs to show chats: the dialog list, history pages, reading, profiles
//! with bot commands, and `@username` lookup.

use simula_core::Sandbox;
use simula_tl::{enums, functions, types};

use crate::common::{as_account, send, setup};

async fn exchange(
    sandbox: &Sandbox,
    from: simula_core::UserId,
    to: simula_core::UserId,
    text: &str,
) {
    sandbox
        .call(
            as_account(from),
            send(sandbox.input_peer(from, to.into()), text, rand::random()),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn dialogs_history_and_reading() {
    let (sandbox, user, bot) = setup().await;
    exchange(&sandbox, user.id, bot.id, "/start").await;
    for i in 1..=4 {
        exchange(&sandbox, bot.id, user.id, &format!("answer {i}")).await;
    }

    let dialogs = sandbox
        .call(
            as_account(user.id),
            functions::messages::GetDialogs {
                offset_peer: enums::InputPeer::Empty,
                limit: 20,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let enums::messages::Dialogs::Dialogs(dialogs) = dialogs else {
        panic!("all dialogs fit in one page");
    };
    let enums::Dialog::Dialog(dialog) = &dialogs.dialogs[0] else {
        panic!("a dialog expected");
    };
    assert_eq!((dialog.top_message, dialog.unread_count), (5, 4));
    assert!(dialogs.users.iter().any(|u| u.id() == bot.id.0));

    // Pages, newest first.
    let page = |offset_id, add_offset, limit| functions::messages::GetHistory {
        peer: sandbox.input_peer(user.id, bot.id.into()),
        offset_id,
        add_offset,
        limit,
        ..Default::default()
    };
    let ids = |m: enums::messages::Messages| -> (Vec<i32>, Option<i32>) {
        match m {
            enums::messages::Messages::Slice(s) => {
                (s.messages.iter().map(|m| m.id()).collect(), Some(s.count))
            }
            enums::messages::Messages::Messages(m) => {
                (m.messages.iter().map(|m| m.id()).collect(), None)
            }
            _ => panic!("unexpected history"),
        }
    };
    let first = sandbox
        .call(as_account(user.id), page(0, 0, 2))
        .await
        .unwrap();
    assert_eq!(ids(first), (vec![5, 4], Some(5)));
    let older = sandbox
        .call(as_account(user.id), page(4, 0, 10))
        .await
        .unwrap();
    assert_eq!(ids(older), (vec![3, 2, 1], Some(5)));
    let newer = sandbox
        .call(as_account(user.id), page(2, -2, 2))
        .await
        .unwrap();
    assert_eq!(ids(newer).0, vec![3, 2]);

    // Reading: the reader's unread count drops, the bot learns its messages were read.
    let mut rx = sandbox.subscribe_updates();
    sandbox
        .call(
            as_account(user.id),
            functions::messages::ReadHistory {
                peer: sandbox.input_peer(user.id, bot.id.into()),
                max_id: 0,
            },
        )
        .await
        .unwrap();
    let mut outbox = None;
    while outbox.is_none() {
        let batch = rx.recv().await.unwrap();
        if let enums::Updates::Updates(u) = &batch.updates
            && let Some(enums::Update::ReadHistoryOutbox(read)) = u.updates.first()
        {
            assert_eq!(batch.account, bot.id);
            outbox = Some(read.max_id);
        }
    }
    assert_eq!(
        outbox,
        Some(5),
        "the bot's own ids: 1 is /start, 2-5 its answers"
    );
}

#[tokio::test]
async fn bot_profiles_and_usernames() {
    let (sandbox, user, bot) = setup().await;
    exchange(&sandbox, user.id, bot.id, "/start").await;
    let command = |name: &str| -> enums::BotCommand {
        types::BotCommand {
            ephemeral: false,
            command: name.into(),
            description: "d".into(),
        }
        .into()
    };
    let set =
        |scope: enums::BotCommandScope, lang: &str, name: &str| functions::bots::SetBotCommands {
            scope,
            lang_code: lang.into(),
            commands: vec![command(name)],
        };
    sandbox
        .call(
            as_account(bot.id),
            set(enums::BotCommandScope::Default, "", "general"),
        )
        .await
        .unwrap();
    sandbox
        .call(
            as_account(bot.id),
            set(enums::BotCommandScope::Users, "en", "english"),
        )
        .await
        .unwrap();

    let resolved = sandbox
        .call(
            as_account(user.id),
            functions::contacts::ResolveUsername {
                username: "@echo_bot".into(),
                referer: None,
            },
        )
        .await
        .unwrap();
    let enums::contacts::ResolvedPeer::ResolvedPeer(resolved) = resolved;
    let enums::User::User(found) = &resolved.users[0] else {
        panic!("user expected");
    };
    let input: enums::InputUser = types::InputUser {
        user_id: found.id,
        access_hash: found.access_hash.unwrap(),
    }
    .into();
    let full = sandbox
        .call(
            as_account(user.id),
            functions::users::GetFullUser { id: input },
        )
        .await
        .unwrap();
    let enums::users::UserFull::UserFull(full) = full;
    let enums::UserFull::UserFull(full_user) = &full.full_user;
    let enums::BotInfo::BotInfo(info) = full_user.bot_info.as_ref().unwrap();
    let names: Vec<_> = info
        .commands
        .iter()
        .flatten()
        .map(|enums::BotCommand::BotCommand(c)| c.command.clone())
        .collect();
    assert_eq!(
        names,
        ["english"],
        "Alice speaks English: private-chat scope in her language"
    );

    let missing = sandbox
        .call(
            as_account(user.id),
            functions::contacts::ResolveUsername {
                username: "nobody_here".into(),
                referer: None,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(missing.message, "USERNAME_NOT_OCCUPIED");
}

#[tokio::test]
async fn deep_links_start_the_bot() {
    let (sandbox, user, bot) = setup().await;
    let start = |param: &str| functions::messages::StartBot {
        bot: types::InputUser {
            user_id: bot.id.0,
            access_hash: match sandbox.input_peer(user.id, bot.id.into()) {
                enums::InputPeer::User(p) => p.access_hash,
                _ => unreachable!(),
            },
        }
        .into(),
        peer: sandbox.input_peer(user.id, bot.id.into()),
        random_id: rand::random(),
        start_param: param.into(),
    };
    let updates = sandbox
        .call(as_account(user.id), start("ref_42"))
        .await
        .unwrap();
    let enums::Updates::Updates(updates) = updates else {
        panic!("updates expected");
    };
    let sent = updates.updates.iter().find_map(|u| match u {
        enums::Update::NewMessage(u) => match &u.message {
            enums::Message::Message(m) => Some(m.message.clone()),
            _ => None,
        },
        _ => None,
    });
    assert_eq!(sent.as_deref(), Some("/start ref_42"));
    let bad = sandbox
        .call(as_account(user.id), start("no spaces"))
        .await
        .unwrap_err();
    assert_eq!(bad.message, "START_PARAM_INVALID");
}
