//! Editing and deleting private messages at the MTProto level: every copy changes, each
//! owner gets its own update and pts.

mod common;

use common::{as_account, send, setup};
use simula_core::{Account, Sandbox, UserId};
use simula_tl::{enums, functions, types};

/// Alice writes "/start", the bot answers "v1" (id 2 in both boxes).
async fn conversation() -> (std::sync::Arc<Sandbox>, Account, Account) {
    let (sandbox, user, bot) = setup().await;
    sandbox
        .call(
            as_account(user.id),
            send(sandbox.input_peer(user.id, bot.id.into()), "/start", 1),
        )
        .await
        .unwrap();
    sandbox
        .call(
            as_account(bot.id),
            send(sandbox.input_peer(bot.id, user.id.into()), "v1", 2),
        )
        .await
        .unwrap();
    (sandbox, user, bot)
}

fn edit(sandbox: &Sandbox, from: UserId, to: UserId, id: i32) -> functions::messages::EditMessage {
    functions::messages::EditMessage {
        peer: sandbox.input_peer(from, to.into()),
        id,
        message: Some("v2".into()),
        ..Default::default()
    }
}

async fn message(sandbox: &Sandbox, owner: UserId, id: i32) -> enums::Message {
    let found = sandbox
        .call(
            as_account(owner),
            functions::messages::GetMessages {
                id: vec![types::InputMessageID { id }.into()],
            },
        )
        .await
        .unwrap();
    let enums::messages::Messages::Messages(found) = found else {
        panic!("messages.messages expected");
    };
    found.messages[0].clone()
}

#[tokio::test]
async fn the_author_edits_every_copy() {
    let (sandbox, user, bot) = conversation().await;
    let mut rx = sandbox.subscribe_updates();

    sandbox
        .call(as_account(bot.id), edit(&sandbox, bot.id, user.id, 2))
        .await
        .unwrap();
    let batch = rx.recv().await.unwrap();
    let enums::Updates::Updates(updates) = &batch.updates else {
        panic!("updates expected");
    };
    let [enums::Update::EditMessage(update)] = updates.updates.as_slice() else {
        panic!("one updateEditMessage expected");
    };
    assert_eq!(update.pts, 3, "Alice: two new messages, then the edit");

    let enums::Message::Message(alice_copy) = message(&sandbox, user.id, 2).await else {
        panic!("message expected");
    };
    assert_eq!(alice_copy.message, "v2");
    assert!(alice_copy.edit_date.is_some() && !alice_copy.edit_hide);

    let again = sandbox
        .call(as_account(bot.id), edit(&sandbox, bot.id, user.id, 2))
        .await
        .unwrap_err();
    assert_eq!(again.message, "MESSAGE_NOT_MODIFIED");
    let not_author = sandbox
        .call(as_account(bot.id), edit(&sandbox, bot.id, user.id, 1))
        .await
        .unwrap_err();
    assert_eq!(
        (not_author.code, not_author.message.as_str()),
        (403, "MESSAGE_AUTHOR_REQUIRED")
    );
}

#[tokio::test]
async fn keyboard_only_edits_are_hidden() {
    let (sandbox, user, bot) = conversation().await;
    let keyboard: enums::ReplyMarkup = types::ReplyInlineMarkup {
        force_reply: false,
        rows: vec![
            types::KeyboardInlineButtonRow {
                buttons: vec![
                    types::KeyboardInlineButton {
                        style: None,
                        text: "Go".into(),
                        r#type: types::InlineButtonTypeCallback {
                            requires_password: false,
                            data: b"go".to_vec(),
                        }
                        .into(),
                    }
                    .into(),
                ],
            }
            .into(),
        ],
    }
    .into();
    sandbox
        .call(
            as_account(bot.id),
            functions::messages::EditMessage {
                reply_markup: Some(keyboard.clone()),
                message: None,
                ..edit(&sandbox, bot.id, user.id, 2)
            },
        )
        .await
        .unwrap();
    let enums::Message::Message(copy) = message(&sandbox, user.id, 2).await else {
        panic!("message expected");
    };
    assert_eq!(copy.message, "v1");
    assert_eq!(copy.reply_markup, Some(keyboard));
    assert!(copy.edit_hide);
}

#[tokio::test]
async fn deleting_for_everyone_or_just_yourself() {
    let (sandbox, user, bot) = conversation().await;

    // Alice deletes the bot's answer just for herself.
    let affected = sandbox
        .call(
            as_account(user.id),
            functions::messages::DeleteMessages {
                revoke: false,
                id: vec![2],
            },
        )
        .await
        .unwrap();
    let enums::messages::AffectedMessages::AffectedMessages(affected) = affected;
    assert_eq!((affected.pts, affected.pts_count), (3, 1));
    assert!(matches!(
        message(&sandbox, user.id, 2).await,
        enums::Message::Empty(_)
    ));
    assert!(matches!(
        message(&sandbox, bot.id, 2).await,
        enums::Message::Message(_)
    ));

    // The bot deletes Alice's "/start" for everyone (bots always revoke).
    sandbox
        .call(
            as_account(bot.id),
            functions::messages::DeleteMessages {
                revoke: true,
                id: vec![1, 999],
            },
        )
        .await
        .unwrap();
    assert!(matches!(
        message(&sandbox, bot.id, 1).await,
        enums::Message::Empty(_)
    ));
    assert!(matches!(
        message(&sandbox, user.id, 1).await,
        enums::Message::Empty(_)
    ));
}

#[tokio::test]
async fn bots_delete_only_recent_messages() {
    let (sandbox, _user, bot) = conversation().await;
    sandbox.clock().advance(49 * 3600 * 1000);
    let err = sandbox
        .call(
            as_account(bot.id),
            functions::messages::DeleteMessages {
                revoke: true,
                id: vec![2],
            },
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "MESSAGE_DELETE_FORBIDDEN");
}
