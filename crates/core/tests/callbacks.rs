//! Callback buttons at the MTProto level: the presser's call waits for the bot's answer.

mod common;

use std::time::Duration;

use common::{as_account, send, setup};
use simula_core::{Account, Sandbox};
use simula_tl::{enums, functions, types};

/// Alice writes, the bot answers with a "Go" callback button (id 2 in both boxes).
async fn with_button() -> (std::sync::Arc<Sandbox>, Account, Account) {
    let (sandbox, user, bot) = setup().await;
    sandbox
        .call(
            as_account(user.id),
            send(sandbox.input_peer(user.id, bot.id.into()), "/start", 1),
        )
        .await
        .unwrap();
    let button = types::KeyboardInlineButton {
        style: None,
        text: "Go".into(),
        r#type: types::InlineButtonTypeCallback {
            requires_password: false,
            data: b"go".to_vec(),
        }
        .into(),
    };
    let markup = types::ReplyInlineMarkup {
        force_reply: false,
        rows: vec![
            types::KeyboardInlineButtonRow {
                buttons: vec![button.into()],
            }
            .into(),
        ],
    };
    sandbox
        .call(
            as_account(bot.id),
            functions::messages::SendMessage {
                reply_markup: Some(markup.into()),
                ..send(sandbox.input_peer(bot.id, user.id.into()), "Pick", 2)
            },
        )
        .await
        .unwrap();
    (sandbox, user, bot)
}

fn press(
    sandbox: &Sandbox,
    user: &Account,
    bot: &Account,
    data: &[u8],
) -> functions::messages::GetBotCallbackAnswer {
    functions::messages::GetBotCallbackAnswer {
        peer: sandbox.input_peer(user.id, bot.id.into()),
        msg_id: 2,
        data: Some(data.to_vec()),
        ..Default::default()
    }
}

fn answer(query_id: i64, text: &str) -> functions::messages::SetBotCallbackAnswer {
    functions::messages::SetBotCallbackAnswer {
        alert: true,
        query_id,
        message: Some(text.into()),
        ..Default::default()
    }
}

#[tokio::test]
async fn the_press_returns_the_bots_answer() {
    let (sandbox, user, bot) = with_button().await;
    let mut rx = sandbox.subscribe_updates();
    let pressing = {
        let sandbox = sandbox.clone();
        let call = press(&sandbox, &user, &bot, b"go");
        tokio::spawn(async move { sandbox.call(as_account(user.id), call).await })
    };

    let query = loop {
        let batch = rx.recv().await.unwrap();
        let enums::Updates::Updates(updates) = &batch.updates else {
            continue;
        };
        if let Some(enums::Update::BotCallbackQuery(query)) = updates.updates.first() {
            assert_eq!(batch.account, bot.id);
            break (**query).clone();
        }
    };
    assert_eq!((query.user_id, query.msg_id), (user.id.0, 2));
    assert_eq!(query.data.as_deref(), Some(&b"go"[..]));

    sandbox
        .call(as_account(bot.id), answer(query.query_id, "Done!"))
        .await
        .unwrap();
    let enums::messages::BotCallbackAnswer::BotCallbackAnswer(got) =
        pressing.await.unwrap().unwrap();
    assert!(got.alert);
    assert_eq!(got.message.as_deref(), Some("Done!"));

    let again = sandbox
        .call(as_account(bot.id), answer(query.query_id, "again"))
        .await
        .unwrap_err();
    assert_eq!(again.message, "QUERY_ID_INVALID");
}

#[tokio::test]
async fn unanswered_presses_time_out() {
    let (sandbox, user, bot) = with_button().await;
    sandbox.set_answer_timeout(Duration::from_millis(50));
    let err = sandbox
        .call(as_account(user.id), press(&sandbox, &user, &bot, b"go"))
        .await
        .unwrap_err();
    assert_eq!(err.message, "BOT_RESPONSE_TIMEOUT");

    let wrong = sandbox
        .call(as_account(user.id), press(&sandbox, &user, &bot, b"nope"))
        .await
        .unwrap_err();
    assert_eq!(wrong.message, "DATA_INVALID");
}
