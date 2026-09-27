//! A sandbox with a user (Alice) and a bot (Echo), and TL call helpers.

#![allow(dead_code, reason = "not every helper is used yet")]

use simula_core::services::accounts::{self, NewBot, NewUser};
use simula_core::{Account, Caller, Sandbox, SandboxConfig, UserId};
use simula_tl::{enums, functions};

pub fn as_account(id: UserId) -> Caller {
    Caller {
        account: id,
        session: None,
    }
}

pub fn send(
    peer: enums::InputPeer,
    text: &str,
    random_id: i64,
) -> functions::messages::SendMessage {
    functions::messages::SendMessage {
        peer,
        message: text.into(),
        random_id,
        ..Default::default()
    }
}

pub async fn setup() -> (std::sync::Arc<Sandbox>, Account, Account) {
    let sandbox = Sandbox::open(SandboxConfig {
        id: "test".into(),
        path: None,
    })
    .unwrap();
    let user = sandbox
        .admin(|ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: "Alice".into(),
                    username: Some("alice".into()),
                    lang_code: Some("en".into()),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    let bot = sandbox
        .admin(|ctx| {
            accounts::create_bot(
                ctx,
                NewBot {
                    first_name: "Echo".into(),
                    username: "echo_bot".into(),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    (sandbox, user, bot)
}
