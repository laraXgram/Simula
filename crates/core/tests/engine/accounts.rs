//! Editing accounts (own profile over TL, or the Control API) and deleting them.

use simula_core::services::accounts::{self, AccountChange, NewUser};
use simula_tl::{enums, functions, types};

use crate::common::{as_account, send, setup};

#[tokio::test]
async fn profiles_change_and_accounts_go() {
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
    // Alice and the bot talk, so the bot knows her.
    sandbox
        .call(
            as_account(alice.id),
            send(sandbox.input_peer(alice.id, bot.id.into()), "hi", 1),
        )
        .await
        .unwrap();
    let mut rx = sandbox.subscribe_updates();
    let enums::User::User(me) = sandbox
        .call(
            as_account(alice.id),
            functions::account::UpdateProfile {
                first_name: Some("Alicia".into()),
                last_name: Some("Smith".into()),
                about: Some("tester".into()),
            },
        )
        .await
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(me.first_name.as_deref(), Some("Alicia"));
    let told = std::iter::from_fn(|| rx.try_recv().ok()).any(|batch| {
        batch.account == bot.id
            && matches!(&batch.updates, enums::Updates::Updates(u) if u.updates.iter().any(|u| matches!(u, enums::Update::UserName(n) if n.first_name == "Alicia")))
    });
    assert!(told, "the bot learns the new name");

    // Usernames: taken ones are refused, own ones checked as free.
    let taken = sandbox
        .call(
            as_account(alice.id),
            functions::account::UpdateUsername {
                username: "bobby".into(),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(taken.message, "USERNAME_OCCUPIED");
    assert!(
        !sandbox
            .call(
                as_account(alice.id),
                functions::account::CheckUsername {
                    username: "bobby".into()
                }
            )
            .await
            .unwrap()
    );
    sandbox
        .call(
            as_account(alice.id),
            functions::account::UpdateUsername {
                username: "alicia_s".into(),
            },
        )
        .await
        .unwrap();
    let edited = sandbox
        .admin(move |ctx| {
            accounts::update(
                ctx,
                bob.id,
                AccountChange {
                    first_name: Some("Robert".into()),
                    username: Some(String::new()),
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    assert_eq!(
        (edited.first_name.as_str(), edited.username),
        ("Robert", None)
    );

    // Deleting: the bot sees "Deleted Account" and can't write to it.
    let alice_id = alice.id;
    sandbox
        .admin(move |ctx| accounts::delete(ctx, alice_id))
        .await
        .unwrap();
    let users = sandbox
        .call(
            as_account(bot.id),
            functions::users::GetUsers {
                id: vec![
                    types::InputUser {
                        user_id: alice.id.0,
                        access_hash: 0,
                    }
                    .into(),
                ],
            },
        )
        .await
        .unwrap();
    assert!(matches!(&users[..], [enums::User::User(u)] if u.deleted && u.first_name.is_none()));
    let err = sandbox
        .call(
            as_account(bot.id),
            send(
                sandbox.input_peer(bot.id, alice.id.into()),
                "still there?",
                2,
            ),
        )
        .await
        .unwrap_err();
    assert_eq!(err.message, "INPUT_USER_DEACTIVATED");
}
