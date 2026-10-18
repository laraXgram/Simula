//! Invite links and join requests from the bot's side: creating links, a request the
//! bot hears about and may answer in private, approving and declining, primary links
//! and revoking.

use serde_json::json;
use simula_core::Peer;
use simula_tl::{enums, functions, types};

use crate::support::{
    as_user, bob, create_channel, fails, input_channel, input_user, next_updates, ok, world,
};

#[tokio::test]
async fn join_requests_and_links() {
    let w = world().await;
    let bob = bob(&w).await;
    let mut offset = 0;
    let dev = create_channel(&w, false, "Dev").await;
    let chat_id = Peer::Channel(dev).bot_api_id();
    as_user(
        &w,
        w.user.id,
        functions::channels::EditAdmin {
            channel: input_channel(&w, w.user.id, dev),
            user_id: input_user(&w, w.user.id, w.bot_id),
            admin_rights: types::ChatAdminRights {
                invite_users: true,
                other: true,
                ..Default::default()
            }
            .into(),
            rank: None,
        },
    )
    .await
    .unwrap();
    next_updates(&w, &mut offset).await;

    assert_eq!(
        fails(
            &w,
            "createChatInviteLink",
            json!({ "chat_id": chat_id, "member_limit": 5, "creates_join_request": true })
        )
        .await,
        "Bad Request: member limit can't be specified for links requiring administrator approval"
    );
    let link = ok(
        &w,
        "createChatInviteLink",
        json!({ "chat_id": chat_id, "name": "beta", "creates_join_request": true }),
    )
    .await;
    assert_eq!(link["creator"]["id"], w.bot_id.0);
    assert_eq!(link["name"], "beta");
    assert_eq!(link["is_primary"], false);
    let url = link["invite_link"].as_str().unwrap().to_owned();
    assert!(url.starts_with("https://t.me/+"));
    let hash = url.trim_start_matches("https://t.me/+").to_owned();

    // Bob sees what the link leads to, and asks to join.
    let invite = as_user(
        &w,
        bob.id,
        functions::messages::CheckChatInvite { hash: hash.clone() },
    )
    .await
    .unwrap();
    assert!(
        matches!(invite, enums::ChatInvite::ChatInvite(ref i) if i.request_needed && i.megagroup)
    );
    let err = as_user(
        &w,
        bob.id,
        functions::messages::ImportChatInvite { hash: hash.clone() },
    )
    .await
    .unwrap_err();
    assert_eq!(err.message, "INVITE_REQUEST_SENT");
    let updates = next_updates(&w, &mut offset).await;
    let request = &updates[0]["chat_join_request"];
    assert_eq!(request["from"]["id"], bob.id.0);
    assert_eq!(request["user_chat_id"], bob.id.0);
    assert_eq!(request["invite_link"]["name"], "beta");

    // The bot may write to the requester first, and approves.
    ok(
        &w,
        "sendMessage",
        json!({ "chat_id": bob.id.0, "text": "Why do you want to join?" }),
    )
    .await;
    ok(
        &w,
        "approveChatJoinRequest",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    let member = ok(
        &w,
        "getChatMember",
        json!({ "chat_id": chat_id, "user_id": bob.id.0 }),
    )
    .await;
    assert_eq!(member["status"], "member");
    assert_eq!(
        fails(
            &w,
            "declineChatJoinRequest",
            json!({ "chat_id": chat_id, "user_id": bob.id.0 })
        )
        .await,
        "Bad Request: HIDE_REQUESTER_MISSING"
    );

    // Primary links: exporting replaces the previous one, which stops working.
    let first = ok(&w, "exportChatInviteLink", json!({ "chat_id": chat_id })).await;
    let chat = ok(&w, "getChat", json!({ "chat_id": chat_id })).await;
    assert_eq!(chat["invite_link"], first);
    let second = ok(&w, "exportChatInviteLink", json!({ "chat_id": chat_id })).await;
    assert_ne!(first, second);
    let carol = crate::support::bob(&w).await;
    let err = as_user(
        &w,
        carol.id,
        functions::messages::ImportChatInvite {
            hash: first
                .as_str()
                .unwrap()
                .trim_start_matches("https://t.me/+")
                .into(),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.message, "INVITE_HASH_EXPIRED");
    // The current one works right away.
    as_user(
        &w,
        carol.id,
        functions::messages::ImportChatInvite {
            hash: second
                .as_str()
                .unwrap()
                .trim_start_matches("https://t.me/+")
                .into(),
        },
    )
    .await
    .unwrap();

    let revoked = ok(
        &w,
        "revokeChatInviteLink",
        json!({ "chat_id": chat_id, "invite_link": url }),
    )
    .await;
    assert_eq!(revoked["is_revoked"], true);
}
