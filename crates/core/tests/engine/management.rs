//! What chat management screens use: invite links listed, revoked and deleted, who
//! joined or asked to join, admins in basic groups, deleting a supergroup.

use std::sync::Arc;

use simula_core::services::accounts::{self, NewUser};
use simula_core::{Account, ChannelId, Peer, RpcError, Sandbox, UserId};
use simula_tl::{enums, functions, types};

use crate::common::{as_account, setup};

async fn user(sandbox: &Sandbox, name: &str) -> Account {
    let name = name.to_owned();
    sandbox
        .admin(move |ctx| {
            accounts::create_user(
                ctx,
                NewUser {
                    first_name: name,
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap()
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

async fn call<F>(sandbox: &Sandbox, account: UserId, f: F) -> Result<F::Return, RpcError>
where
    F: simula_tl::RemoteCall + Into<simula_tl::Function>,
    F::Return: 'static,
{
    sandbox.call(as_account(account), f).await
}

async fn supergroup(sandbox: &Arc<Sandbox>, owner: UserId) -> ChannelId {
    let enums::Updates::Updates(created) = call(
        sandbox,
        owner,
        functions::channels::CreateChannel {
            megagroup: true,
            title: "Team".into(),
            about: String::new(),
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    created
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Channel(c) => Some(ChannelId(c.id)),
            _ => None,
        })
        .unwrap()
}

async fn new_link(
    sandbox: &Sandbox,
    owner: UserId,
    peer: enums::InputPeer,
    request: bool,
) -> String {
    let enums::ExportedChatInvite::ChatInviteExported(link) = call(
        sandbox,
        owner,
        functions::messages::ExportChatInvite {
            peer,
            request_needed: request,
            ..Default::default()
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    link.link
}

#[tokio::test]
async fn invite_links_and_importers() {
    let (sandbox, alice, _) = setup().await;
    let (bob, carol) = (user(&sandbox, "Bob").await, user(&sandbox, "Carol").await);
    let team = supergroup(&sandbox, alice.id).await;
    let peer = sandbox.input_peer(alice.id, Peer::Channel(team));
    let open = new_link(&sandbox, alice.id, peer.clone(), false).await;
    let asking = new_link(&sandbox, alice.id, peer.clone(), true).await;
    let hash = |link: &str| link.rsplit('+').next().unwrap().to_owned();
    call(
        &sandbox,
        bob.id,
        functions::messages::ImportChatInvite { hash: hash(&open) },
    )
    .await
    .unwrap();
    let requested = call(
        &sandbox,
        carol.id,
        functions::messages::ImportChatInvite {
            hash: hash(&asking),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(requested.message, "INVITE_REQUEST_SENT");

    let list = |revoked| functions::messages::GetExportedChatInvites {
        revoked,
        peer: peer.clone(),
        admin_id: enums::InputUser::InputUserSelf,
        limit: 50,
        ..Default::default()
    };
    let enums::messages::ExportedChatInvites::ExportedChatInvites(live) =
        call(&sandbox, alice.id, list(false)).await.unwrap();
    assert_eq!(live.count, 2);

    let importers = |requested, link: Option<String>| functions::messages::GetChatInviteImporters {
        requested,
        peer: peer.clone(),
        link,
        offset_user: enums::InputUser::Empty,
        limit: 50,
        ..Default::default()
    };
    let enums::messages::ChatInviteImporters::ChatInviteImporters(joined) =
        call(&sandbox, alice.id, importers(false, Some(open.clone())))
            .await
            .unwrap();
    assert_eq!(joined.importers.len(), 1);
    assert!(
        matches!(&joined.importers[0], enums::ChatInviteImporter::ChatInviteImporter(i) if i.user_id == bob.id.0 && !i.requested)
    );
    let enums::messages::ChatInviteImporters::ChatInviteImporters(waiting) =
        call(&sandbox, alice.id, importers(true, None))
            .await
            .unwrap();
    assert!(
        matches!(&waiting.importers[..], [enums::ChatInviteImporter::ChatInviteImporter(i)] if i.user_id == carol.id.0 && i.requested)
    );
    let denied = call(
        &sandbox,
        bob.id,
        functions::messages::GetChatInviteImporters {
            requested: true,
            peer: sandbox.input_peer(bob.id, Peer::Channel(team)),
            offset_user: enums::InputUser::Empty,
            limit: 50,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(denied.message, "CHAT_ADMIN_REQUIRED");

    // Live links can't be deleted; revoked ones can.
    let delete = |link: &str| functions::messages::DeleteExportedChatInvite {
        peer: peer.clone(),
        link: link.to_owned(),
    };
    let err = call(&sandbox, alice.id, delete(&open)).await.unwrap_err();
    assert_eq!(err.message, "INVITE_REVOKED_MISSING");
    call(
        &sandbox,
        alice.id,
        functions::messages::EditExportedChatInvite {
            revoked: true,
            peer: peer.clone(),
            link: open.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let enums::messages::ExportedChatInvites::ExportedChatInvites(revoked) =
        call(&sandbox, alice.id, list(true)).await.unwrap();
    assert_eq!(revoked.count, 1);
    assert!(call(&sandbox, alice.id, delete(&open)).await.unwrap());
    let enums::messages::ExportedChatInvites::ExportedChatInvites(revoked) =
        call(&sandbox, alice.id, list(true)).await.unwrap();
    assert_eq!(revoked.count, 0);
}

#[tokio::test]
async fn basic_group_admins_and_deleting_a_supergroup() {
    let (sandbox, alice, _) = setup().await;
    let bob = user(&sandbox, "Bob").await;
    let created = call(
        &sandbox,
        alice.id,
        functions::messages::CreateChat {
            users: vec![input_user(&sandbox, alice.id, bob.id)],
            title: "Small".into(),
            ttl_period: None,
        },
    )
    .await
    .unwrap();
    let enums::messages::InvitedUsers::InvitedUsers(created) = created;
    let enums::Updates::Updates(updates) = created.updates else {
        panic!()
    };
    let chat = updates
        .chats
        .iter()
        .find_map(|c| match c {
            enums::Chat::Chat(c) => Some(c.id),
            _ => None,
        })
        .unwrap();
    let promote = |viewer, is_admin| functions::messages::EditChatAdmin {
        chat_id: chat,
        user_id: input_user(
            &sandbox,
            viewer,
            if viewer == alice.id { bob.id } else { alice.id },
        ),
        is_admin,
    };
    assert_eq!(
        call(&sandbox, bob.id, promote(bob.id, true))
            .await
            .unwrap_err()
            .message,
        "CHAT_ADMIN_REQUIRED"
    );
    assert!(
        call(&sandbox, alice.id, promote(alice.id, true))
            .await
            .unwrap()
    );
    let enums::messages::ChatFull::ChatFull(full) = call(
        &sandbox,
        bob.id,
        functions::messages::GetFullChat { chat_id: chat },
    )
    .await
    .unwrap();
    let enums::ChatFull::ChatFull(info) = &full.full_chat else {
        panic!()
    };
    let enums::ChatParticipants::ChatParticipants(members) = &info.participants else {
        panic!()
    };
    assert!(
        members
            .participants
            .iter()
            .any(|p| matches!(p, enums::ChatParticipant::Admin(a) if a.user_id == bob.id.0))
    );

    // Only the owner deletes a supergroup; members are left with a forbidden chat.
    let team = supergroup(&sandbox, alice.id).await;
    let enums::InputPeer::Channel(input) = sandbox.input_peer(alice.id, Peer::Channel(team)) else {
        unreachable!()
    };
    let channel = |id: i64, hash: i64| -> enums::InputChannel {
        types::InputChannel {
            channel_id: id,
            access_hash: hash,
        }
        .into()
    };
    call(
        &sandbox,
        alice.id,
        functions::channels::InviteToChannel {
            channel: channel(input.channel_id, input.access_hash),
            users: vec![input_user(&sandbox, alice.id, bob.id)],
        },
    )
    .await
    .unwrap();
    let enums::InputPeer::Channel(bobs) = sandbox.input_peer(bob.id, Peer::Channel(team)) else {
        unreachable!()
    };
    let err = call(
        &sandbox,
        bob.id,
        functions::channels::DeleteChannel {
            channel: channel(bobs.channel_id, bobs.access_hash),
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.message, "CHAT_ADMIN_REQUIRED");
    call(
        &sandbox,
        alice.id,
        functions::channels::DeleteChannel {
            channel: channel(input.channel_id, input.access_hash),
        },
    )
    .await
    .unwrap();
    let enums::messages::Chats::Chats(seen) = call(
        &sandbox,
        bob.id,
        functions::channels::GetChannels {
            id: vec![channel(bobs.channel_id, bobs.access_hash)],
        },
    )
    .await
    .unwrap() else {
        panic!()
    };
    assert!(matches!(
        seen.chats.first(),
        Some(enums::Chat::ChannelForbidden(_))
    ));
}
