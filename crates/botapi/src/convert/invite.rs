//! TL invite links → Bot API `ChatInviteLink`, and join requests → `ChatJoinRequest`.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::{enums, types};

use super::{Entities, chat::chat, user::user};

/// `None` for kinds the Bot API has no link for (public join requests).
pub fn link(
    invite: &enums::ExportedChatInvite,
    context: &Entities<'_>,
) -> Option<bot::ChatInviteLink> {
    let enums::ExportedChatInvite::ChatInviteExported(i) = invite else {
        return None;
    };
    let creator = context
        .user(i.admin_id)
        .map(|u| user(u, false))
        .unwrap_or(bot::User {
            id: i.admin_id,
            ..Default::default()
        });
    Some(bot::ChatInviteLink {
        invite_link: i.link.clone(),
        creator,
        creates_join_request: i.request_needed,
        is_primary: i.permanent,
        is_revoked: i.revoked,
        name: i.title.clone(),
        expire_date: i.expire_date.map(Into::into),
        member_limit: i.usage_limit.map(Into::into),
        pending_join_request_count: i.requested.map(Into::into),
        ..Default::default()
    })
}

/// `chat_join_request` for an `updateBotChatInviteRequester`.
pub fn join_request(
    u: &types::UpdateBotChatInviteRequester,
    context: &Entities<'_>,
) -> bot::ChatJoinRequest {
    bot::ChatJoinRequest {
        chat: chat(Peer::from_tl(&u.peer), context),
        from: context
            .user(u.user_id)
            .map(|found| user(found, false))
            .unwrap_or(bot::User {
                id: u.user_id,
                ..Default::default()
            }),
        user_chat_id: u.user_id,
        date: u.date.into(),
        bio: Some(u.about.clone()).filter(|a| !a.is_empty()),
        invite_link: link(&u.invite, context).map(Box::new),
        query_id: u.query_id.map(|id| id.to_string()),
    }
}
