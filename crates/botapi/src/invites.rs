//! What the invite link methods share: the chat check, the bot's own user (links it
//! creates name it), and turning TL links into `ChatInviteLink`.

use simula_botapi_types::{ChatId, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::convert::{self, Entities};
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

/// Groups, supergroups and channels; tdlib refuses private chats.
pub async fn chat(ctx: &BotCtx, chat_id: &ChatId) -> BotResult<Peer> {
    match resolve::chat_for(ctx, chat_id, Access::Write).await? {
        Peer::User(_) => Err(BotError::bad_request(
            "can't invite members to a private chat",
        )),
        peer => Ok(peer),
    }
}

/// tdlib's checks on a link's settings.
pub fn check(
    name: Option<&str>,
    member_limit: Option<i64>,
    creates_join_request: bool,
) -> BotResult<()> {
    if name.is_some_and(|n| n.chars().count() > 32) {
        return Err(BotError::bad_request("invite link name is too long"));
    }
    if creates_join_request && member_limit.is_some_and(|l| l != 0) {
        return Err(BotError::bad_request(
            "member limit can't be specified for links requiring administrator approval",
        ));
    }
    if member_limit.is_some_and(|l| !(0..=99_999).contains(&l)) {
        return Err(BotError::bad_request("invalid member limit specified"));
    }
    Ok(())
}

/// A TL link as `ChatInviteLink`, with `users` (and the bot) to name its creator.
pub async fn link(
    ctx: &BotCtx,
    invite: &enums::ExportedChatInvite,
    users: &[enums::User],
) -> BotResult<bot::ChatInviteLink> {
    let mut users = users.to_vec();
    let me = ctx
        .call(
            functions::users::GetUsers {
                id: vec![enums::InputUser::InputUserSelf],
            },
            ErrorContext::Plain,
        )
        .await?;
    users.extend(me);
    let entities = Entities {
        users: &users,
        chats: &[],
        messages: &[],
    };
    convert::invite::link(invite, &entities)
        .ok_or_else(|| BotError::internal("the server returned no invite link"))
}

/// Edit or revoke a link with `messages.editExportedChatInvite`; returns the edited link.
pub async fn edit(
    ctx: &BotCtx,
    call: functions::messages::EditExportedChatInvite,
) -> BotResult<bot::ChatInviteLink> {
    let result = ctx.call(call, ErrorContext::Plain).await.map_err(|e| {
        if e.description.ends_with("INVITE_HASH_INVALID") {
            BotError::bad_request("INVITE_HASH_EXPIRED")
        } else {
            e
        }
    })?;
    let (invite, users) = match result {
        enums::messages::ExportedChatInvite::ExportedChatInvite(r) => (r.invite, r.users),
        enums::messages::ExportedChatInvite::Replaced(r) => (r.invite, r.users),
    };
    link(ctx, &invite, &users).await
}
