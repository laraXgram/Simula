//! Changing members' status in supergroups and channels (tdlib's
//! `setChatMemberStatus`), shared by the ban, restrict and promote methods.

use simula_core::{Peer, UserId};
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

/// `channels.editBanned` for `user`. tdlib checks the bot's right first and reports it
/// in its own words.
pub async fn edit_banned(
    ctx: &BotCtx,
    chat: Peer,
    user: i64,
    rights: types::ChatBannedRights,
) -> BotResult<()> {
    ctx.call(
        functions::channels::EditBanned {
            channel: ctx.input_channel(chat),
            participant: ctx.input_peer(Peer::User(UserId(user))),
            banned_rights: rights.into(),
        },
        ErrorContext::Plain,
    )
    .await
    .map_err(|e| rights_error(e, "not enough rights to restrict/unrestrict chat member"))?;
    Ok(())
}

/// `channels.editAdmin` for `user`.
pub async fn edit_admin(
    ctx: &BotCtx,
    chat: Peer,
    user: i64,
    rights: types::ChatAdminRights,
    rank: Option<String>,
) -> BotResult<()> {
    ctx.call(
        functions::channels::EditAdmin {
            channel: ctx.input_channel(chat),
            user_id: ctx.input_user(user),
            admin_rights: rights.into(),
            rank,
        },
        ErrorContext::Plain,
    )
    .await?;
    Ok(())
}

/// The status a user has, as TL (`None`: no status).
pub async fn participant(
    ctx: &BotCtx,
    chat: Peer,
    user: i64,
) -> BotResult<Option<enums::ChannelParticipant>> {
    let found = ctx
        .call(
            functions::channels::GetParticipant {
                channel: ctx.input_channel(chat),
                participant: ctx.input_peer(Peer::User(UserId(user))),
            },
            ErrorContext::Plain,
        )
        .await;
    match found {
        Ok(enums::channels::ChannelParticipant::ChannelParticipant(found)) => {
            Ok(Some(found.participant))
        }
        Err(e) if e.description.ends_with("USER_NOT_PARTICIPANT") => Ok(None),
        Err(e) => Err(e),
    }
}

fn rights_error(error: BotError, message: &str) -> BotError {
    if error.description.ends_with("CHAT_ADMIN_REQUIRED") {
        BotError::bad_request(message)
    } else {
        error
    }
}

/// Only supergroups and channels have these statuses.
pub fn require_channel(chat: Peer, message: &str) -> BotResult<()> {
    match chat {
        Peer::Channel(_) => Ok(()),
        _ => Err(BotError::bad_request(message)),
    }
}
