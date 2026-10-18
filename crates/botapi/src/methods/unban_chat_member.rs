//! `unbanChatMember` — https://core.telegram.org/bots/api#unbanchatmember

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::{enums, types};

use crate::admin;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::resolve::{self, Access};

/// Afterwards the user isn't a member but may join: a member is removed too, unless
/// `only_if_banned` (then only banned users change).
pub async fn handle(ctx: BotCtx, params: methods::UnbanChatMember) -> BotResult<bool> {
    let chat = match resolve::chat_for(&ctx, &params.chat_id, Access::Write).await? {
        Peer::User(_) => {
            return Err(BotError::bad_request(
                "can't remove members from private chats",
            ));
        }
        Peer::Chat(_) => {
            return Err(BotError::bad_request(
                "can't remove members from group chats",
            ));
        }
        chat => chat,
    };
    let current = admin::participant(&ctx, chat, params.user_id).await?;
    let banned = matches!(&current, Some(enums::ChannelParticipant::Banned(b)) if b.left);
    if params.only_if_banned == Some(true) && !banned {
        return Ok(true);
    }
    let is_member = matches!(
        &current,
        Some(enums::ChannelParticipant::ChannelParticipant(_))
            | Some(enums::ChannelParticipant::ChannelParticipantSelf(_))
            | Some(enums::ChannelParticipant::Admin(_))
            | Some(enums::ChannelParticipant::Creator(_))
    ) || matches!(&current, Some(enums::ChannelParticipant::Banned(b)) if !b.left);
    if is_member {
        let ban = types::ChatBannedRights {
            view_messages: true,
            ..Default::default()
        };
        admin::edit_banned(&ctx, chat, params.user_id, ban).await?;
    }
    if is_member || banned {
        admin::edit_banned(
            &ctx,
            chat,
            params.user_id,
            types::ChatBannedRights::default(),
        )
        .await?;
    }
    Ok(true)
}
