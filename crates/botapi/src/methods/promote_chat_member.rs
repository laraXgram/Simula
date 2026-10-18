//! `promoteChatMember` — https://core.telegram.org/bots/api#promotechatmember

use simula_botapi_types::methods;
use simula_tl::enums;

use crate::admin;
use crate::convert::permissions;
use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::resolve::{self, Access};

/// Supergroups and channels. No rights demotes; an admin keeps their custom title.
pub async fn handle(ctx: BotCtx, params: methods::PromoteChatMember) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    admin::require_channel(
        chat,
        "method is available for supergroup and channel chats only",
    )?;
    let rights = permissions::promoted(&params);
    let rank = match admin::participant(&ctx, chat, params.user_id).await? {
        Some(enums::ChannelParticipant::Admin(a)) => a.rank,
        _ => None,
    };
    admin::edit_admin(&ctx, chat, params.user_id, rights, rank).await?;
    Ok(true)
}
