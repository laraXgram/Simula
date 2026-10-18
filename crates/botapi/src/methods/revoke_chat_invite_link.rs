//! `revokeChatInviteLink` — https://core.telegram.org/bots/api#revokechatinvitelink

use simula_botapi_types::{methods, types as bot};
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::invites;

/// The revoked link; revoking a primary link makes a new one.
pub async fn handle(
    ctx: BotCtx,
    params: methods::RevokeChatInviteLink,
) -> BotResult<bot::ChatInviteLink> {
    let chat = invites::chat(&ctx, &params.chat_id).await?;
    invites::edit(
        &ctx,
        functions::messages::EditExportedChatInvite {
            revoked: true,
            peer: ctx.input_peer(chat),
            link: params.invite_link,
            ..Default::default()
        },
    )
    .await
}
