//! `exportChatInviteLink` — https://core.telegram.org/bots/api#exportchatinvitelink

use simula_botapi_types::methods;
use simula_tl::{enums, functions};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::invites;

/// A new primary link; the bot's previous one stops working.
pub async fn handle(ctx: BotCtx, params: methods::ExportChatInviteLink) -> BotResult<String> {
    let chat = invites::chat(&ctx, &params.chat_id).await?;
    let invite = ctx
        .call(
            functions::messages::ExportChatInvite {
                legacy_revoke_permanent: true,
                peer: ctx.input_peer(chat),
                ..Default::default()
            },
            ErrorContext::Plain,
        )
        .await?;
    match invite {
        enums::ExportedChatInvite::ChatInviteExported(i) => Ok(i.link),
        _ => Err(BotError::internal("the server returned no invite link")),
    }
}
