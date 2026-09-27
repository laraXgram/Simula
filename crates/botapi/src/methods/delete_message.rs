//! `deleteMessage`

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::delete;
use crate::error::BotResult;
use crate::lookup;

pub async fn handle(ctx: BotCtx, params: methods::DeleteMessage) -> BotResult<bool> {
    let (chat, message) = lookup::message(
        &ctx,
        Some(&params.chat_id),
        params.message_id,
        "message to delete",
    )
    .await?;
    delete::delete(&ctx, chat, &[message]).await
}
