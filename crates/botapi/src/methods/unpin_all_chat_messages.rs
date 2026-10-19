//! `unpinAllChatMessages` — https://core.telegram.org/bots/api#unpinallchatmessages

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::pins;
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::UnpinAllChatMessages) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    pins::unpin_all(&ctx, chat, None).await?;
    Ok(true)
}
