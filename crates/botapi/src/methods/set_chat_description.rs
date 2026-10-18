//! `setChatDescription` — https://core.telegram.org/bots/api#setchatdescription

use simula_botapi_types::methods;
use simula_core::Peer;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::SetChatDescription) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    if matches!(chat, Peer::User(_)) {
        return Err(BotError::bad_request(
            "can't change private chat description",
        ));
    }
    ctx.call(
        functions::messages::EditChatAbout {
            peer: ctx.input_peer(chat),
            about: params.description.unwrap_or_default(),
        },
        ErrorContext::Plain,
    )
    .await?;
    Ok(true)
}
