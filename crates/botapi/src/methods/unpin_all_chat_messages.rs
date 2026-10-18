//! `unpinAllChatMessages` — https://core.telegram.org/bots/api#unpinallchatmessages

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve::{self, Access};

pub async fn handle(ctx: BotCtx, params: methods::UnpinAllChatMessages) -> BotResult<bool> {
    let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
    ctx.call(
        functions::messages::UnpinAllMessages {
            peer: ctx.input_peer(chat),
            ..Default::default()
        },
        ErrorContext::Plain,
    )
    .await
    .map_err(rights_error)?;
    Ok(true)
}

/// tdlib checks the right itself: "Not enough rights to manage pinned messages".
fn rights_error(error: BotError) -> BotError {
    if error.description.ends_with("CHAT_ADMIN_REQUIRED") {
        BotError::bad_request("not enough rights to manage pinned messages in the chat")
    } else {
        error
    }
}
