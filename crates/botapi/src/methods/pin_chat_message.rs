//! `pinChatMessage` — https://core.telegram.org/bots/api#pinchatmessage

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::lookup;

pub async fn handle(ctx: BotCtx, params: methods::PinChatMessage) -> BotResult<bool> {
    if params
        .business_connection_id
        .is_some_and(|id| !id.is_empty())
    {
        return Err(BotError::not_implemented("business connections"));
    }
    let (chat, message) = lookup::message(
        &ctx,
        Some(&params.chat_id),
        params.message_id,
        "message to pin",
    )
    .await?;
    ctx.call(
        functions::messages::UpdatePinnedMessage {
            silent: params.disable_notification == Some(true),
            peer: ctx.input_peer(chat),
            id: message.id,
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
