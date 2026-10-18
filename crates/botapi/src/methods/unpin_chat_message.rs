//! `unpinChatMessage` — https://core.telegram.org/bots/api#unpinchatmessage

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::lookup;
use crate::pins;
use crate::resolve::{self, Access};

/// The given message, or the newest pinned one.
pub async fn handle(ctx: BotCtx, params: methods::UnpinChatMessage) -> BotResult<bool> {
    if params
        .business_connection_id
        .is_some_and(|id| !id.is_empty())
    {
        return Err(BotError::not_implemented("business connections"));
    }
    let (chat, id) = match params.message_id.filter(|&id| id != 0) {
        Some(id) => {
            let (chat, message) =
                lookup::message(&ctx, Some(&params.chat_id), id, "message to unpin").await?;
            (chat, message.id)
        }
        None => {
            let chat = resolve::chat_for(&ctx, &params.chat_id, Access::Write).await?;
            let id = pins::pinned_id(&ctx, chat)
                .await?
                .ok_or_else(|| BotError::bad_request("message to unpin not found"))?;
            (chat, id)
        }
    };
    ctx.call(
        functions::messages::UpdatePinnedMessage {
            unpin: true,
            peer: ctx.input_peer(chat),
            id,
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
