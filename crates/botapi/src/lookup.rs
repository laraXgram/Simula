//! telegram-bot-api's `check_message` / `check_messages`: resolve the chat, then fetch
//! the message(s) in it (as the bot, `messages.getMessages`).

use simula_botapi_types::ChatId;
use simula_core::Peer;
use simula_tl::{enums, functions, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::resolve;

/// One message. `what` names it in the error: "message to edit not found".
pub async fn message(
    ctx: &BotCtx,
    chat_id: Option<&ChatId>,
    message_id: i64,
    what: &str,
) -> BotResult<(Peer, types::Message)> {
    let chat_id = chat_id.ok_or_else(|| BotError::bad_request("chat_id is empty"))?;
    let chat = resolve::chat_for(ctx, chat_id, resolve::Access::Write).await?;
    let not_found = || BotError::bad_request(format!("{what} not found"));
    let id = i32::try_from(message_id).map_err(|_| not_found())?;
    if id <= 0 {
        return Err(not_found());
    }
    let message = messages(ctx, chat, &[id])
        .await?
        .pop()
        .ok_or_else(not_found)?;
    Ok((chat, message))
}

/// The messages among `ids` that exist in `chat`, in order; missing ones are skipped.
pub async fn messages(ctx: &BotCtx, chat: Peer, ids: &[i32]) -> BotResult<Vec<types::Message>> {
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let found = ctx
        .call(
            functions::messages::GetMessages {
                id: ids
                    .iter()
                    .map(|&id| types::InputMessageID { id }.into())
                    .collect(),
            },
            ErrorContext::default(),
        )
        .await?;
    let enums::messages::Messages::Messages(found) = found else {
        return Ok(Vec::new());
    };
    Ok(found
        .messages
        .into_iter()
        .filter_map(|m| match m {
            enums::Message::Message(m) if Peer::from_tl(&m.peer_id) == chat => Some(*m),
            _ => None,
        })
        .collect())
}
