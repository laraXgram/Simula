//! `reply_parameters`. telegram-bot-api parses them (quote included) before anything
//! else, then, once the chat is known, fetches the replied message: a missing one is
//! "message to be replied not found" unless `allow_sending_without_reply` is set.

use simula_botapi_types::{ChatId, types as bot};
use simula_core::Peer;
use simula_tl::{enums, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::resolve;
use crate::text::{self, Formatted};

#[derive(Debug, Default)]
pub struct Request {
    message_id: i32,
    chat_id: Option<ChatId>,
    allow_sending_without_reply: bool,
    quote: Option<(Formatted, i32)>,
    checklist_task_id: Option<i32>,
    poll_option_id: Option<String>,
    ephemeral_message_id: i64,
}

pub fn parse(params: Option<&bot::ReplyParameters>) -> BotResult<Request> {
    let Some(p) = params else {
        return Ok(Request::default());
    };
    let quote = match p.quote.as_deref() {
        Some(quote) if !quote.is_empty() => Some((
            text::parse(
                quote,
                p.quote_parse_mode.as_deref(),
                p.quote_entities.as_deref(),
            )?,
            p.quote_position.unwrap_or(0) as i32,
        )),
        _ => None,
    };
    Ok(Request {
        message_id: p.message_id.unwrap_or(0).clamp(0, i32::MAX as i64) as i32,
        chat_id: p.chat_id.clone(),
        allow_sending_without_reply: p.allow_sending_without_reply == Some(true),
        quote,
        checklist_task_id: p
            .checklist_task_id
            .filter(|&id| id != 0)
            .map(|id| id as i32),
        poll_option_id: p.poll_option_id.clone().filter(|id| !id.is_empty()),
        ephemeral_message_id: p.ephemeral_message_id.unwrap_or(0).max(0),
    })
}

/// The TL reply for a send into `chat`, after checking the replied message.
pub async fn check(
    ctx: &BotCtx,
    chat: Peer,
    request: Request,
) -> BotResult<Option<enums::InputReplyTo>> {
    if request.ephemeral_message_id > 0 {
        return Err(BotError::not_implemented("replies to ephemeral messages"));
    }
    if request.message_id <= 0 {
        return Ok(None);
    }
    if let Some(chat_id) = &request.chat_id
        && resolve::chat(ctx, chat_id).await? != chat
    {
        return Err(BotError::not_implemented(
            "replies to messages in other chats",
        ));
    }

    let exists = !crate::lookup::messages(ctx, chat, &[request.message_id])
        .await?
        .is_empty();
    if !exists {
        if request.allow_sending_without_reply {
            return Ok(None);
        }
        return Err(BotError::bad_request("message to be replied not found"));
    }

    // tdlib's `MessageQuote`: a quote that is empty after trimming is dropped.
    let quote = request
        .quote
        .and_then(|(quote, position)| quote.into_quote(position));
    Ok(Some(
        types::InputReplyToMessage {
            reply_to_msg_id: request.message_id,
            quote_offset: quote.as_ref().map(|(_, position)| *position),
            quote_entities: quote
                .as_ref()
                .map(|(quote, _)| ctx.tl_entities(&quote.entities))
                .filter(|entities| !entities.is_empty()),
            quote_text: quote.map(|(quote, _)| quote.text),
            todo_item_id: request.checklist_task_id,
            poll_option: request.poll_option_id.map(String::into_bytes),
            ..Default::default()
        }
        .into(),
    ))
}
