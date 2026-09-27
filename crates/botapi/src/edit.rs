//! The pipeline the `editMessage*` methods share, in telegram-bot-api's order: parse the
//! new keyboard, pick the message (chat + id, or an inline message), fetch it; then
//! tdlib's `can_edit_message` checks, the TL call, and the edited message (rendered
//! without `reply_to_message`, as telegram-bot-api does for edits).

use simula_botapi_types::{ChatId, types as bot};
use simula_core::Peer;
use simula_tl::{enums, functions};

use crate::convert::{self, Entities};
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};
use crate::lookup;
use crate::send::markup::{self, Markup};
use crate::text::Formatted;

/// Which message: the parameters every `editMessage*` method has.
pub struct Target {
    pub business_connection_id: Option<String>,
    pub chat_id: Option<ChatId>,
    pub message_id: Option<i64>,
    pub inline_message_id: Option<String>,
}

pub enum Change {
    Text {
        text: Formatted,
        link_preview: Option<Box<bot::LinkPreviewOptions>>,
    },
    /// Only the keyboard (`editMessageReplyMarkup`).
    ReplyMarkup,
}

pub async fn edit(
    ctx: &BotCtx,
    target: Target,
    reply_markup: Option<Box<bot::InlineKeyboardMarkup>>,
    change: Change,
) -> BotResult<bot::MessageOrTrue> {
    let reply_markup = reply_markup.map(bot::ReplyMarkup::InlineKeyboardMarkup);
    let markup = markup::parse(reply_markup.as_ref())?;
    let message_id = target.message_id.unwrap_or(0).max(0);
    if target.chat_id.is_none() && message_id == 0 {
        if target.inline_message_id.is_none_or(|id| id.is_empty()) {
            return Err(BotError::bad_request("message identifier is not specified"));
        }
        return Err(BotError::not_implemented("editing inline messages"));
    }
    if target
        .business_connection_id
        .is_some_and(|id| !id.is_empty())
    {
        return Err(BotError::not_implemented("business connections"));
    }
    let (chat, message) =
        lookup::message(ctx, target.chat_id.as_ref(), message_id, "message to edit").await?;

    // tdlib's `can_edit_message`, then the content kind.
    let has_reply_keyboard = message
        .reply_markup
        .as_ref()
        .is_some_and(|m| !matches!(m, enums::ReplyMarkup::ReplyInlineMarkup(_)));
    if !message.out || has_reply_keyboard {
        return Err(BotError::bad_request("message can't be edited"));
    }
    if matches!(change, Change::Text { .. }) && message.media.is_some() {
        return Err(BotError::bad_request(
            "there is no text in the message to edit",
        ));
    }
    let private_chat = matches!(chat, Peer::User(_));
    let reply_markup = match markup {
        None => None,
        Some(m @ Markup::Inline { .. }) => m.into_tl(markup::Target { private_chat })?,
        Some(_) => return Err(BotError::bad_request("inline keyboard expected")),
    };

    let mut call = functions::messages::EditMessage {
        peer: ctx.input_peer(chat),
        id: message.id,
        reply_markup,
        ..Default::default()
    };
    if let Change::Text { text, link_preview } = change {
        let text = text.fix()?;
        let preview = link_preview.as_deref();
        call.no_webpage = preview.is_some_and(|p| p.is_disabled == Some(true));
        call.invert_media = preview.is_some_and(|p| p.show_above_text == Some(true));
        call.entities = Some(ctx.tl_entities(&text.entities)).filter(|e| !e.is_empty());
        call.message = Some(text.text);
    }
    let updates = ctx.call(call, ErrorContext::Plain).await?;
    let enums::Updates::Updates(batch) = &updates else {
        return Err(BotError::internal(
            "messages.editMessage returned no updates",
        ));
    };
    let edited = batch.updates.iter().find_map(|u| match u {
        enums::Update::EditMessage(u) => match &u.message {
            enums::Message::Message(m) => Some(&**m),
            _ => None,
        },
        _ => None,
    });
    let edited =
        edited.ok_or_else(|| BotError::internal("messages.editMessage returned no message"))?;
    let context = Entities {
        users: &batch.users,
        messages: &[],
    };
    Ok(bot::MessageOrTrue::Message(Box::new(
        convert::message::message(edited, &context),
    )))
}
