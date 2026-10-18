//! TL `message` → Bot API `Message`.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::{enums, types};

use super::{Entities, chat::chat, entities::entities, link_preview, markup, user::user};

pub fn message(m: &types::Message, context: &Entities<'_>) -> bot::Message {
    convert(m, context, true)
}

/// `with_reply`: whether to include `reply_to_message` (never nested twice, like
/// telegram-bot-api's `need_reply`).
fn convert(m: &types::Message, context: &Entities<'_>, with_reply: bool) -> bot::Message {
    let from = m
        .from_id
        .as_ref()
        .map(Peer::from_tl)
        .and_then(|peer| match peer {
            Peer::User(id) => context.user(id.0).map(|u| Box::new(user(u, false))),
            _ => None,
        });
    let header = match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(header)) => Some(&**header),
        _ => None,
    };
    let is_text = is_text(m);
    bot::Message {
        message_id: m.id.into(),
        from,
        date: m.date.into(),
        chat: chat(Peer::from_tl(&m.peer_id), context),
        reply_to_message: header
            .filter(|_| with_reply)
            .and_then(|h| replied(m, h, context))
            .map(|replied| Box::new(convert(replied, context, false))),
        quote: header.and_then(|h| quote(h, context)).map(Box::new),
        edit_date: m.edit_date.map(Into::into),
        has_protected_content: m.noforwards,
        text: (!m.message.is_empty() && is_text).then(|| m.message.clone()),
        entities: entities(m.entities.as_ref(), context),
        link_preview_options: is_text
            .then(|| link_preview::options(m))
            .flatten()
            .map(Box::new),
        effect_id: m.effect.map(|id| id.to_string()),
        reply_markup: markup::inline_keyboard(m.reply_markup.as_ref()).map(Box::new),
        ..Default::default()
    }
}

/// A text message, possibly with a link preview (which bots don't see as media).
pub fn is_text(m: &types::Message) -> bool {
    matches!(m.media, None | Some(enums::MessageMedia::WebPage(_)))
}

/// The id of the message `m` replies to in the same chat, if any.
pub fn reply_id(m: &types::Message) -> Option<i32> {
    match &m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) if h.reply_to_peer_id.is_none() => {
            h.reply_to_msg_id
        }
        _ => None,
    }
}

/// The replied message, when it is in the same chat and was fetched.
fn replied<'a>(
    m: &types::Message,
    header: &types::MessageReplyHeader,
    context: &Entities<'a>,
) -> Option<&'a types::Message> {
    if header.reply_to_peer_id.is_some() {
        return None;
    }
    context
        .message(header.reply_to_msg_id?)
        .filter(|replied| replied.peer_id == m.peer_id)
}

fn quote(header: &types::MessageReplyHeader, context: &Entities<'_>) -> Option<bot::TextQuote> {
    let text = header.quote_text.clone().filter(|t| !t.is_empty())?;
    Some(bot::TextQuote {
        text,
        entities: entities(header.quote_entities.as_ref(), context),
        position: header.quote_offset.unwrap_or(0).max(0).into(),
        is_manual: header.quote,
    })
}
