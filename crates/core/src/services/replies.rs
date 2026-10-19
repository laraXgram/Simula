//! Replies. Within a chat, the sender names the replied message by its id in their own
//! box; every participant's copy of the reply points at their own copy of it. In
//! supergroups and channels there is one box, so the id is the same for everyone.
//!
//! Replies to messages in other chats (`reply_to_peer_id`) carry where the message came
//! from instead (`reply_from`, like a forward header), its link preview, and a quote —
//! the sender's, or else the start of its text (an automatic quote). Only messages of
//! supergroups and channels keep their chat and id in the header: private chats' and
//! basic groups' ids mean nothing outside their boxes.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{channels, peers};
use crate::store;

/// A checked reply target, independent of whose box it is rendered for.
#[derive(Debug, Clone)]
pub struct Target {
    /// The copies' `global_id`, or the message id in a channel's box.
    global_id: i64,
    in_channel: bool,
    quote: Option<Quote>,
    author: Option<UserId>,
    external: Option<External>,
}

/// A replied message from another chat.
#[derive(Debug, Clone)]
struct External {
    /// The channel and message id, for messages of supergroups and channels.
    channel_message: Option<(ChannelId, i32)>,
    from: types::MessageFwdHeader,
    media: Option<enums::MessageMedia>,
}

/// Telegram's `quote_length_max`: automatic quotes are cut to this many UTF-16 units.
const MAX_QUOTE_LENGTH: usize = 1024;

impl Target {
    /// Who sent the replied message, when it is in the same chat.
    pub fn author(&self) -> Option<UserId> {
        self.author.filter(|_| self.external.is_none())
    }
}

#[derive(Debug, Clone)]
struct Quote {
    text: String,
    entities: Vec<enums::MessageEntity>,
    /// UTF-16 offset of the quote in the replied message.
    offset: i32,
    /// Chosen by the sender (`quote` in the header), not the automatic one.
    manual: bool,
}

/// Check `input` against `sender`'s box for the dialog with `peer`.
pub fn resolve(
    ctx: &Ctx<'_>,
    sender: UserId,
    peer: Peer,
    input: Option<&enums::InputReplyTo>,
) -> RpcResult<Option<Target>> {
    let Some(input) = input else {
        return Ok(None);
    };
    let enums::InputReplyTo::Message(reply) = input else {
        return Err(RpcError::not_implemented(
            "replies to stories and ephemeral messages",
        ));
    };
    if reply.monoforum_peer_id.is_some() {
        return Err(RpcError::not_implemented(
            "replies in channel direct messages",
        ));
    }
    if reply.todo_item_id.is_some() || reply.poll_option.is_some() {
        return Err(RpcError::not_implemented(
            "replies to checklist tasks and poll options",
        ));
    }
    if let Some(input) = &reply.reply_to_peer_id {
        let source = peers::resolve_input_peer(ctx, sender, input)?;
        if source != peer {
            return external(ctx, sender, source, reply).map(Some);
        }
    }
    let found = match peer {
        Peer::Channel(channel) => store::channels::message(ctx.db, channel, reply.reply_to_msg_id)?
            .and_then(|stored| match stored.message {
                enums::Message::Message(m) => Some((i64::from(m.id), *m)),
                _ => None,
            }),
        _ => store::messages::get_in(ctx.db, sender, peer, reply.reply_to_msg_id)?,
    };
    let Some((global_id, replied)) = found else {
        return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID"));
    };
    let quote = manual_quote(reply, &replied)?;
    let author = match peer {
        Peer::Channel(channel) => {
            store::channels::message(ctx.db, channel, replied.id)?.map(|stored| stored.author)
        }
        _ => match replied.from_id.as_ref().map(Peer::from_tl) {
            Some(Peer::User(user)) => Some(user),
            _ => None,
        },
    };
    Ok(Some(Target {
        global_id,
        in_channel: matches!(peer, Peer::Channel(_)),
        quote,
        author,
        external: None,
    }))
}

/// The sender's quote of `replied`, located in its text.
fn manual_quote(
    reply: &types::InputReplyToMessage,
    replied: &types::Message,
) -> RpcResult<Option<Quote>> {
    match &reply.quote_text {
        Some(text) if !text.is_empty() => {
            let offset = locate(&replied.message, text, reply.quote_offset)
                .ok_or_else(|| RpcError::bad_request("QUOTE_TEXT_INVALID"))?;
            Ok(Some(Quote {
                text: text.clone(),
                entities: reply.quote_entities.clone().unwrap_or_default(),
                offset,
                manual: true,
            }))
        }
        _ => Ok(None),
    }
}

/// A reply to message `reply.reply_to_msg_id` of `source`, a chat other than the one
/// the reply goes to. `sender` must be able to read it, and it must be one that could
/// be forwarded (not protected, not a service message).
fn external(
    ctx: &Ctx<'_>,
    sender: UserId,
    source: Peer,
    reply: &types::InputReplyToMessage,
) -> RpcResult<Target> {
    let id = reply.reply_to_msg_id;
    let (replied, author) = match source {
        Peer::Channel(channel) => {
            let found = channels::get(ctx, channel)?;
            if channels::require_reader(ctx, &found, sender)?.is_none_or(|m| !m.status.is_member())
                && found.username.is_none()
            {
                return Err(RpcError::forbidden("CHANNEL_PRIVATE"));
            }
            match store::channels::message(ctx.db, channel, id)? {
                Some(stored) => match stored.message {
                    enums::Message::Message(m) => (*m, stored.author),
                    _ => return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID")),
                },
                None => return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID")),
            }
        }
        _ => match store::messages::get_in(ctx.db, sender, source, id)? {
            Some((_, m)) => {
                let author = match m.from_id.as_ref().map(Peer::from_tl) {
                    Some(Peer::User(user)) => user,
                    _ => return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID")),
                };
                (m, author)
            }
            None => return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID")),
        },
    };
    if replied.noforwards {
        return Err(RpcError::bad_request("CHAT_FORWARDS_RESTRICTED"));
    }
    let quote = match manual_quote(reply, &replied)? {
        Some(quote) => quote,
        None => automatic_quote(&replied),
    };
    let channel_message = match source {
        Peer::Channel(channel) => Some((channel, id)),
        _ => None,
    };
    // Channel posts come from the channel; everything else from its author.
    let from = if replied.post {
        types::MessageFwdHeader {
            from_id: Some(source.to_tl()),
            channel_post: Some(id),
            post_author: replied.post_author.clone(),
            date: replied.date,
            ..Default::default()
        }
    } else {
        types::MessageFwdHeader {
            from_id: Some(Peer::User(author).to_tl()),
            date: replied.date,
            ..Default::default()
        }
    };
    Ok(Target {
        global_id: 0,
        in_channel: false,
        quote: Some(quote),
        author: Some(author),
        external: Some(External {
            channel_message,
            from,
            media: replied.media.clone(),
        }),
    })
}

/// The start of the replied text, with the entities a quote may keep.
fn automatic_quote(replied: &types::Message) -> Quote {
    let units: Vec<u16> = replied.message.encode_utf16().collect();
    let end = units.len().min(MAX_QUOTE_LENGTH) as i32;
    let text = String::from_utf16_lossy(&units[..end as usize]);
    let entities = replied
        .entities
        .iter()
        .flatten()
        .filter(|e| quote_allowed(e) && e.offset() < end)
        .map(|e| clip(e, end))
        .collect();
    Quote {
        text,
        entities,
        offset: 0,
        manual: false,
    }
}

/// Entity kinds a quote keeps (tdlib's `is_allowed_quote_entity_type`).
fn quote_allowed(entity: &enums::MessageEntity) -> bool {
    matches!(
        entity,
        enums::MessageEntity::Bold(_)
            | enums::MessageEntity::Italic(_)
            | enums::MessageEntity::Underline(_)
            | enums::MessageEntity::Strike(_)
            | enums::MessageEntity::Spoiler(_)
            | enums::MessageEntity::CustomEmoji(_)
            | enums::MessageEntity::FormattedDate(_)
    )
}

/// `entity` cut to end by UTF-16 offset `end`.
fn clip(entity: &enums::MessageEntity, end: i32) -> enums::MessageEntity {
    let mut clipped = entity.clone();
    let length = (end - entity.offset()).min(entity.length());
    *clipped.length_mut() = length;
    clipped
}

/// The reply header of `owner`'s copy. If `owner` no longer has the replied message, the
/// header stays without an id, as for replies to deleted messages.
pub fn header(
    ctx: &Ctx<'_>,
    owner: UserId,
    target: &Target,
) -> RpcResult<enums::MessageReplyHeader> {
    let quote = target.quote.as_ref();
    if let Some(external) = &target.external {
        return Ok(types::MessageReplyHeader {
            reply_to_peer_id: external
                .channel_message
                .map(|(channel, _)| Peer::Channel(channel).to_tl()),
            reply_to_msg_id: external.channel_message.map(|(_, id)| id),
            reply_from: Some(external.from.clone().into()),
            reply_media: external.media.clone(),
            quote: quote.is_some_and(|q| q.manual),
            quote_text: quote.map(|q| q.text.clone()),
            quote_entities: quote
                .and_then(|q| (!q.entities.is_empty()).then(|| q.entities.clone())),
            quote_offset: quote.filter(|q| q.manual).map(|q| q.offset),
            ..Default::default()
        }
        .into());
    }
    Ok(types::MessageReplyHeader {
        reply_to_msg_id: if target.in_channel {
            Some(target.global_id as i32)
        } else {
            store::messages::copy_id(ctx.db, owner, target.global_id)?
        },
        quote: quote.is_some(),
        quote_text: quote.map(|q| q.text.clone()),
        quote_entities: quote.and_then(|q| (!q.entities.is_empty()).then(|| q.entities.clone())),
        quote_offset: quote.map(|q| q.offset),
        ..Default::default()
    }
    .into())
}

/// Where `quote` occurs in `text` (UTF-16 units): the occurrence nearest to the
/// sender's approximate `offset`.
fn locate(text: &str, quote: &str, offset: Option<i32>) -> Option<i32> {
    let text: Vec<u16> = text.encode_utf16().collect();
    let quote: Vec<u16> = quote.encode_utf16().collect();
    let wanted = offset.unwrap_or(0).max(0) as usize;
    text.windows(quote.len())
        .enumerate()
        .filter(|(_, window)| *window == quote.as_slice())
        .map(|(position, _)| position)
        .min_by_key(|&position| position.abs_diff(wanted))
        .map(|position| position as i32)
}

#[cfg(test)]
mod tests {
    use super::locate;

    #[test]
    fn quotes_are_found_nearest_to_the_given_offset() {
        assert_eq!(locate("ab ab ab", "ab", None), Some(0));
        assert_eq!(locate("ab ab ab", "ab", Some(4)), Some(3));
        assert_eq!(locate("😀 hi", "hi", None), Some(3), "UTF-16 units");
        assert_eq!(locate("hello", "bye", None), None);
    }
}
