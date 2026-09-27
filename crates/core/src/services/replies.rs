//! Replies within a chat. The sender names the replied message by its id in their own
//! box; every participant's copy of the reply points at their own copy of it.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::store;

/// A checked reply target, independent of whose box it is rendered for.
#[derive(Debug, Clone)]
pub struct Target {
    global_id: i64,
    quote: Option<Quote>,
}

#[derive(Debug, Clone)]
struct Quote {
    text: String,
    entities: Vec<enums::MessageEntity>,
    /// UTF-16 offset of the quote in the replied message.
    offset: i32,
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
    if reply.reply_to_peer_id.is_some() || reply.monoforum_peer_id.is_some() {
        return Err(RpcError::not_implemented(
            "replies to messages in other chats",
        ));
    }
    if reply.todo_item_id.is_some() || reply.poll_option.is_some() {
        return Err(RpcError::not_implemented(
            "replies to checklist tasks and poll options",
        ));
    }
    let Some((global_id, replied)) =
        store::messages::get_in(ctx.db, sender, peer, reply.reply_to_msg_id)?
    else {
        return Err(RpcError::bad_request("REPLY_MESSAGE_ID_INVALID"));
    };
    let quote = match &reply.quote_text {
        Some(text) if !text.is_empty() => {
            let offset = locate(&replied.message, text, reply.quote_offset)
                .ok_or_else(|| RpcError::bad_request("QUOTE_TEXT_INVALID"))?;
            Some(Quote {
                text: text.clone(),
                entities: reply.quote_entities.clone().unwrap_or_default(),
                offset,
            })
        }
        _ => None,
    };
    Ok(Some(Target { global_id, quote }))
}

/// The reply header of `owner`'s copy. If `owner` no longer has the replied message, the
/// header stays without an id, as for replies to deleted messages.
pub fn header(
    ctx: &Ctx<'_>,
    owner: UserId,
    target: &Target,
) -> RpcResult<enums::MessageReplyHeader> {
    let quote = target.quote.as_ref();
    Ok(types::MessageReplyHeader {
        reply_to_msg_id: store::messages::copy_id(ctx.db, owner, target.global_id)?,
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
