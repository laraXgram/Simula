//! `messages.getMessages`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::messages;
use crate::store;

/// Messages from the caller's own box (private chats and basic groups). Missing or
/// deleted ones come back as `messageEmpty`, in request order.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetMessages,
) -> RpcResult<enums::messages::Messages> {
    let caller = ctx.caller()?.account;
    let mut found = Vec::with_capacity(req.id.len());
    let mut users = BTreeSet::new();
    let mut chats = BTreeSet::new();
    let mut channels = BTreeSet::new();
    for input in &req.id {
        let message = match input {
            enums::InputMessage::ID(m) => store::messages::get_any(ctx.db, caller, m.id)?,
            enums::InputMessage::ReplyTo(m) => replied(ctx, m.id)?.map(Into::into),
            _ => {
                return Err(RpcError::not_implemented(
                    "messages.getMessages for pinned and callback query messages",
                ));
            }
        };
        found.push(match message {
            Some(message) => {
                users.extend(messages::users_any(&message));
                channels.extend(messages::channels_any(&message));
                if let Some(enums::Peer::Chat(chat)) = message_peer(&message) {
                    chats.insert(crate::ids::ChatId(chat.chat_id));
                }
                message
            }
            None => types::MessageEmpty {
                id: input_id(input),
                peer_id: None,
            }
            .into(),
        });
    }
    Ok(types::messages::Messages {
        messages: found,
        topics: vec![],
        chats: render::all_chats(ctx, caller, &chats, &channels)?,
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}

/// The message that the caller's message `id` replies to.
fn replied(ctx: &Ctx<'_>, id: i32) -> RpcResult<Option<types::Message>> {
    let caller = ctx.caller()?.account;
    let reply_to =
        store::messages::get(ctx.db, caller, id)?.and_then(|m| messages::same_chat_reply_id(&m));
    match reply_to {
        Some(id) => Ok(store::messages::get(ctx.db, caller, id)?),
        None => Ok(None),
    }
}

fn input_id(input: &enums::InputMessage) -> i32 {
    match input {
        enums::InputMessage::ID(m) => m.id,
        enums::InputMessage::ReplyTo(m) => m.id,
        _ => 0,
    }
}

fn message_peer(message: &enums::Message) -> Option<&enums::Peer> {
    match message {
        enums::Message::Message(m) => Some(&m.peer_id),
        enums::Message::Service(m) => Some(&m.peer_id),
        enums::Message::Empty(_) => None,
    }
}
