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
    for input in &req.id {
        let message = match input {
            enums::InputMessage::ID(m) => store::messages::get(ctx.db, caller, m.id)?,
            enums::InputMessage::ReplyTo(m) => replied(ctx, m.id)?,
            _ => {
                return Err(RpcError::not_implemented(
                    "messages.getMessages for pinned and callback query messages",
                ));
            }
        };
        found.push(match message {
            Some(message) => {
                users.extend(messages::users(&message));
                message.into()
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
        chats: vec![],
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}

/// The message that the caller's message `id` replies to.
fn replied(ctx: &Ctx<'_>, id: i32) -> RpcResult<Option<types::Message>> {
    let caller = ctx.caller()?.account;
    let reply_to = store::messages::get(ctx.db, caller, id)?.and_then(|m| match m.reply_to {
        Some(enums::MessageReplyHeader::MessageReplyHeader(header)) => header.reply_to_msg_id,
        _ => None,
    });
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
