//! `channels.getMessages`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::{channel_messages, channels, peers};

/// Messages by id, or the messages that messages reply to; missing ones come back as
/// `messageEmpty`, in request order.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::GetMessages,
) -> RpcResult<enums::messages::Messages> {
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let channel = channels::get(ctx, id)?;
    let mut ids = Vec::with_capacity(req.id.len());
    for input in &req.id {
        ids.push(match input {
            enums::InputMessage::ID(m) => m.id,
            enums::InputMessage::ReplyTo(m) => {
                channel_messages::replied_id(ctx, id, m.id)?.unwrap_or(0)
            }
            _ => {
                return Err(RpcError::not_implemented(
                    "channels.getMessages for pinned and callback query messages",
                ));
            }
        });
    }
    let (messages, users, pts) = channel_messages::get(ctx, caller, id, &ids)?;
    Ok(types::messages::ChannelMessages {
        pts,
        count: messages.len() as i32,
        messages,
        chats: vec![render::channel(ctx, caller, &channel)?],
        users: render::users(ctx, caller, &users)?,
        ..Default::default()
    }
    .into())
}
