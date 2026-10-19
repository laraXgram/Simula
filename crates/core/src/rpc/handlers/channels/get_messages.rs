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
    let found = channel_messages::get(ctx, caller, id, &ids)?;
    let mut chats = vec![render::channel(ctx, caller, &channel)?];
    let others = found
        .channels
        .iter()
        .copied()
        .filter(|&c| c != id)
        .collect();
    chats.extend(render::channels(ctx, caller, &others)?);
    Ok(types::messages::ChannelMessages {
        pts: found.pts,
        count: found.messages.len() as i32,
        messages: found.messages,
        chats,
        users: render::users(ctx, caller, &found.users)?,
        ..Default::default()
    }
    .into())
}
