//! `messages.getHistory`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::Peer;
use crate::render;
use crate::services::{channel_messages, channels, dialogs, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetHistory,
) -> RpcResult<enums::messages::Messages> {
    let owner = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, owner, &req.peer)?;
    let limit = req.limit.clamp(1, 100) as usize;
    let window = dialogs::Window {
        offset_id: req.offset_id,
        add_offset: req.add_offset,
        limit,
        min_id: req.min_id,
        max_id: req.max_id,
    };
    if let Peer::Channel(channel) = peer {
        let history = channel_messages::history(ctx, owner, channel, window)?;
        let found = channels::get(ctx, channel)?;
        return Ok(types::messages::ChannelMessages {
            pts: history.pts,
            count: history.count as i32,
            messages: history.messages,
            chats: vec![render::channel(ctx, owner, &found)?],
            users: render::users(ctx, owner, &history.users)?,
            ..Default::default()
        }
        .into());
    }
    let history = dialogs::history(ctx, owner, peer, window)?;
    let users = render::users(ctx, owner, &history.users)?;
    let chats = match peer {
        Peer::Chat(chat) => render::chats(ctx, owner, &[chat].into())?,
        _ => vec![],
    };
    Ok(if history.messages.len() < history.count {
        types::messages::MessagesSlice {
            count: history.count as i32,
            messages: history.messages,
            users,
            chats: chats.clone(),
            ..Default::default()
        }
        .into()
    } else {
        types::messages::Messages {
            messages: history.messages,
            topics: vec![],
            chats,
            users,
        }
        .into()
    })
}
