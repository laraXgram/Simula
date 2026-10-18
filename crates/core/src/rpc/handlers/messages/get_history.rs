//! `messages.getHistory`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{dialogs, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetHistory,
) -> RpcResult<enums::messages::Messages> {
    let owner = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, owner, &req.peer)?;
    if !matches!(peer, Peer::User(_)) {
        return Err(RpcError::not_implemented("history of groups and channels"));
    }
    let limit = req.limit.clamp(1, 100) as usize;
    let window = dialogs::Window {
        offset_id: req.offset_id,
        add_offset: req.add_offset,
        limit,
        min_id: req.min_id,
        max_id: req.max_id,
    };
    let history = dialogs::history(ctx, owner, peer, window)?;
    let users = render::users(ctx, owner, &history.users)?;
    Ok(if history.messages.len() < history.count {
        types::messages::MessagesSlice {
            count: history.count as i32,
            messages: history.messages,
            users,
            ..Default::default()
        }
        .into()
    } else {
        types::messages::Messages {
            messages: history.messages,
            topics: vec![],
            chats: vec![],
            users,
        }
        .into()
    })
}
