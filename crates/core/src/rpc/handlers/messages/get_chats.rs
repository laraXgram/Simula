//! `messages.getChats`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::render;

/// Basic groups by id, as the caller sees them; unknown ids are left out.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetChats,
) -> RpcResult<enums::messages::Chats> {
    let caller = ctx.caller()?.account;
    let ids = req.id.iter().map(|&id| ChatId(id)).collect();
    Ok(types::messages::Chats {
        chats: render::chats(ctx, caller, &ids)?,
    }
    .into())
}
