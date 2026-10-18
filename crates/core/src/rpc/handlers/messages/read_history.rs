//! `messages.readHistory`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{dialogs, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::ReadHistory,
) -> RpcResult<enums::messages::AffectedMessages> {
    let reader = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, reader, &req.peer)?;
    let (pts, pts_count) = dialogs::read(ctx, reader, peer, req.max_id)?;
    Ok(types::messages::AffectedMessages { pts, pts_count }.into())
}
