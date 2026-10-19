//! `messages.unpinAllMessages`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{peers, pins};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::UnpinAllMessages,
) -> RpcResult<enums::messages::AffectedHistory> {
    if req.saved_peer_id.is_some() {
        return Err(RpcError::not_implemented("unpinning in saved messages"));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let (pts, pts_count) = pins::unpin_all(ctx, caller, peer, req.top_msg_id)?;
    Ok(types::messages::AffectedHistory {
        pts,
        pts_count,
        offset: 0,
    }
    .into())
}
