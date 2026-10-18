//! `messages.unpinAllMessages`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{peers, pins};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::UnpinAllMessages,
) -> RpcResult<enums::messages::AffectedHistory> {
    if req.top_msg_id.is_some() || req.saved_peer_id.is_some() {
        return Err(RpcError::not_implemented(
            "unpinning in forum topics and saved messages",
        ));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let (pts, pts_count) = pins::unpin_all(ctx, caller, peer)?;
    Ok(types::messages::AffectedHistory {
        pts,
        pts_count,
        offset: 0,
    }
    .into())
}
