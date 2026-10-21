//! `messages.deleteExportedChatInvite`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{invites, peers};

/// Delete a revoked link.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::DeleteExportedChatInvite,
) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    invites::delete(ctx, caller, peer, &req.link)?;
    Ok(true)
}
