//! `messages.deleteRevokedExportedChatInvites`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{invites, peers};

/// Delete all of an admin's revoked links.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::DeleteRevokedExportedChatInvites,
) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let admin = peers::resolve_input_user(ctx, caller, &req.admin_id)?;
    invites::delete_revoked(ctx, caller, peer, admin)?;
    Ok(true)
}
