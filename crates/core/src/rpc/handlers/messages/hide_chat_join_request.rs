//! `messages.hideChatJoinRequest`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{invites, peers};

/// Approve or decline a join request.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::HideChatJoinRequest,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let user = peers::resolve_input_user(ctx, caller, &req.user_id)?;
    invites::hide_request(ctx, caller, peer, user, req.approved)?;
    render::own_updates(ctx, caller)
}
