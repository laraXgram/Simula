//! `channels.toggleForum`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::{forums, peers};

/// Turn a supergroup into a forum or back (only its creator may).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::ToggleForum,
) -> RpcResult<enums::Updates> {
    if req.tabs {
        return Err(RpcError::not_implemented("forums shown as tabs"));
    }
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    forums::toggle(ctx, caller, id, req.enabled)?;
    render::own_updates(ctx, caller)
}
