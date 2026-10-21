//! `channels.deleteChannel`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

/// Delete a supergroup or channel for everyone (its creator only).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::DeleteChannel,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channels::delete(ctx, caller, id)?;
    render::own_updates(ctx, caller)
}
