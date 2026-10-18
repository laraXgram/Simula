//! `channels.leaveChannel`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::LeaveChannel,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channels::leave(ctx, caller, channel)?;
    render::own_updates(ctx, caller)
}
