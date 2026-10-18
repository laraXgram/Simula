//! `channels.editTitle`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

pub fn handle(ctx: &mut Ctx<'_>, req: functions::channels::EditTitle) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channels::rename(ctx, caller, channel, &req.title)?;
    render::own_updates(ctx, caller)
}
