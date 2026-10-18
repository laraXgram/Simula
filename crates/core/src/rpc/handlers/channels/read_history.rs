//! `channels.readHistory`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{channel_messages, peers};

pub fn handle(ctx: &mut Ctx<'_>, req: functions::channels::ReadHistory) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channel_messages::read(ctx, caller, channel, req.max_id)?;
    Ok(true)
}
