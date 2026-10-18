//! `channels.updateUsername`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{channels, peers};

/// Set or remove (`""`) the public username, which makes the channel public.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::channels::UpdateUsername) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channels::set_username(ctx, caller, channel, &req.username)?;
    Ok(true)
}
