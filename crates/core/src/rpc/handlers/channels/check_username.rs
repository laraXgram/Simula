//! `channels.checkUsername`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{accounts, peers};

/// Whether the username is valid and free (errors say why not).
pub fn handle(ctx: &mut Ctx<'_>, req: functions::channels::CheckUsername) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    if !matches!(req.channel, simula_tl::enums::InputChannel::Empty) {
        peers::resolve_input_channel(ctx, caller, &req.channel)?;
    }
    accounts::check_username(ctx, &req.username)?;
    Ok(true)
}
