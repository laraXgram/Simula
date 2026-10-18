//! `channels.joinChannel`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

/// Join a public supergroup or channel.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::JoinChannel,
) -> RpcResult<enums::messages::ChatInviteJoinResult> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    channels::join(ctx, caller, channel)?;
    Ok(types::messages::ChatInviteJoinResultOk {
        updates: render::own_updates(ctx, caller)?,
    }
    .into())
}
