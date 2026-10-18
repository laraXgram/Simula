//! `channels.deleteMessages`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{channel_messages, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::DeleteMessages,
) -> RpcResult<enums::messages::AffectedMessages> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let (pts, pts_count) = channel_messages::delete(ctx, caller, channel, &req.id)?;
    Ok(types::messages::AffectedMessages { pts, pts_count }.into())
}
