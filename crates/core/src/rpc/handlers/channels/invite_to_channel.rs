//! `channels.inviteToChannel`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::InviteToChannel,
) -> RpcResult<enums::messages::InvitedUsers> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let users = req
        .users
        .iter()
        .map(|input| peers::resolve_input_user(ctx, caller, input))
        .collect::<RpcResult<Vec<_>>>()?;
    channels::invite(ctx, caller, channel, &users)?;
    Ok(types::messages::InvitedUsers {
        updates: render::own_updates(ctx, caller)?,
        missing_invitees: vec![],
    }
    .into())
}
