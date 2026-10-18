//! `channels.editBanned`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{channels, peers};

/// Restrict, ban (`view_messages`) or lift restrictions.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::EditBanned,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let Peer::User(user) = peers::resolve_input_peer(ctx, caller, &req.participant)? else {
        return Err(RpcError::not_implemented("banning chats from a supergroup"));
    };
    let enums::ChatBannedRights::ChatBannedRights(rights) = req.banned_rights;
    channels::edit_banned(ctx, caller, channel, user, *rights)?;
    render::own_updates(ctx, caller)
}
