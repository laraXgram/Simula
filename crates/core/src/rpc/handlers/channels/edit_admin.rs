//! `channels.editAdmin`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

/// Promote, edit or demote (empty rights) an admin.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::channels::EditAdmin) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let channel = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let user = peers::resolve_input_user(ctx, caller, &req.user_id)?;
    let enums::ChatAdminRights::ChatAdminRights(rights) = req.admin_rights;
    channels::edit_admin(ctx, caller, channel, user, *rights, req.rank)?;
    render::own_updates(ctx, caller)
}
