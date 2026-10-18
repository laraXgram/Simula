//! `messages.importChatInvite`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::invites;

/// Join through a link (or file a join request: `INVITE_REQUEST_SENT`).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::ImportChatInvite,
) -> RpcResult<enums::messages::ChatInviteJoinResult> {
    let caller = ctx.caller()?.account;
    invites::import(ctx, caller, &req.hash)?;
    Ok(types::messages::ChatInviteJoinResultOk {
        updates: render::own_updates(ctx, caller)?,
    }
    .into())
}
