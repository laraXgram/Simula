//! `messages.deleteMessages`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::deletes;

/// Messages in private chats and basic groups, by id in the caller's box.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::DeleteMessages,
) -> RpcResult<enums::messages::AffectedMessages> {
    let caller = ctx.caller()?.account;
    let (pts, pts_count) = deletes::delete(ctx, caller, &req.id, req.revoke)?;
    Ok(types::messages::AffectedMessages { pts, pts_count }.into())
}
