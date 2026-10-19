//! `messages.deleteTopicHistory`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::forums;

/// Delete a topic and everything in it.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::DeleteTopicHistory,
) -> RpcResult<enums::messages::AffectedHistory> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    let (pts, pts_count) = forums::delete(ctx, caller, id, req.top_msg_id)?;
    Ok(types::messages::AffectedHistory {
        pts,
        pts_count,
        offset: 0,
    }
    .into())
}
