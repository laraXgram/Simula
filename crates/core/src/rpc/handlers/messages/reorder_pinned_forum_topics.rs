//! `messages.reorderPinnedForumTopics`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::forums;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::ReorderPinnedForumTopics,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    forums::reorder(ctx, caller, id, &req.order, req.force)?;
    render::own_updates(ctx, caller)
}
