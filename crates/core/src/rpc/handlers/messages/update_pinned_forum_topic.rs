//! `messages.updatePinnedForumTopic`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::forums;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::UpdatePinnedForumTopic,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    forums::pin(ctx, caller, id, req.topic_id, req.pinned)?;
    render::own_updates(ctx, caller)
}
