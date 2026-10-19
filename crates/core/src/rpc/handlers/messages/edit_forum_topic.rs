//! `messages.editForumTopic`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::forums;

/// Rename, change the icon of, close or reopen a topic, or hide General.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::EditForumTopic,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    forums::edit(
        ctx,
        caller,
        id,
        req.topic_id,
        forums::Edit {
            title: req.title,
            icon_emoji_id: req.icon_emoji_id,
            closed: req.closed,
            hidden: req.hidden,
        },
    )?;
    render::own_updates(ctx, caller)
}
