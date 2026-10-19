//! `messages.createForumTopic`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::forums;

/// Open a topic; the result carries its creation message, whose id is the topic's.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::CreateForumTopic,
) -> RpcResult<enums::Updates> {
    if req.send_as.is_some() || req.title_missing {
        return Err(RpcError::not_implemented(
            "topics created on behalf of a chat or without a title",
        ));
    }
    let caller = ctx.caller()?.account;
    let id = forums::resolve(ctx, caller, &req.peer)?;
    forums::create(
        ctx,
        caller,
        id,
        &req.title,
        req.icon_color,
        req.icon_emoji_id,
    )?;
    render::own_updates(ctx, caller)
}
