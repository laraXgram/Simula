//! `messages.editChatAdmin`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::services::{chats, peers};

/// Make a basic group's member an admin, or a member again (its creator only).
pub fn handle(ctx: &mut Ctx<'_>, req: functions::messages::EditChatAdmin) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let user = peers::resolve_input_user(ctx, caller, &req.user_id)?;
    chats::set_admin(ctx, caller, ChatId(req.chat_id), user, req.is_admin)?;
    Ok(true)
}
