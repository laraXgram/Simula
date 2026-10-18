//! `messages.deleteChatUser`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::render;
use crate::services::{chats, peers};

/// Remove a member, or leave when the user is the caller.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::DeleteChatUser,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let user = peers::resolve_input_user(ctx, caller, &req.user_id)?;
    chats::remove_member(ctx, caller, ChatId(req.chat_id), user)?;
    render::own_updates(ctx, caller)
}
