//! `messages.addChatUser`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::render;
use crate::services::{chats, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::AddChatUser,
) -> RpcResult<enums::messages::InvitedUsers> {
    let caller = ctx.caller()?.account;
    let user = peers::resolve_input_user(ctx, caller, &req.user_id)?;
    chats::add_member(ctx, caller, ChatId(req.chat_id), user)?;
    Ok(types::messages::InvitedUsers {
        updates: render::own_updates(ctx, caller)?,
        missing_invitees: vec![],
    }
    .into())
}
