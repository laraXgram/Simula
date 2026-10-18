//! `messages.createChat`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{chats, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::CreateChat,
) -> RpcResult<enums::messages::InvitedUsers> {
    let caller = ctx.caller()?.account;
    let users = req
        .users
        .iter()
        .map(|input| peers::resolve_input_user(ctx, caller, input))
        .collect::<RpcResult<Vec<_>>>()?;
    chats::create(ctx, caller, &req.title, &users)?;
    Ok(types::messages::InvitedUsers {
        updates: render::own_updates(ctx, caller)?,
        missing_invitees: vec![],
    }
    .into())
}
