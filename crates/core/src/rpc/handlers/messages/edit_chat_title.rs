//! `messages.editChatTitle`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::render;
use crate::services::chats;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::EditChatTitle,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    chats::rename(ctx, caller, ChatId(req.chat_id), &req.title)?;
    render::own_updates(ctx, caller)
}
