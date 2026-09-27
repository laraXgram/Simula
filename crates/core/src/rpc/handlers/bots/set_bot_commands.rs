//! `bots.setBotCommands`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::commands;

pub fn handle(ctx: &mut Ctx<'_>, req: functions::bots::SetBotCommands) -> RpcResult<bool> {
    let bot = ctx.caller()?.account;
    commands::set(ctx, bot, &req.scope, &req.lang_code, req.commands)?;
    Ok(true)
}
