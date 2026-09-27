//! `bots.resetBotCommands`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::commands;

pub fn handle(ctx: &mut Ctx<'_>, req: functions::bots::ResetBotCommands) -> RpcResult<bool> {
    let bot = ctx.caller()?.account;
    commands::reset(ctx, bot, &req.scope, &req.lang_code)?;
    Ok(true)
}
