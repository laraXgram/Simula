//! `bots.getBotCommands`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::commands;

/// The list set for exactly this scope and language (no fallback), as on Telegram.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::bots::GetBotCommands,
) -> RpcResult<Vec<enums::BotCommand>> {
    let bot = ctx.caller()?.account;
    commands::get(ctx, bot, &req.scope, &req.lang_code)
}
