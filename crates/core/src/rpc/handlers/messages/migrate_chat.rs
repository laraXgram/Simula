//! `messages.migrateChat`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::ChatId;
use crate::render;
use crate::services::upgrades;

/// Upgrade a basic group to a supergroup; only its creator may.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::MigrateChat,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    upgrades::migrate(ctx, caller, ChatId(req.chat_id))?;
    render::own_updates(ctx, caller)
}
