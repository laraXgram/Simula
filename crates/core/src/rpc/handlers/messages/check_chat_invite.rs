//! `messages.checkChatInvite`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::invites;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::CheckChatInvite,
) -> RpcResult<enums::ChatInvite> {
    let caller = ctx.caller()?.account;
    invites::check(ctx, caller, &req.hash)
}
