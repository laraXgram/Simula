//! `account.checkUsername`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::accounts;

/// Whether the caller may take `username` (`USERNAME_INVALID` when malformed).
pub fn handle(ctx: &mut Ctx<'_>, req: functions::account::CheckUsername) -> RpcResult<bool> {
    let caller = ctx.caller()?.account;
    let mine = ctx.account(caller)?.username.unwrap_or_default();
    if mine.eq_ignore_ascii_case(req.username.trim_start_matches('@')) {
        return Ok(true);
    }
    match accounts::check_username(ctx, &req.username) {
        Ok(()) => Ok(true),
        Err(error) if error.message == "USERNAME_OCCUPIED" => Ok(false),
        Err(error) => Err(error),
    }
}
