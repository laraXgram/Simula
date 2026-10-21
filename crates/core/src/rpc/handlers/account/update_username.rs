//! `account.updateUsername`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::accounts::{self, AccountChange};

/// Set or remove ("") the caller's username.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::account::UpdateUsername,
) -> RpcResult<enums::User> {
    let caller = ctx.caller()?.account;
    let current = ctx.account(caller)?;
    if current.is_bot {
        return Err(RpcError::bad_request("BOT_METHOD_INVALID"));
    }
    if current
        .username
        .as_deref()
        .unwrap_or_default()
        .eq_ignore_ascii_case(req.username.trim_start_matches('@'))
    {
        return Err(RpcError::bad_request("USERNAME_NOT_MODIFIED"));
    }
    let account = accounts::update(
        ctx,
        caller,
        AccountChange {
            username: Some(req.username),
            ..Default::default()
        },
    )?;
    Ok(render::user(ctx, &account, &account))
}
