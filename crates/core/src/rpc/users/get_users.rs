//! `users.getUsers`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::peers;

/// Unknown or inaccessible users are left out.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::users::GetUsers) -> RpcResult<Vec<enums::User>> {
    let caller = ctx.caller()?;
    let viewer = ctx.account(caller.account)?;
    let mut users = Vec::with_capacity(req.id.len());
    for input in &req.id {
        if let Ok(id) = peers::resolve_input_user(ctx, caller.account, input) {
            users.push(render::user(ctx, &viewer, &ctx.account(id)?));
        }
    }
    Ok(users)
}
