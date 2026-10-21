//! `account.updateProfile`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::accounts::{self, AccountChange};

/// Change the caller's name or bio; everyone who knows them learns the new name.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::account::UpdateProfile) -> RpcResult<enums::User> {
    let caller = ctx.caller()?.account;
    let account = accounts::update(
        ctx,
        caller,
        AccountChange {
            first_name: req.first_name,
            last_name: req.last_name,
            about: req.about,
            ..Default::default()
        },
    )?;
    Ok(render::user(ctx, &account, &account))
}
