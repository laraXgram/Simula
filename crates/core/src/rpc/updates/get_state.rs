//! `updates.getState`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::store;

pub fn handle(ctx: &mut Ctx<'_>, _req: functions::updates::GetState) -> RpcResult<enums::updates::State> {
    let caller = ctx.caller()?;
    let (pts, qts, seq) = store::updates::state(ctx.db, caller.account)?;
    Ok(types::updates::State { pts, qts, date: ctx.now(), seq, unread_count: 0 }.into())
}
