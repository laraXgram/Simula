//! Producing pts-carrying updates: bump the account's pts, log the update, queue it.

use simula_tl::enums;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::UserId;
use crate::store;

/// Record one pts-carrying update for `account`. `build` receives `(pts, pts_count)`.
pub fn push_pts(
    ctx: &mut Ctx<'_>,
    account: UserId,
    mentions: impl IntoIterator<Item = UserId>,
    build: impl FnOnce(i32, i32) -> enums::Update,
) -> RpcResult<enums::Update> {
    let pts = store::updates::advance_pts(ctx.db, account, 1)?;
    let update = build(pts, 1);
    store::updates::log(ctx.db, account, pts, 1, ctx.now(), &update)?;
    let origin = ctx.origin_for(account);
    ctx.out.push(account, update.clone(), mentions, origin);
    Ok(update)
}
