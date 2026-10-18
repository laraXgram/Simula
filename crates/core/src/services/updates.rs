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
    push_pts_count(ctx, account, 1, mentions, build)
}

/// [`push_pts`] for an update that counts as `count` events (deleting several messages).
pub fn push_pts_count(
    ctx: &mut Ctx<'_>,
    account: UserId,
    count: i32,
    mentions: impl IntoIterator<Item = UserId>,
    build: impl FnOnce(i32, i32) -> enums::Update,
) -> RpcResult<enums::Update> {
    let origin = ctx.origin_for(account);
    push_pts_from(ctx, account, count, mentions, origin, build)
}

/// [`push_pts`] for every session of `account`, including the caller's: for updates
/// that aren't part of the call's result (a bot reading the message just sent to it).
pub fn push_pts_to_all(
    ctx: &mut Ctx<'_>,
    account: UserId,
    mentions: impl IntoIterator<Item = UserId>,
    build: impl FnOnce(i32, i32) -> enums::Update,
) -> RpcResult<enums::Update> {
    push_pts_from(ctx, account, 1, mentions, None, build)
}

fn push_pts_from(
    ctx: &mut Ctx<'_>,
    account: UserId,
    count: i32,
    mentions: impl IntoIterator<Item = UserId>,
    origin: Option<crate::ctx::SessionId>,
    build: impl FnOnce(i32, i32) -> enums::Update,
) -> RpcResult<enums::Update> {
    let pts = store::updates::advance_pts(ctx.db, account, count)?;
    let update = build(pts, count);
    store::updates::log(ctx.db, account, pts, count, ctx.now(), &update)?;
    ctx.out.push(account, update.clone(), mentions, origin);
    Ok(update)
}
