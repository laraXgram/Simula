//! Producing pts-carrying updates: bump the account's pts, log the update, queue it.

use simula_tl::enums;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::{ChannelId, UserId};
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

/// Record one qts-carrying update for `account` (bots' membership updates).
/// `build` receives the new qts.
pub fn push_qts(
    ctx: &mut Ctx<'_>,
    account: UserId,
    mentions: impl IntoIterator<Item = UserId>,
    build: impl FnOnce(i32) -> enums::Update,
) -> RpcResult<enums::Update> {
    let qts = store::updates::advance_qts(ctx.db, account)?;
    let update = build(qts);
    ctx.out.push(account, update.clone(), mentions, None);
    Ok(update)
}

/// Record one pts-carrying update of `channel` (its pts advances by `count`, once for
/// everyone) and queue it for each of `recipients`, built as that recipient sees it.
/// Returns the new pts.
pub fn push_channel(
    ctx: &mut Ctx<'_>,
    channel: ChannelId,
    count: i32,
    recipients: &[UserId],
    mentions: &[UserId],
    build: impl Fn(Option<UserId>, i32, i32) -> enums::Update,
) -> RpcResult<i32> {
    let pts = store::channels::advance_pts(ctx.db, channel, count)?;
    store::channels::log_update(
        ctx.db,
        channel,
        pts,
        count,
        ctx.now(),
        &build(None, pts, count),
    )?;
    for &recipient in recipients {
        let origin = ctx.origin_for(recipient);
        ctx.out.mention_channel(recipient, channel);
        ctx.out.push(
            recipient,
            build(Some(recipient), pts, count),
            mentions.iter().copied(),
            origin,
        );
    }
    Ok(pts)
}

/// Queue an update that carries no pts (`updateChannel`, read receipts) for every
/// session of `account` but the caller's own, when the caller is `account`.
pub fn push_plain(
    ctx: &mut Ctx<'_>,
    account: UserId,
    update: enums::Update,
    mentions: impl IntoIterator<Item = UserId>,
) {
    let origin = ctx.origin_for(account);
    ctx.out.push(account, update, mentions, origin);
}
