//! Deleting messages from per-account boxes. With `revoke`, the other side's copies go
//! too. Every box that loses messages gets `updateDeleteMessages` with its own pts.

use std::collections::BTreeMap;

use simula_tl::types;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::updates;
use crate::store;

/// Bots may delete messages only within 48 hours of sending.
const BOT_DELETE_TIME_LIMIT: i32 = 2 * 86400;

/// Returns the caller's `(pts, pts_count)` after the deletion.
pub fn delete(
    ctx: &mut Ctx<'_>,
    caller: UserId,
    ids: &[i32],
    revoke: bool,
) -> RpcResult<(i32, i32)> {
    let is_bot = ctx.account(caller)?.is_bot;
    let mut by_owner: BTreeMap<UserId, Vec<i32>> = BTreeMap::new();
    for &id in ids {
        // Unknown and already deleted ids are ignored, as on Telegram.
        let Some((global_id, message)) = store::messages::get_with_global(ctx.db, caller, id)?
        else {
            continue;
        };
        if matches!(Peer::from_tl(&message.peer_id), Peer::Channel(_)) {
            return Err(RpcError::not_implemented("deleting channel messages"));
        }
        if is_bot && ctx.now() - message.date > BOT_DELETE_TIME_LIMIT {
            return Err(RpcError::bad_request("MESSAGE_DELETE_FORBIDDEN"));
        }
        for (owner, copy_id) in store::messages::copies(ctx.db, global_id)? {
            if owner == caller || revoke {
                by_owner.entry(owner).or_default().push(copy_id);
            }
        }
    }

    let mut affected = None;
    for (owner, ids) in by_owner {
        for &id in &ids {
            store::messages::delete(ctx.db, owner, id)?;
        }
        let count = ids.len() as i32;
        let update = updates::push_pts_count(ctx, owner, count, [], |pts, pts_count| {
            types::UpdateDeleteMessages {
                messages: ids,
                pts,
                pts_count,
            }
            .into()
        })?;
        if owner == caller
            && let simula_tl::enums::Update::DeleteMessages(u) = update
        {
            affected = Some((u.pts, u.pts_count));
        }
    }
    match affected {
        Some(affected) => Ok(affected),
        None => Ok((store::updates::state(ctx.db, caller)?.0, 0)),
    }
}
