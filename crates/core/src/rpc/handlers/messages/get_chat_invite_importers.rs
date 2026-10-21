//! `messages.getChatInviteImporters`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::UserId;
use crate::render;
use crate::services::{invites, peers};

/// Pending join requests (`requested`), or who joined through `link`.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetChatInviteImporters,
) -> RpcResult<enums::messages::ChatInviteImporters> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let all = invites::importers(
        ctx,
        caller,
        peer,
        req.requested,
        req.link.as_deref(),
        req.q.as_deref(),
    )?;
    let limit = req.limit.clamp(1, 100) as usize;
    let page: Vec<_> = all.iter().take(limit).cloned().collect();
    let users: BTreeSet<UserId> = page
        .iter()
        .flat_map(|i| [Some(UserId(i.user_id)), i.approved_by.map(UserId)])
        .flatten()
        .collect();
    Ok(types::messages::ChatInviteImporters {
        count: all.len() as i32,
        importers: page.into_iter().map(Into::into).collect(),
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
