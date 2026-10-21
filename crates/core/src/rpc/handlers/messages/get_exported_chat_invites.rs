//! `messages.getExportedChatInvites`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{invites, peers};

/// An admin's links, live or revoked, newest first (after `offset_link`).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetExportedChatInvites,
) -> RpcResult<enums::messages::ExportedChatInvites> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let admin = peers::resolve_input_user(ctx, caller, &req.admin_id)?;
    let all = invites::list(ctx, caller, peer, admin, req.revoked)?;
    let start = match req.offset_link.as_deref() {
        Some(link) if !link.is_empty() => {
            let hash = invites::hash_of(link);
            all.iter()
                .position(|i| i.hash == hash)
                .map_or(all.len(), |i| i + 1)
        }
        _ => 0,
    };
    let limit = req.limit.clamp(1, 100) as usize;
    let mut rendered = Vec::new();
    for invite in all.iter().skip(start).take(limit) {
        rendered.push(invites::render(ctx, Some(caller), invite)?);
    }
    let users: BTreeSet<_> = [admin].into();
    Ok(types::messages::ExportedChatInvites {
        count: all.len() as i32,
        invites: rendered,
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
