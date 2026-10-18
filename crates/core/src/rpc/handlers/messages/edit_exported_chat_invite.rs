//! `messages.editExportedChatInvite`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{invites, peers};

/// Change or revoke a link; revoking a primary link returns its replacement too.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::EditExportedChatInvite,
) -> RpcResult<enums::messages::ExportedChatInvite> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let (invite, replacement) = invites::edit(
        ctx,
        caller,
        peer,
        &req.link,
        invites::InviteChange {
            revoked: req.revoked,
            expire_date: req.expire_date,
            usage_limit: req.usage_limit,
            request_needed: req.request_needed,
            title: req.title,
        },
    )?;
    let users = render::users(ctx, caller, &BTreeSet::from([invite.admin]))?;
    let rendered = invites::render(ctx, Some(caller), &invite)?;
    Ok(match replacement {
        Some(new) => types::messages::ExportedChatInviteReplaced {
            invite: rendered,
            new_invite: invites::render(ctx, Some(caller), &new)?,
            users,
        }
        .into(),
        None => types::messages::ExportedChatInvite {
            invite: rendered,
            users,
        }
        .into(),
    })
}
