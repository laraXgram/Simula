//! `messages.exportChatInvite`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{invites, peers};

/// A new invite link (with `legacy_revoke_permanent`: a new primary link).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::ExportChatInvite,
) -> RpcResult<enums::ExportedChatInvite> {
    if req.subscription_pricing.is_some() {
        return Err(RpcError::not_implemented("paid subscription links"));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let invite = invites::export(
        ctx,
        caller,
        peer,
        invites::NewInvite {
            replace_permanent: req.legacy_revoke_permanent,
            request_needed: req.request_needed,
            expire_date: req.expire_date,
            usage_limit: req.usage_limit,
            title: req.title,
        },
    )?;
    invites::render(ctx, Some(caller), &invite)
}
