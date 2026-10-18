//! `messages.updatePinnedMessage`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::{peers, pins};

/// Pin or unpin a message in any chat.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::UpdatePinnedMessage,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    if req.id <= 0 {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    }
    pins::pin(
        ctx,
        caller,
        peer,
        req.id,
        pins::Pin {
            unpin: req.unpin,
            silent: req.silent,
            one_side: req.pm_oneside,
        },
    )?;
    render::own_updates(ctx, caller)
}
