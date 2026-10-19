//! `messages.uploadMedia`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{media, peers};

/// Store an upload as a photo or document without sending it (clients do this before
/// sending albums); the result names it by id.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::UploadMedia,
) -> RpcResult<enums::MessageMedia> {
    if req.business_connection_id.is_some() {
        return Err(RpcError::not_implemented("business connections"));
    }
    let caller = ctx.caller()?.account;
    peers::resolve_input_peer(ctx, caller, &req.peer)?;
    media::resolve(ctx, caller, &req.media)
}
