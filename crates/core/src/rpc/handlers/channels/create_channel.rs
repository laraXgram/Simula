//! `channels.createChannel`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::{channels, forums};

/// A channel (`broadcast`) or supergroup (`megagroup`, a forum with `forum`); the caller
/// becomes its creator.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::CreateChannel,
) -> RpcResult<enums::Updates> {
    if req.forum && !req.megagroup {
        return Err(RpcError::bad_request("CHANNEL_INVALID"));
    }
    if req.for_import || req.geo_point.is_some() || req.ttl_period.is_some() {
        return Err(RpcError::not_implemented(
            "imported, location-based and self-destructing supergroups",
        ));
    }
    let caller = ctx.caller()?.account;
    let id = channels::create(ctx, caller, !req.megagroup, &req.title, &req.about)?;
    if req.forum {
        forums::set_forum(ctx, &channels::get(ctx, id)?, true)?;
    }
    render::own_updates(ctx, caller)
}
