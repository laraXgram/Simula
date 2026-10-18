//! `channels.createChannel`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::render;
use crate::services::channels;

/// A channel (`broadcast`) or supergroup (`megagroup`); the caller becomes its creator.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::CreateChannel,
) -> RpcResult<enums::Updates> {
    if req.forum {
        return Err(RpcError::not_implemented("forums"));
    }
    if req.for_import || req.geo_point.is_some() || req.ttl_period.is_some() {
        return Err(RpcError::not_implemented(
            "imported, location-based and self-destructing supergroups",
        ));
    }
    let caller = ctx.caller()?.account;
    channels::create(ctx, caller, !req.megagroup, &req.title, &req.about)?;
    render::own_updates(ctx, caller)
}
