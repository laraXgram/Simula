//! `channels.getChannels`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::render;
use crate::services::{channels, peers};

/// Channels by `InputChannel`, as the caller sees them (deleted ones as
/// `channelForbidden`).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::GetChannels,
) -> RpcResult<enums::messages::Chats> {
    let caller = ctx.caller()?.account;
    let mut ids = BTreeSet::new();
    for input in &req.id {
        let id = peers::resolve_input_channel(ctx, caller, input)?;
        if crate::store::channels::get(ctx.db, id)?.is_none() {
            channels::get(ctx, id)?;
        }
        ids.insert(id);
    }
    Ok(types::messages::Chats {
        chats: render::channels(ctx, caller, &ids)?,
    }
    .into())
}
