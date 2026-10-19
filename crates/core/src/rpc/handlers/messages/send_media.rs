//! `messages.sendMedia`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{media, messages, peers};

/// A photo or document with an optional caption.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::messages::SendMedia) -> RpcResult<enums::Updates> {
    if req.schedule_date.is_some() || req.quick_reply_shortcut.is_some() {
        return Err(RpcError::not_implemented(
            "scheduled and quick-reply messages",
        ));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let media = media::resolve(ctx, caller, &req.media)?;
    let out = messages::Outgoing {
        text: req.message,
        entities: req.entities.unwrap_or_default(),
        reply_markup: req.reply_markup,
        reply_to: req.reply_to,
        random_id: Some(req.random_id),
        silent: req.silent,
        noforwards: req.noforwards,
        invert_media: req.invert_media,
        effect: req.effect,
        media: Some(media),
        ..Default::default()
    };
    let sent = messages::send_to(ctx, caller, peer, out)?;
    messages::sent_result(ctx, caller, vec![(req.random_id, sent)])
}
