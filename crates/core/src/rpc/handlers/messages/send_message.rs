//! `messages.sendMessage`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::{messages, peers};

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::SendMessage,
) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let out = messages::Outgoing {
        text: req.message,
        entities: req.entities.unwrap_or_default(),
        reply_markup: req.reply_markup,
        reply_to: req.reply_to,
        random_id: Some(req.random_id),
        silent: req.silent,
        noforwards: req.noforwards,
        no_webpage: req.no_webpage,
        invert_media: req.invert_media,
        effect: req.effect,
        ..Default::default()
    };
    let sent = messages::send_to(ctx, caller, peer, out)?;
    // The caller learns the new id through updateMessageID, then gets its own copy.
    messages::sent_result(ctx, caller, vec![(req.random_id, sent)])
}
