//! `messages.sendMultiMedia`

use simula_tl::{enums, functions};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{media, messages, peers};

/// At most ten media in one album, each its own message.
const MAX_ALBUM: usize = 10;

/// An album: photos and videos together, or documents, or audio files, up to ten, each
/// a message with its own caption, sharing a `grouped_id` and the reply.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::SendMultiMedia,
) -> RpcResult<enums::Updates> {
    if req.schedule_date.is_some() || req.quick_reply_shortcut.is_some() {
        return Err(RpcError::not_implemented(
            "scheduled and quick-reply messages",
        ));
    }
    if req.multi_media.is_empty() {
        return Err(RpcError::bad_request("MEDIA_EMPTY"));
    }
    if req.multi_media.len() > MAX_ALBUM {
        return Err(RpcError::bad_request("MULTI_MEDIA_TOO_LONG"));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let mut items = Vec::with_capacity(req.multi_media.len());
    let mut kind = None;
    for single in &req.multi_media {
        let enums::InputSingleMedia::InputSingleMedia(single) = single;
        let resolved = media::resolve(ctx, caller, &single.media)?;
        let this =
            media::album_kind(&resolved).ok_or_else(|| RpcError::bad_request("MEDIA_INVALID"))?;
        if kind.is_some_and(|k| k != this) {
            return Err(RpcError::bad_request("MEDIA_INVALID"));
        }
        kind = Some(this);
        items.push((single.clone(), resolved));
    }
    let grouped_id = rand::random::<i64>() & i64::MAX;
    let mut sent = Vec::with_capacity(items.len());
    for (single, resolved) in items {
        let out = messages::Outgoing {
            text: single.message,
            entities: single.entities.unwrap_or_default(),
            reply_to: req.reply_to.clone(),
            random_id: Some(single.random_id),
            silent: req.silent,
            noforwards: req.noforwards,
            invert_media: req.invert_media,
            effect: req.effect,
            media: Some(resolved),
            grouped_id: Some(grouped_id),
            ..Default::default()
        };
        sent.push((single.random_id, messages::send_to(ctx, caller, peer, out)?));
    }
    messages::sent_result(ctx, caller, sent)
}
