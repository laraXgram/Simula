//! `messages.editMessage`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{channel_messages, edits, peers};

/// Text, entities and inline keyboards of messages in private chats.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::EditMessage,
) -> RpcResult<enums::Updates> {
    if req.media.is_some() || req.rich_message.is_some() {
        return Err(RpcError::not_implemented("editing message media"));
    }
    if req.schedule_date.is_some() || req.quick_reply_shortcut_id.is_some() {
        return Err(RpcError::not_implemented(
            "editing scheduled and quick reply messages",
        ));
    }
    let caller = ctx.caller()?;
    let peer = peers::resolve_input_peer(ctx, caller.account, &req.peer)?;
    let edit = edits::Edit {
        text: req.message,
        entities: req.entities.unwrap_or_default(),
        reply_markup: req.reply_markup,
        no_webpage: req.no_webpage,
        invert_media: req.invert_media,
    };
    let edited = match peer {
        Peer::Channel(channel) => {
            channel_messages::edit(ctx, caller.account, channel, req.id, edit)?
        }
        _ => edits::edit(ctx, caller.account, peer, req.id, edit)?,
    };
    let mentioned = ctx.out.users_for(caller.account);
    Ok(types::Updates {
        updates: vec![edited.update],
        users: render::users(ctx, caller.account, &mentioned)?,
        chats: render::all_chats(
            ctx,
            caller.account,
            &ctx.out.chats_for(caller.account),
            &ctx.out.channels_for(caller.account),
        )?,
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
