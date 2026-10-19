//! `messages.getReplies`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{channel_messages, channels, dialogs, messages, peers};
use crate::store;

/// A page of a thread: a forum topic's messages (`msg_id` is the topic), or the replies
/// in a supergroup's reply chain starting at `msg_id`.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetReplies,
) -> RpcResult<enums::messages::Messages> {
    let caller = ctx.caller()?.account;
    let Peer::Channel(id) = peers::resolve_input_peer(ctx, caller, &req.peer)? else {
        return Err(RpcError::bad_request("PEER_ID_INVALID"));
    };
    let channel = channels::get(ctx, id)?;
    channels::require_reader(ctx, &channel, caller)?;
    if channel.broadcast {
        return Err(RpcError::bad_request("MSG_ID_INVALID"));
    }
    let window = dialogs::Window {
        offset_id: req.offset_id,
        add_offset: req.add_offset,
        limit: req.limit.clamp(1, 100) as usize,
        min_id: req.min_id,
        max_id: req.max_id,
    };
    let (page, count) = window.page(store::topics::message_ids(ctx.db, id, req.msg_id)?);
    let mut found = Vec::with_capacity(page.len());
    let mut users = std::collections::BTreeSet::new();
    let mut others = std::collections::BTreeSet::new();
    for message_id in page {
        if let Some(stored) = store::channels::message(ctx.db, id, message_id)? {
            let message =
                channel_messages::as_seen_by(&stored.message, stored.author, Some(caller));
            users.extend(messages::users_any(&message));
            others.extend(messages::channels_any(&message));
            found.push(message);
        }
    }
    others.remove(&id);
    let mut chats = vec![render::channel(ctx, caller, &channel)?];
    chats.extend(render::channels(ctx, caller, &others)?);
    Ok(types::messages::ChannelMessages {
        pts: channel.pts,
        count: count as i32,
        messages: found,
        chats,
        users: render::users(ctx, caller, &users)?,
        ..Default::default()
    }
    .into())
}
