//! Pinned messages. Pinning marks every copy (`pinned`), tells each box with
//! `updatePinnedMessages` (or the channel with `updatePinnedChannelMessages`), and posts
//! a "pinned a message" notice replying to it, unless only pinned for oneself in a
//! private chat. Unpinning posts nothing.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{channels, chats, forums, rights, service_messages, updates};
use crate::store::{self, chats::Role};

pub struct Pin {
    pub unpin: bool,
    /// No notification for the notice (Simula doesn't notify, so nothing changes).
    pub silent: bool,
    /// Private chats: pin only in the caller's own box.
    pub one_side: bool,
}

pub fn pin(ctx: &mut Ctx<'_>, actor: UserId, peer: Peer, id: i32, pin: Pin) -> RpcResult<()> {
    match peer {
        Peer::Channel(channel) => pin_in_channel(ctx, actor, channel, id, &pin),
        _ => pin_in_boxes(ctx, actor, peer, id, &pin),
    }
}

/// Unpin everything in the dialog (or in one forum topic); returns the caller's
/// `(pts, pts_count)`.
pub fn unpin_all(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    peer: Peer,
    topic: Option<i32>,
) -> RpcResult<(i32, i32)> {
    let ids = match peer {
        Peer::Channel(channel) => {
            let found = channels::get(ctx, channel)?;
            check_channel_right(ctx, &found, actor)?;
            let mut ids = store::channels::pinned_ids(ctx.db, channel)?;
            if let Some(topic) = topic {
                if !forums::is_forum(&found) {
                    return Err(RpcError::bad_request("CHANNEL_FORUM_MISSING"));
                }
                let mut kept = Vec::new();
                for id in ids {
                    let thread = store::topics::thread_of(ctx.db, channel, id)?;
                    if thread.unwrap_or(forums::GENERAL) == topic {
                        kept.push(id);
                    }
                }
                ids = kept;
            }
            ids
        }
        _ if topic.is_some() => return Err(RpcError::bad_request("CHANNEL_FORUM_MISSING")),
        _ => {
            check_box_right(ctx, actor, peer)?;
            store::messages::pinned_ids(ctx.db, actor, peer)?
        }
    };
    let before = state_pts(ctx, actor, peer)?;
    for id in ids.iter().rev() {
        pin(
            ctx,
            actor,
            peer,
            *id,
            Pin {
                unpin: true,
                silent: true,
                one_side: false,
            },
        )?;
    }
    let after = state_pts(ctx, actor, peer)?;
    Ok((after, after - before))
}

fn state_pts(ctx: &Ctx<'_>, actor: UserId, peer: Peer) -> RpcResult<i32> {
    Ok(match peer {
        Peer::Channel(channel) => channels::get(ctx, channel)?.pts,
        _ => store::updates::state(ctx.db, actor)?.0,
    })
}

/// Private chats: both sides may pin. Basic groups: admins, and members unless the
/// group's defaults forbid it.
fn check_box_right(ctx: &Ctx<'_>, actor: UserId, peer: Peer) -> RpcResult<()> {
    if let Peer::Chat(chat) = peer {
        let found = chats::get(ctx, chat)?;
        let member = chats::require_member(ctx, chat, actor)?;
        if member.role == Role::Member && chats::default_banned(&found).pin_messages {
            return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
        }
    }
    Ok(())
}

fn pin_in_boxes(ctx: &mut Ctx<'_>, actor: UserId, peer: Peer, id: i32, pin: &Pin) -> RpcResult<()> {
    check_box_right(ctx, actor, peer)?;
    let Some((global_id, _)) = store::messages::get_in(ctx.db, actor, peer, id)? else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    let one_side = pin.one_side && matches!(peer, Peer::User(_));
    let mut copies = Vec::new();
    for (owner, copy_id) in store::messages::copies(ctx.db, global_id)? {
        if one_side && owner != actor {
            continue;
        }
        let Some(mut copy) = store::messages::get(ctx.db, owner, copy_id)? else {
            continue;
        };
        copy.pinned = !pin.unpin;
        store::messages::update(ctx.db, owner, &copy)?;
        let owner_peer = Peer::from_tl(&copy.peer_id);
        if let Peer::Chat(chat) = owner_peer {
            ctx.out.mention_chat(owner, chat);
        }
        updates::push_pts(ctx, owner, [actor], |pts, pts_count| {
            types::UpdatePinnedMessages {
                pinned: !pin.unpin,
                peer: owner_peer.to_tl(),
                messages: vec![copy_id],
                pts,
                pts_count,
            }
            .into()
        })?;
        copies.push((owner, owner_peer));
    }
    if !pin.unpin && !one_side {
        service_messages::post(
            ctx,
            actor,
            &copies,
            types::MessageActionPinMessage {}.into(),
            Some(global_id),
        )?;
    }
    Ok(())
}

/// Channels: admins with `pin_messages` (or `edit_messages` in channels); supergroup
/// members when the defaults allow it.
fn check_channel_right(
    ctx: &Ctx<'_>,
    channel: &store::channels::Channel,
    actor: UserId,
) -> RpcResult<()> {
    let member = channels::require_member(ctx, channel, actor)?;
    let allowed = rights::can(&member, |r| {
        r.pin_messages || (channel.broadcast && r.edit_messages)
    }) || (!channel.broadcast
        && !member.status.is_admin()
        && !rights::restricted(ctx, channel, &member, |r| r.pin_messages));
    if !allowed {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    Ok(())
}

fn pin_in_channel(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    message_id: i32,
    pin: &Pin,
) -> RpcResult<()> {
    let channel = channels::get(ctx, id)?;
    check_channel_right(ctx, &channel, actor)?;
    let Some(store::channels::Stored {
        message: enums::Message::Message(mut message),
        ..
    }) = store::channels::message(ctx.db, id, message_id)?
    else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    message.pinned = !pin.unpin;
    store::channels::update_message(ctx.db, id, &message)?;
    let recipients: Vec<UserId> = store::channels::current_members(ctx.db, id)?
        .into_iter()
        .map(|m| m.user)
        .collect();
    updates::push_channel(ctx, id, 1, &recipients, &[actor], |_, pts, pts_count| {
        types::UpdatePinnedChannelMessages {
            pinned: !pin.unpin,
            channel_id: id.0,
            messages: vec![message_id],
            pts,
            pts_count,
        }
        .into()
    })?;
    if !pin.unpin {
        // The notice goes where the pinned message is (its forum topic or thread).
        let (forum_topic, top, _) = forums::place_reply(ctx, &channel, message_id, None)?;
        let header = types::MessageReplyHeader {
            forum_topic,
            reply_to_msg_id: Some(message_id),
            reply_to_top_id: top,
            ..Default::default()
        };
        channels::post_service_with(
            ctx,
            &channel,
            actor,
            types::MessageActionPinMessage {}.into(),
            &[],
            Some(header.into()),
        )?;
    }
    Ok(())
}
