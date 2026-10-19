//! Dialogs as clients see them: the chat list with unread counts, a dialog's history
//! in pages, and reading (each side gets its read-history update with its own pts).

use std::collections::BTreeSet;

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, ChatId, Peer, UserId};
use crate::services::{channel_messages, messages, updates};
use crate::store;

pub struct DialogList {
    pub dialogs: Vec<enums::Dialog>,
    pub messages: Vec<enums::Message>,
    pub users: BTreeSet<UserId>,
    pub chats: BTreeSet<ChatId>,
    pub channels: BTreeSet<ChannelId>,
    /// All dialogs, when this page holds fewer.
    pub total: usize,
}

/// Dialogs older than `(offset_date, offset_id)` (0 = from the newest), at most `limit`.
pub fn list(
    ctx: &Ctx<'_>,
    owner: UserId,
    offset_date: i32,
    offset_id: i32,
    limit: usize,
) -> RpcResult<DialogList> {
    let mut all = store::dialogs::list(ctx.db, owner)?;
    // Supergroups and channels the owner is in, by their newest message.
    for (channel, status) in store::channels::of_user(ctx.db, owner)? {
        if !status.is_member() {
            continue;
        }
        let (top, date) = match store::channels::top(ctx.db, channel)? {
            Some(top) => top,
            None => (
                0,
                store::channels::get(ctx.db, channel)?.map_or(0, |c| c.date),
            ),
        };
        all.push((Peer::Channel(channel), top, date));
    }
    all.sort_by_key(|&(_, id, date)| std::cmp::Reverse((date, id)));
    let total = all.len();
    let mut out = DialogList {
        dialogs: Vec::new(),
        messages: Vec::new(),
        users: BTreeSet::new(),
        chats: BTreeSet::new(),
        channels: BTreeSet::new(),
        total,
    };
    let page = all.into_iter().filter(|&(_, id, date)| {
        offset_date == 0 || date < offset_date || (date == offset_date && id < offset_id)
    });
    for (peer, top, _) in page.take(limit) {
        let (read_inbox, read_outbox) = store::dialogs::read_state(ctx.db, owner, peer)?;
        if let Peer::Channel(channel) = peer {
            let pts = store::channels::get(ctx.db, channel)?.map_or(0, |c| c.pts);
            out.dialogs.push(
                types::Dialog {
                    peer: peer.to_tl(),
                    top_message: top,
                    read_inbox_max_id: read_inbox,
                    read_outbox_max_id: read_outbox,
                    unread_count: store::channels::unread_count(
                        ctx.db, channel, owner, read_inbox,
                    )?,
                    notify_settings: types::PeerNotifySettings::default().into(),
                    pts: Some(pts),
                    ..Default::default()
                }
                .into(),
            );
            if let Some(stored) = store::channels::message(ctx.db, channel, top)? {
                let message =
                    channel_messages::as_seen_by(&stored.message, stored.author, Some(owner));
                out.users.extend(messages::users_any(&message));
                out.channels.extend(messages::channels_any(&message));
                out.messages.push(message);
            }
            out.channels.insert(channel);
            continue;
        }
        out.dialogs.push(
            types::Dialog {
                peer: peer.to_tl(),
                top_message: top,
                read_inbox_max_id: read_inbox,
                read_outbox_max_id: read_outbox,
                unread_count: store::dialogs::unread_count(ctx.db, owner, peer, read_inbox)?,
                notify_settings: types::PeerNotifySettings::default().into(),
                ..Default::default()
            }
            .into(),
        );
        if let Some(message) = store::messages::get_any(ctx.db, owner, top)? {
            out.users.extend(messages::users_any(&message));
            out.channels.extend(messages::channels_any(&message));
            out.messages.push(message);
        }
        if let Peer::Chat(chat) = peer {
            out.chats.insert(chat);
        }
    }
    Ok(out)
}

/// Which page of a history (see [`Window::page`]).
#[derive(Debug, Clone, Copy, Default)]
pub struct Window {
    pub offset_id: i32,
    pub add_offset: i32,
    pub limit: usize,
    pub min_id: i32,
    pub max_id: i32,
}

pub struct History {
    pub messages: Vec<enums::Message>,
    pub users: BTreeSet<UserId>,
    /// Supergroups and channels the messages refer to.
    pub channels: BTreeSet<ChannelId>,
    /// Messages in the dialog within the id bounds.
    pub count: usize,
}

impl Window {
    /// `messages.getHistory`'s window over `ids` (newest first): starting below
    /// `offset_id` (0 = the newest), shifted by `add_offset` (negative reaches newer
    /// messages), within `min_id < id < max_id` when those are set. Returns the page and
    /// how many ids are within the bounds.
    pub fn page(&self, ids: Vec<i32>) -> (Vec<i32>, usize) {
        let ids: Vec<i32> = ids
            .into_iter()
            .filter(|&id| {
                (self.min_id <= 0 || id > self.min_id) && (self.max_id <= 0 || id < self.max_id)
            })
            .collect();
        let first_below = if self.offset_id > 0 {
            ids.iter()
                .position(|&id| id < self.offset_id)
                .unwrap_or(ids.len())
        } else {
            0
        };
        let start = (first_below as i64 + i64::from(self.add_offset)).max(0) as usize;
        let end = (start + self.limit).min(ids.len());
        let page = ids.get(start..end).unwrap_or_default().to_vec();
        (page, ids.len())
    }
}

/// A page of `owner`'s history with `peer` (see [`Window::page`]).
pub fn history(ctx: &Ctx<'_>, owner: UserId, peer: Peer, window: Window) -> RpcResult<History> {
    let (page, count) = window.page(store::dialogs::message_ids(ctx.db, owner, peer)?);
    let mut history = History {
        messages: Vec::new(),
        users: BTreeSet::new(),
        channels: BTreeSet::new(),
        count,
    };
    for id in page {
        if let Some(message) = store::messages::get_any(ctx.db, owner, id)? {
            history.users.extend(messages::users_any(&message));
            history.channels.extend(messages::channels_any(&message));
            history.messages.push(message);
        }
    }
    Ok(history)
}

/// `reader` read its dialog with `peer` up to `max_id` (0 = everything). Returns the
/// reader's `(pts, pts_count)`.
pub fn read(ctx: &mut Ctx<'_>, reader: UserId, peer: Peer, max_id: i32) -> RpcResult<(i32, i32)> {
    // Who learns their messages were read, and how they see this dialog.
    let others: Vec<(UserId, Peer)> = match peer {
        Peer::User(other) => vec![(other, Peer::User(reader))],
        Peer::Chat(chat) => store::chats::members(ctx.db, chat)?
            .into_iter()
            .map(|m| (m.user, Peer::Chat(chat)))
            .collect(),
        // Channels are read with `channels.readHistory`.
        Peer::Channel(_) => return Err(RpcError::peer_id_invalid()),
    };
    let mentioned: Vec<UserId> = others.iter().map(|&(user, _)| user).collect();
    let top = store::dialogs::message_ids(ctx.db, reader, peer)?
        .first()
        .copied()
        .unwrap_or(0);
    let max_id = if max_id <= 0 { top } else { max_id.min(top) };
    let (read_inbox, _) = store::dialogs::read_state(ctx.db, reader, peer)?;
    if max_id <= read_inbox {
        return Ok((store::updates::state(ctx.db, reader)?.0, 0));
    }
    store::dialogs::set_read_inbox(ctx.db, reader, peer, max_id)?;
    let still_unread = store::dialogs::unread_count(ctx.db, reader, peer, max_id)?;
    let update = updates::push_pts(ctx, reader, mentioned, |pts, pts_count| {
        types::UpdateReadHistoryInbox {
            peer: peer.to_tl(),
            max_id,
            still_unread_count: still_unread,
            pts,
            pts_count,
            ..Default::default()
        }
        .into()
    })?;

    // Senders learn their messages up to there were read, by their own ids.
    for (other, their_peer) in others {
        if other == reader {
            continue;
        }
        let Some(outbox_max) =
            store::dialogs::counterpart_max_id(ctx.db, reader, peer, other, max_id)?
        else {
            continue;
        };
        let (_, read_outbox) = store::dialogs::read_state(ctx.db, other, their_peer)?;
        if outbox_max > read_outbox {
            store::dialogs::set_read_outbox(ctx.db, other, their_peer, outbox_max)?;
            if let Peer::Chat(chat) = their_peer {
                ctx.out.mention_chat(other, chat);
            }
            updates::push_pts_to_all(ctx, other, [reader], |pts, pts_count| {
                types::UpdateReadHistoryOutbox {
                    peer: their_peer.to_tl(),
                    max_id: outbox_max,
                    pts,
                    pts_count,
                }
                .into()
            })?;
        }
    }
    match update {
        enums::Update::ReadHistoryInbox(u) => Ok((u.pts, u.pts_count)),
        _ => Err(RpcError::internal("read update missing")),
    }
}
