//! Dialogs as clients see them: the chat list with unread counts, a dialog's history
//! in pages, and reading (each side gets its read-history update with its own pts).

use std::collections::BTreeSet;

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::{messages, updates};
use crate::store;

pub struct DialogList {
    pub dialogs: Vec<enums::Dialog>,
    pub messages: Vec<enums::Message>,
    pub users: BTreeSet<UserId>,
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
    let all = store::dialogs::list(ctx.db, owner)?;
    let total = all.len();
    let mut out = DialogList {
        dialogs: Vec::new(),
        messages: Vec::new(),
        users: BTreeSet::new(),
        total,
    };
    let page = all.into_iter().filter(|&(_, id, date)| {
        offset_date == 0 || date < offset_date || (date == offset_date && id < offset_id)
    });
    for (peer, top, _) in page.take(limit) {
        let (read_inbox, read_outbox) = store::dialogs::read_state(ctx.db, owner, peer)?;
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
        if let Some(message) = store::messages::get(ctx.db, owner, top)? {
            out.users.extend(messages::users(&message));
            out.messages.push(message.into());
        }
    }
    Ok(out)
}

/// Which page of a history (see [`history`]).
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
    /// Messages in the dialog within the id bounds.
    pub count: usize,
}

/// `messages.getHistory`'s window: newest first, starting below `offset_id` (0 = the
/// newest), shifted by `add_offset` (negative reaches newer messages), within
/// `min_id < id < max_id` when those are set.
pub fn history(ctx: &Ctx<'_>, owner: UserId, peer: Peer, window: Window) -> RpcResult<History> {
    let Window {
        offset_id,
        add_offset,
        limit,
        min_id,
        max_id,
    } = window;
    let ids: Vec<i32> = store::dialogs::message_ids(ctx.db, owner, peer)?
        .into_iter()
        .filter(|&id| (min_id <= 0 || id > min_id) && (max_id <= 0 || id < max_id))
        .collect();
    let first_below = if offset_id > 0 {
        ids.iter()
            .position(|&id| id < offset_id)
            .unwrap_or(ids.len())
    } else {
        0
    };
    let start = (first_below as i64 + i64::from(add_offset)).max(0) as usize;
    let end = (start + limit).min(ids.len());
    let mut history = History {
        messages: Vec::new(),
        users: BTreeSet::new(),
        count: ids.len(),
    };
    for &id in ids.get(start..end).unwrap_or_default() {
        if let Some(message) = store::messages::get(ctx.db, owner, id)? {
            history.users.extend(messages::users(&message));
            history.messages.push(message.into());
        }
    }
    Ok(history)
}

/// `reader` read its dialog with `peer` up to `max_id` (0 = everything). Returns the
/// reader's `(pts, pts_count)`.
pub fn read(ctx: &mut Ctx<'_>, reader: UserId, peer: Peer, max_id: i32) -> RpcResult<(i32, i32)> {
    let Peer::User(other) = peer else {
        return Err(RpcError::not_implemented("reading groups and channels"));
    };
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
    let update = updates::push_pts(ctx, reader, [other], |pts, pts_count| {
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

    // The other side learns its messages up to there were read, by its own ids.
    if other != reader
        && let Some(outbox_max) = store::dialogs::counterpart_max_id(ctx.db, reader, other, max_id)?
    {
        let reader_peer = Peer::User(reader);
        let (_, read_outbox) = store::dialogs::read_state(ctx.db, other, reader_peer)?;
        if outbox_max > read_outbox {
            store::dialogs::set_read_outbox(ctx.db, other, reader_peer, outbox_max)?;
            updates::push_pts_to_all(ctx, other, [reader], |pts, pts_count| {
                types::UpdateReadHistoryOutbox {
                    peer: reader_peer.to_tl(),
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
