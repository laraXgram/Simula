//! Messages in supergroups and channels. Each channel has one box: a message is stored
//! once with one id, and every member who gets it receives the same update carrying the
//! channel's pts, marked `out` for its author. In supergroups, bots in privacy mode get
//! only what is meant for them (see `services::group_messages`); in channels, posts
//! don't name their author and bots are always admins.

use std::collections::BTreeSet;

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{
    channels, dialogs, edits, group_messages, messages, replies, rights, updates,
};
use crate::store::{self, channels::Channel};

/// Bots may delete messages only within 48 hours of sending.
const BOT_DELETE_TIME_LIMIT: i32 = 2 * 86400;

/// A stored message as `viewer` sees it (`None`: as stored, for logs).
pub fn as_seen_by(
    message: &enums::Message,
    author: UserId,
    viewer: Option<UserId>,
) -> enums::Message {
    let out = viewer == Some(author);
    match message {
        enums::Message::Message(m) => types::Message {
            out,
            ..(**m).clone()
        }
        .into(),
        enums::Message::Service(m) => types::MessageService {
            out,
            ..(**m).clone()
        }
        .into(),
        other => other.clone(),
    }
}

pub fn send(
    ctx: &mut Ctx<'_>,
    from: UserId,
    id: ChannelId,
    out: messages::Outgoing,
) -> RpcResult<messages::Sent> {
    let channel = channels::get(ctx, id)?;
    let sender = channels::require_member(ctx, &channel, from)?;
    rights::check_can_send(ctx, &channel, &sender)?;
    messages::check_text(&out.text)?;
    let members = store::channels::current_members(ctx.db, id)?;
    let accounts = members
        .iter()
        .map(|m| Ok((ctx.account(m.user)?, m.status)))
        .collect::<RpcResult<Vec<_>>>()?;
    let bots: Vec<UserId> = accounts
        .iter()
        .filter(|(a, _)| a.is_bot)
        .map(|(a, _)| a.id)
        .collect();
    let prepared = messages::prepare(ctx, from, Peer::Channel(id), out, !bots.is_empty())?;
    let last_bot = channel
        .extra
        .get("last_bot")
        .and_then(|v| v.as_i64())
        .map(UserId)
        .filter(|bot| bots.contains(bot))
        .or_else(|| (bots.len() == 1).then(|| bots[0]));
    let recipients: Vec<UserId> = accounts
        .iter()
        .filter(|(account, status)| {
            account.id == from
                || !account.is_bot
                || channel.broadcast
                || group_messages::bot_gets(account, status.is_admin(), &prepared, last_bot)
        })
        .map(|(account, _)| account.id)
        .collect();

    let out = &prepared.out;
    let message = types::Message {
        post: channel.broadcast,
        silent: out.silent,
        noforwards: out.noforwards,
        invert_media: out.invert_media && prepared.preview.is_some(),
        media: prepared.preview.clone(),
        id: store::channels::next_message_id(ctx.db, id)?,
        from_id: (!channel.broadcast).then(|| Peer::User(from).to_tl()),
        peer_id: Peer::Channel(id).to_tl(),
        reply_to: prepared
            .reply_to
            .as_ref()
            .map(|target| replies::header(ctx, from, target))
            .transpose()?,
        date: ctx.now(),
        message: out.text.clone(),
        entities: (!prepared.entities.is_empty()).then(|| prepared.entities.clone()),
        reply_markup: prepared.reply_markup.clone(),
        ..Default::default()
    };
    let stored: enums::Message = message.clone().into();
    store::channels::insert_message(ctx.db, id, from, out.random_id, &stored)?;
    let mut mentioned = BTreeSet::new();
    for &recipient in &recipients {
        if recipient != from && messages::mentions(ctx, &prepared, recipient)? {
            mentioned.insert(recipient);
        }
    }
    let mut mentions: Vec<UserId> = messages::users(&message).into_iter().collect();
    mentions.push(from);
    for &recipient in &recipients {
        for other in messages::channels(&message) {
            ctx.out.mention_channel(recipient, other);
        }
    }
    updates::push_channel(
        ctx,
        id,
        1,
        &recipients,
        &mentions,
        |viewer, pts, pts_count| {
            let mut seen = as_seen_by(&stored, from, viewer);
            if let (enums::Message::Message(m), Some(viewer)) = (&mut seen, viewer) {
                m.mentioned = mentioned.contains(&viewer);
            }
            types::UpdateNewChannelMessage {
                message: seen,
                pts,
                pts_count,
            }
            .into()
        },
    )?;

    if !channel.broadcast && ctx.account(from)?.is_bot {
        let mut extra = channel.extra;
        extra["last_bot"] = from.0.into();
        store::channels::set_extra(ctx.db, id, &extra)?;
    }
    let message = types::Message {
        out: true,
        ..message
    };
    let update = ctx
        .out
        .updates_for(from)
        .into_iter()
        .rev()
        .find(|u| matches!(u, enums::Update::NewChannelMessage(_)))
        .ok_or_else(|| RpcError::internal("sender update missing"))?;
    Ok(messages::Sent { message, update })
}

pub struct History {
    pub messages: Vec<enums::Message>,
    pub users: BTreeSet<UserId>,
    /// Other supergroups and channels the messages refer to.
    pub channels: BTreeSet<ChannelId>,
    pub count: usize,
    pub pts: i32,
}

/// A page of the channel's history, for members and readers of public channels.
pub fn history(
    ctx: &Ctx<'_>,
    viewer: UserId,
    id: ChannelId,
    window: dialogs::Window,
) -> RpcResult<History> {
    let channel = channels::get(ctx, id)?;
    channels::require_reader(ctx, &channel, viewer)?;
    let (page, count) = window.page(store::channels::message_ids(ctx.db, id)?);
    let mut history = History {
        messages: Vec::new(),
        users: BTreeSet::new(),
        channels: BTreeSet::new(),
        count,
        pts: channel.pts,
    };
    for message_id in page {
        if let Some(stored) = store::channels::message(ctx.db, id, message_id)? {
            let message = as_seen_by(&stored.message, stored.author, Some(viewer));
            history.users.extend(messages::users_any(&message));
            history.channels.extend(messages::channels_any(&message));
            history.messages.push(message);
        }
    }
    Ok(history)
}

/// Messages by id, as `viewer` sees them; missing ones are `messageEmpty`.
pub fn get(ctx: &Ctx<'_>, viewer: UserId, id: ChannelId, ids: &[i32]) -> RpcResult<History> {
    let channel = channels::get(ctx, id)?;
    channels::require_reader(ctx, &channel, viewer)?;
    let mut found = Vec::with_capacity(ids.len());
    let mut users = BTreeSet::new();
    let mut referred = BTreeSet::new();
    for &message_id in ids {
        found.push(match store::channels::message(ctx.db, id, message_id)? {
            Some(stored) => {
                let message = as_seen_by(&stored.message, stored.author, Some(viewer));
                users.extend(messages::users_any(&message));
                referred.extend(messages::channels_any(&message));
                message
            }
            None => types::MessageEmpty {
                id: message_id,
                peer_id: Some(Peer::Channel(id).to_tl()),
            }
            .into(),
        });
    }
    Ok(History {
        count: found.len(),
        messages: found,
        users,
        channels: referred,
        pts: channel.pts,
    })
}

/// The id of the message `viewer`'s message `id` replies to.
pub fn replied_id(ctx: &Ctx<'_>, id: ChannelId, message_id: i32) -> RpcResult<Option<i32>> {
    Ok(
        store::channels::message(ctx.db, id, message_id)?.and_then(|stored| match stored.message {
            enums::Message::Message(m) => messages::same_chat_reply_id(&m),
            _ => None,
        }),
    )
}

/// `reader` read the channel up to `max_id` (0 = everything). Authors of the messages
/// read learn it through `updateReadChannelOutbox` (in supergroups).
pub fn read(ctx: &mut Ctx<'_>, reader: UserId, id: ChannelId, max_id: i32) -> RpcResult<()> {
    let channel = channels::get(ctx, id)?;
    channels::require_member(ctx, &channel, reader)?;
    let peer = Peer::Channel(id);
    let top = store::channels::top(ctx.db, id)?.map_or(0, |(top, _)| top);
    let max_id = if max_id <= 0 { top } else { max_id.min(top) };
    let (read_inbox, _) = store::dialogs::read_state(ctx.db, reader, peer)?;
    if max_id <= read_inbox {
        return Ok(());
    }
    store::dialogs::set_read_inbox(ctx.db, reader, peer, max_id)?;
    let still_unread = store::channels::unread_count(ctx.db, id, reader, max_id)?;
    updates::push_plain(
        ctx,
        reader,
        types::UpdateReadChannelInbox {
            folder_id: None,
            channel_id: id.0,
            max_id,
            still_unread_count: still_unread,
            pts: channel.pts,
        }
        .into(),
        [],
    );
    ctx.out.mention_channel(reader, id);
    if channel.broadcast {
        return Ok(());
    }
    for author in store::channels::authors_between(ctx.db, id, reader, read_inbox, max_id)? {
        let is_member =
            store::channels::member(ctx.db, id, author)?.is_some_and(|m| m.status.is_member());
        let (_, read_outbox) = store::dialogs::read_state(ctx.db, author, peer)?;
        if !is_member || read_outbox >= max_id {
            continue;
        }
        store::dialogs::set_read_outbox(ctx.db, author, peer, max_id)?;
        ctx.out.mention_channel(author, id);
        ctx.out.push(
            author,
            types::UpdateReadChannelOutbox {
                channel_id: id.0,
                max_id,
            }
            .into(),
            [reader],
            None,
        );
    }
    Ok(())
}

/// Edit a message: its author may (users within 48 hours), and in channels admins
/// with `edit_messages` may edit any post.
pub fn edit(
    ctx: &mut Ctx<'_>,
    editor: UserId,
    id: ChannelId,
    message_id: i32,
    edit: edits::Edit,
) -> RpcResult<edits::Edited> {
    let channel = channels::get(ctx, id)?;
    let member = channels::require_member(ctx, &channel, editor)?;
    let Some(stored) = store::channels::message(ctx.db, id, message_id)? else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    let author = stored.author;
    let enums::Message::Message(old) = stored.message else {
        return Err(RpcError::bad_request("MESSAGE_ID_INVALID"));
    };
    let is_bot = ctx.account(editor)?.is_bot;
    let own = author == editor;
    let may = own || (channel.broadcast && rights::can(&member, |r| r.edit_messages));
    if !may {
        return Err(RpcError::forbidden("MESSAGE_AUTHOR_REQUIRED"));
    }
    if own {
        edits::check_time(ctx, is_bot, &old)?;
    }
    let with_commands = is_bot || has_bot(ctx, id)?;
    let content = edits::Content::new(ctx, editor, with_commands, &old, edit)?;
    let mut message = *old;
    content.apply(&mut message);
    store::channels::update_message(ctx.db, id, &message)?;
    let stored: enums::Message = message.clone().into();
    let recipients = recipients(ctx, &channel)?;
    let mut mentions: Vec<UserId> = messages::users(&message).into_iter().collect();
    mentions.push(editor);
    updates::push_channel(
        ctx,
        id,
        1,
        &recipients,
        &mentions,
        |viewer, pts, pts_count| {
            types::UpdateEditChannelMessage {
                message: as_seen_by(&stored, author, viewer),
                pts,
                pts_count,
            }
            .into()
        },
    )?;
    let update = ctx
        .out
        .updates_for(editor)
        .into_iter()
        .rev()
        .find(|u| matches!(u, enums::Update::EditChannelMessage(_)))
        .ok_or_else(|| RpcError::internal("editor update missing"))?;
    Ok(edits::Edited {
        message: types::Message {
            out: author == editor,
            ..message
        },
        update,
    })
}

/// Delete messages: anyone their own (bots within 48 hours), admins with
/// `delete_messages` anyone's. Unknown ids are skipped. Returns the channel's
/// `(pts, pts_count)`.
pub fn delete(
    ctx: &mut Ctx<'_>,
    caller: UserId,
    id: ChannelId,
    ids: &[i32],
) -> RpcResult<(i32, i32)> {
    let channel = channels::get(ctx, id)?;
    let member = channels::require_member(ctx, &channel, caller)?;
    let is_bot = ctx.account(caller)?.is_bot;
    let may_delete_any = rights::can(&member, |r| r.delete_messages);
    let mut deleted = Vec::new();
    for &message_id in ids {
        let Some(stored) = store::channels::message(ctx.db, id, message_id)? else {
            continue;
        };
        let own = stored.author == caller;
        if !own && !may_delete_any {
            return Err(RpcError::forbidden("MESSAGE_DELETE_FORBIDDEN"));
        }
        let date = match &stored.message {
            enums::Message::Message(m) => m.date,
            enums::Message::Service(m) => m.date,
            enums::Message::Empty(_) => 0,
        };
        if is_bot && ctx.now() - date > BOT_DELETE_TIME_LIMIT {
            return Err(RpcError::bad_request("MESSAGE_DELETE_FORBIDDEN"));
        }
        if !deleted.contains(&message_id) {
            deleted.push(message_id);
        }
    }
    if deleted.is_empty() {
        return Ok((channel.pts, 0));
    }
    for &message_id in &deleted {
        store::channels::delete_message(ctx.db, id, message_id)?;
    }
    let count = deleted.len() as i32;
    let recipients = recipients(ctx, &channel)?;
    let pts = updates::push_channel(ctx, id, count, &recipients, &[], |_, pts, pts_count| {
        types::UpdateDeleteChannelMessages {
            channel_id: id.0,
            messages: deleted.clone(),
            pts,
            pts_count,
        }
        .into()
    })?;
    Ok((pts, count))
}

/// Everyone who gets the channel's updates.
fn recipients(ctx: &Ctx<'_>, channel: &Channel) -> RpcResult<Vec<UserId>> {
    Ok(store::channels::current_members(ctx.db, channel.id)?
        .into_iter()
        .map(|m| m.user)
        .collect())
}

fn has_bot(ctx: &Ctx<'_>, id: ChannelId) -> RpcResult<bool> {
    for member in store::channels::current_members(ctx.db, id)? {
        if ctx.account(member.user)?.is_bot {
            return Ok(true);
        }
    }
    Ok(false)
}
