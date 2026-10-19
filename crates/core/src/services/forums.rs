//! Forums: supergroups whose messages live in topics. A topic is opened by a
//! `messageActionTopicCreate` service message, whose id becomes the topic's id; General
//! is topic 1 and holds everything posted outside a topic. Messages in other topics
//! carry a reply header marked `forum_topic` that names the topic (as `reply_to_msg_id`
//! when they reply to nothing else, or as `reply_to_top_id`).
//!
//! Other supergroups have threads instead: a reply belongs to the thread its reply
//! chain starts from (`reply_to_top_id` when that isn't the replied message itself).
//!
//! Who may do what, as tdlib checks it: creating topics takes `manage_topics` (admins
//! granted it, or members when the group's permissions allow); editing, closing and
//! pinning take an admin's `manage_topics`, though a topic's creator may edit and close
//! their own; deleting takes `delete_messages`, or being the topic's creator. In a closed
//! topic only its creator and admins with `manage_topics` may post.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{channel_messages, channels, rights, updates};
use crate::store::{
    self,
    channels::{Channel, Member, Status},
    topics::Topic,
};

pub const GENERAL: i32 = 1;
/// The colors topic icons may have; the first is the default.
pub const COLORS: [i32; 6] = [0x6FB9F0, 0xFFD67E, 0xCB86DB, 0x8EEE98, 0xFF93B2, 0xFB6F5F];
const MAX_TITLE_LENGTH: usize = 128;
const MAX_PINNED: usize = 5;

pub fn is_forum(channel: &Channel) -> bool {
    !channel.broadcast && channel.extra.get("forum").and_then(|v| v.as_bool()) == Some(true)
}

/// The channel, which must be a forum.
pub fn get_forum(ctx: &Ctx<'_>, id: ChannelId) -> RpcResult<Channel> {
    let channel = channels::get(ctx, id)?;
    if !is_forum(&channel) {
        return Err(RpcError::bad_request("CHANNEL_FORUM_MISSING"));
    }
    Ok(channel)
}

/// The thread a new message of `channel` belongs to (stored with it): its topic in
/// forums, the start of its reply chain in other supergroups, none in channels.
pub fn thread_of(channel: &Channel, message: &enums::Message) -> Option<i32> {
    if channel.broadcast {
        return None;
    }
    let (id, header) = match message {
        enums::Message::Service(s) if matches!(s.action, enums::MessageAction::TopicCreate(_)) => {
            return Some(s.id);
        }
        enums::Message::Service(s) => (s.id, s.reply_to.as_ref()),
        enums::Message::Message(m) => (m.id, m.reply_to.as_ref()),
        enums::Message::Empty(_) => return None,
    };
    let header = match header {
        Some(enums::MessageReplyHeader::MessageReplyHeader(h)) => Some(&**h),
        _ => None,
    };
    let forum = is_forum(channel);
    match header {
        Some(h) if forum && h.forum_topic => h.reply_to_top_id.or(h.reply_to_msg_id),
        _ if forum => Some(GENERAL),
        Some(h) if h.reply_to_peer_id.is_none() && h.reply_from.is_none() => h
            .reply_to_top_id
            .or(h.reply_to_msg_id)
            .filter(|&top| top < id || id == 0),
        _ => None,
    }
}

/// Where a reply to message `replied` goes: `(forum_topic, reply_to_top_id)` for its
/// header, and the message actually replied to. `top_msg_id` names the topic the sender
/// writes in; a replied message from another topic gives way to the topic itself.
pub fn place_reply(
    ctx: &Ctx<'_>,
    channel: &Channel,
    replied: i32,
    top_msg_id: Option<i32>,
) -> RpcResult<(bool, Option<i32>, i32)> {
    let thread = store::topics::thread_of(ctx.db, channel.id, replied)?;
    if is_forum(channel) {
        let topic = top_msg_id.or(thread).unwrap_or(GENERAL);
        let replied = if thread.unwrap_or(GENERAL) == topic {
            replied
        } else {
            topic
        };
        if topic == GENERAL {
            return Ok((false, None, replied));
        }
        return Ok((true, (replied != topic).then_some(topic), replied));
    }
    let root = thread.unwrap_or(replied);
    Ok((false, (root != replied).then_some(root), replied))
}

/// Whether `member` may post in `thread`: a topic must exist, and closed ones take its
/// creator or an admin with `manage_topics`.
pub fn check_can_post(
    ctx: &Ctx<'_>,
    channel: &Channel,
    member: &Member,
    thread: Option<i32>,
) -> RpcResult<()> {
    if !is_forum(channel) {
        return Ok(());
    }
    let topic = thread.unwrap_or(GENERAL);
    let Some(found) = store::topics::get(ctx.db, channel.id, topic)? else {
        return Err(RpcError::bad_request("TOPIC_DELETED"));
    };
    if found.closed && found.creator != member.user && !can_edit_topics(member) {
        return Err(RpcError::bad_request("TOPIC_CLOSED"));
    }
    Ok(())
}

fn can_edit_topics(member: &Member) -> bool {
    rights::can(member, |r| r.manage_topics)
}

fn can_create_topics(ctx: &Ctx<'_>, channel: &Channel, member: &Member) -> bool {
    can_edit_topics(member)
        || (!member.status.is_admin()
            && !rights::restricted(ctx, channel, member, |r| r.manage_topics))
}

/// Turn a supergroup into a forum or back. Only its creator may. Existing messages go
/// to General.
pub fn toggle(ctx: &mut Ctx<'_>, actor: UserId, id: ChannelId, enabled: bool) -> RpcResult<()> {
    let channel = channels::get(ctx, id)?;
    if channel.broadcast {
        return Err(RpcError::bad_request("CHANNEL_INVALID"));
    }
    if channels::require_member(ctx, &channel, actor)?.status != Status::Creator {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    if is_forum(&channel) == enabled {
        return Err(RpcError::bad_request("CHAT_NOT_MODIFIED"));
    }
    set_forum(ctx, &channel, enabled)?;
    for member in store::channels::current_members(ctx.db, id)? {
        updates::push_plain(
            ctx,
            member.user,
            types::UpdateChannel { channel_id: id.0 }.into(),
            [],
        );
        ctx.out.mention_channel(member.user, id);
    }
    Ok(())
}

/// Mark the channel a forum (with its General topic) or not.
pub fn set_forum(ctx: &Ctx<'_>, channel: &Channel, enabled: bool) -> RpcResult<()> {
    let mut extra = channel.extra.clone();
    extra["forum"] = enabled.into();
    store::channels::set_extra(ctx.db, channel.id, &extra)?;
    if enabled {
        if store::topics::get(ctx.db, channel.id, GENERAL)?.is_none() {
            store::topics::put(
                ctx.db,
                &Topic {
                    channel: channel.id,
                    id: GENERAL,
                    title: "General".into(),
                    icon_color: COLORS[0],
                    icon_emoji_id: None,
                    creator: channel.creator,
                    date: channel.date,
                    closed: false,
                    hidden: false,
                    pinned: None,
                },
            )?;
        }
        ctx.db.execute(
            "UPDATE channel_messages SET thread_id = ?2
             WHERE channel_id = ?1 AND (thread_id IS NULL
                OR thread_id NOT IN (SELECT id FROM forum_topics WHERE channel_id = ?1))",
            rusqlite::params![channel.id.0, GENERAL],
        )?;
    }
    Ok(())
}

/// Open a topic; returns its id.
pub fn create(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    title: &str,
    icon_color: Option<i32>,
    icon_emoji_id: Option<i64>,
) -> RpcResult<i32> {
    let channel = get_forum(ctx, id)?;
    let member = channels::require_member(ctx, &channel, actor)?;
    if !can_create_topics(ctx, &channel, &member) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let title = check_title(title)?;
    let icon_color = icon_color
        .filter(|c| COLORS.contains(c))
        .unwrap_or(COLORS[0]);
    let topic = channels::post_service_with(
        ctx,
        &channel,
        actor,
        types::MessageActionTopicCreate {
            title_missing: false,
            title: title.clone(),
            icon_color,
            icon_emoji_id,
        }
        .into(),
        &[],
        None,
    )?;
    store::topics::put(
        ctx.db,
        &Topic {
            channel: id,
            id: topic,
            title,
            icon_color,
            icon_emoji_id,
            creator: actor,
            date: ctx.now(),
            closed: false,
            hidden: false,
            pinned: None,
        },
    )?;
    Ok(topic)
}

/// What `messages.editForumTopic` changes; `None` leaves a field as it is.
#[derive(Debug, Default)]
pub struct Edit {
    pub title: Option<String>,
    /// `Some(0)` removes the icon.
    pub icon_emoji_id: Option<i64>,
    pub closed: Option<bool>,
    pub hidden: Option<bool>,
}

/// Rename, change the icon, close or reopen a topic, or hide General; posts a
/// `messageActionTopicEdit` with what changed into the topic.
pub fn edit(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    topic: i32,
    edit: Edit,
) -> RpcResult<()> {
    let channel = get_forum(ctx, id)?;
    let member = channels::require_member(ctx, &channel, actor)?;
    let Some(mut found) = store::topics::get(ctx.db, id, topic)? else {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    };
    let others = edit.title.is_some() || edit.icon_emoji_id.is_some();
    if edit.closed.is_some() && (others || edit.hidden.is_some()) {
        return Err(RpcError::bad_request("TOPIC_CLOSE_SEPARATELY"));
    }
    if edit.hidden.is_some() && others {
        return Err(RpcError::bad_request("TOPIC_HIDE_SEPARATELY"));
    }
    if edit.hidden.is_some() && topic != GENERAL {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    }
    if edit.icon_emoji_id.is_some() && topic == GENERAL {
        return Err(RpcError::bad_request("GENERAL_MODIFY_ICON_FORBIDDEN"));
    }
    let own = found.creator == actor && topic != GENERAL;
    if !can_edit_topics(&member) && (edit.hidden.is_some() || !own) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let title = match edit.title {
        Some(title) => Some(check_title(&title)?).filter(|t| *t != found.title),
        None => None,
    };
    let emoji = edit
        .icon_emoji_id
        .filter(|&e| Some(e).filter(|&e| e != 0) != found.icon_emoji_id);
    let closed = edit.closed.filter(|&c| c != found.closed);
    let hidden = edit.hidden.filter(|&h| h != found.hidden);
    if title.is_none() && emoji.is_none() && closed.is_none() && hidden.is_none() {
        return Err(RpcError::bad_request("TOPIC_NOT_MODIFIED"));
    }
    if let Some(title) = &title {
        found.title = title.clone();
    }
    if let Some(emoji) = emoji {
        found.icon_emoji_id = Some(emoji).filter(|&e| e != 0);
    }
    if let Some(closed) = closed {
        found.closed = closed;
    }
    if let Some(hidden) = hidden {
        found.hidden = hidden;
    }
    store::topics::put(ctx.db, &found)?;
    channels::post_service_with(
        ctx,
        &channel,
        actor,
        types::MessageActionTopicEdit {
            title,
            icon_emoji_id: emoji,
            closed,
            hidden,
        }
        .into(),
        &[],
        header(topic),
    )?;
    Ok(())
}

/// The reply header of a service message posted into `topic` (none for General).
fn header(topic: i32) -> Option<enums::MessageReplyHeader> {
    (topic != GENERAL).then(|| {
        types::MessageReplyHeader {
            forum_topic: true,
            reply_to_msg_id: Some(topic),
            ..Default::default()
        }
        .into()
    })
}

/// Delete a topic with all its messages; returns the channel's `(pts, pts_count)`.
pub fn delete(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    topic: i32,
) -> RpcResult<(i32, i32)> {
    let channel = get_forum(ctx, id)?;
    let member = channels::require_member(ctx, &channel, actor)?;
    let Some(found) = store::topics::get(ctx.db, id, topic)? else {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    };
    if topic == GENERAL {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    }
    if found.creator != actor && !rights::can(&member, |r| r.delete_messages) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    if found.pinned.is_some() {
        unpin(ctx, &channel, topic)?;
    }
    store::topics::delete(ctx.db, id, topic)?;
    let ids = store::topics::message_ids(ctx.db, id, topic)?;
    channel_messages::remove(ctx, &channel, ids)
}

/// Pin or unpin a topic (newly pinned ones go first). Every member learns it.
pub fn pin(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    topic: i32,
    pinned: bool,
) -> RpcResult<()> {
    let channel = get_forum(ctx, id)?;
    let member = channels::require_member(ctx, &channel, actor)?;
    if !can_edit_topics(&member) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let Some(found) = store::topics::get(ctx.db, id, topic)? else {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    };
    if found.pinned.is_some() == pinned {
        return Err(RpcError::bad_request("PINNED_TOPIC_NOT_MODIFIED"));
    }
    if !pinned {
        return unpin(ctx, &channel, topic);
    }
    let mut order = pinned_order(ctx, id)?;
    if order.len() >= MAX_PINNED {
        return Err(RpcError::bad_request("PINNED_TOO_MUCH"));
    }
    order.insert(0, topic);
    save_order(ctx, id, &order)?;
    tell_members(ctx, &channel, |peer| {
        types::UpdatePinnedForumTopic {
            pinned: true,
            peer,
            topic_id: topic,
        }
        .into()
    })
}

fn unpin(ctx: &mut Ctx<'_>, channel: &Channel, topic: i32) -> RpcResult<()> {
    let mut order = pinned_order(ctx, channel.id)?;
    order.retain(|&t| t != topic);
    if let Some(mut found) = store::topics::get(ctx.db, channel.id, topic)? {
        found.pinned = None;
        store::topics::put(ctx.db, &found)?;
    }
    save_order(ctx, channel.id, &order)?;
    tell_members(ctx, channel, |peer| {
        types::UpdatePinnedForumTopic {
            pinned: false,
            peer,
            topic_id: topic,
        }
        .into()
    })
}

/// Put the pinned topics in `order` (which must name exactly the pinned ones, unless
/// `force`, which pins what it names and unpins the rest).
pub fn reorder(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    order: &[i32],
    force: bool,
) -> RpcResult<()> {
    let channel = get_forum(ctx, id)?;
    let member = channels::require_member(ctx, &channel, actor)?;
    if !can_edit_topics(&member) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let current = pinned_order(ctx, id)?;
    if order == current.as_slice() {
        return Err(RpcError::bad_request("PINNED_TOPICS_NOT_MODIFIED"));
    }
    let mut sorted_new = order.to_vec();
    sorted_new.sort_unstable();
    sorted_new.dedup();
    let mut sorted_current = current.clone();
    sorted_current.sort_unstable();
    if sorted_new.len() != order.len() || (!force && sorted_new != sorted_current) {
        return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
    }
    if order.len() > MAX_PINNED {
        return Err(RpcError::bad_request("PINNED_TOO_MUCH"));
    }
    for &topic in order {
        if store::topics::get(ctx.db, id, topic)?.is_none() {
            return Err(RpcError::bad_request("TOPIC_ID_INVALID"));
        }
    }
    for topic in current.iter().filter(|t| !order.contains(t)) {
        if let Some(mut found) = store::topics::get(ctx.db, id, *topic)? {
            found.pinned = None;
            store::topics::put(ctx.db, &found)?;
        }
    }
    save_order(ctx, id, order)?;
    let order = order.to_vec();
    tell_members(ctx, &channel, |peer| {
        types::UpdatePinnedForumTopics {
            peer,
            order: Some(order.clone()),
        }
        .into()
    })
}

/// Pinned topics, first first.
fn pinned_order(ctx: &Ctx<'_>, id: ChannelId) -> RpcResult<Vec<i32>> {
    let mut pinned: Vec<(i32, i32)> = store::topics::all(ctx.db, id)?
        .into_iter()
        .filter_map(|t| t.pinned.map(|position| (position, t.id)))
        .collect();
    pinned.sort_unstable();
    Ok(pinned.into_iter().map(|(_, topic)| topic).collect())
}

fn save_order(ctx: &Ctx<'_>, id: ChannelId, order: &[i32]) -> RpcResult<()> {
    for (position, &topic) in order.iter().enumerate() {
        if let Some(mut found) = store::topics::get(ctx.db, id, topic)? {
            found.pinned = Some(position as i32);
            store::topics::put(ctx.db, &found)?;
        }
    }
    Ok(())
}

fn tell_members(
    ctx: &mut Ctx<'_>,
    channel: &Channel,
    build: impl Fn(enums::Peer) -> enums::Update,
) -> RpcResult<()> {
    for member in store::channels::current_members(ctx.db, channel.id)? {
        ctx.out.mention_channel(member.user, channel.id);
        updates::push_plain(
            ctx,
            member.user,
            build(Peer::Channel(channel.id).to_tl()),
            [],
        );
    }
    Ok(())
}

/// Topics of the forum in the order clients list them: pinned first, then by their
/// newest message.
pub fn list(ctx: &Ctx<'_>, id: ChannelId) -> RpcResult<Vec<Topic>> {
    let mut topics = store::topics::all(ctx.db, id)?;
    let mut keyed = Vec::with_capacity(topics.len());
    for topic in topics.drain(..) {
        let top = top_message(ctx, id, topic.id)?;
        keyed.push((
            topic.pinned.unwrap_or(i32::MAX),
            std::cmp::Reverse(top),
            topic,
        ));
    }
    keyed.sort_by_key(|k| (k.0, k.1));
    Ok(keyed.into_iter().map(|(_, _, topic)| topic).collect())
}

/// The newest message of a topic (its creation message when it has no other).
pub fn top_message(ctx: &Ctx<'_>, id: ChannelId, topic: i32) -> RpcResult<i32> {
    Ok(store::topics::message_ids(ctx.db, id, topic)?
        .first()
        .copied()
        .unwrap_or(topic))
}

/// A topic as `viewer` sees it.
pub fn render(ctx: &Ctx<'_>, viewer: UserId, topic: &Topic) -> RpcResult<enums::ForumTopic> {
    let (read_inbox, read_outbox) =
        store::dialogs::read_state(ctx.db, viewer, Peer::Channel(topic.channel))?;
    let ids = store::topics::message_ids(ctx.db, topic.channel, topic.id)?;
    let mut unread = 0;
    for &message in ids.iter().filter(|&&m| m > read_inbox) {
        if store::channels::message(ctx.db, topic.channel, message)?
            .is_some_and(|stored| stored.author != viewer)
        {
            unread += 1;
        }
    }
    Ok(types::ForumTopic {
        my: topic.creator == viewer,
        closed: topic.closed,
        pinned: topic.pinned.is_some(),
        hidden: topic.hidden,
        id: topic.id,
        date: topic.date,
        peer: Peer::Channel(topic.channel).to_tl(),
        title: topic.title.clone(),
        icon_color: topic.icon_color,
        icon_emoji_id: topic.icon_emoji_id,
        top_message: ids.first().copied().unwrap_or(topic.id),
        read_inbox_max_id: read_inbox,
        read_outbox_max_id: read_outbox,
        unread_count: unread,
        from_id: Peer::User(topic.creator).to_tl(),
        notify_settings: types::PeerNotifySettings::default().into(),
        ..Default::default()
    }
    .into())
}

fn check_title(title: &str) -> RpcResult<String> {
    let title = title.trim();
    if title.is_empty() {
        return Err(RpcError::bad_request("TOPIC_TITLE_EMPTY"));
    }
    Ok(title.chars().take(MAX_TITLE_LENGTH).collect())
}

/// The supergroup an `InputPeer` of a forum call names.
pub fn resolve(ctx: &Ctx<'_>, viewer: UserId, input: &enums::InputPeer) -> RpcResult<ChannelId> {
    match crate::services::peers::resolve_input_peer(ctx, viewer, input)? {
        Peer::Channel(id) => Ok(id),
        _ => Err(RpcError::bad_request("CHANNEL_FORUM_MISSING")),
    }
}

/// `messages.forumTopics` for `topics`, as `viewer` sees them: with each topic's newest
/// message and who those messages name.
pub fn topics_result(
    ctx: &Ctx<'_>,
    viewer: UserId,
    channel: &Channel,
    topics: &[Topic],
    count: usize,
) -> RpcResult<enums::messages::ForumTopics> {
    let mut rendered = Vec::with_capacity(topics.len());
    let mut messages = Vec::new();
    let mut users = std::collections::BTreeSet::new();
    for topic in topics {
        rendered.push(render(ctx, viewer, topic)?);
        users.insert(topic.creator);
        let top = top_message(ctx, channel.id, topic.id)?;
        if let Some(stored) = store::channels::message(ctx.db, channel.id, top)? {
            let message =
                channel_messages::as_seen_by(&stored.message, stored.author, Some(viewer));
            users.extend(crate::services::messages::users_any(&message));
            messages.push(message);
        }
    }
    Ok(types::messages::ForumTopics {
        order_by_create_date: false,
        count: count as i32,
        topics: rendered,
        messages,
        chats: vec![crate::render::channel(ctx, viewer, channel)?],
        users: crate::render::users(ctx, viewer, &users)?,
        pts: channel.pts,
    }
    .into())
}
