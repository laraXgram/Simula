//! Supergroups and channels: creating them, members joining, leaving, being invited,
//! promoted, restricted or banned, and the channel's title, description, username and
//! default permissions. In supergroups, membership changes post service messages
//! (channels stay silent about them); bots whose status changes, and admin bots for
//! everyone's, get `updateChannelParticipant`, the source of `my_chat_member` /
//! `chat_member`.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, Peer, UserId};
use crate::services::{accounts, channel_messages, chats, rights, updates};
use crate::store::{
    self,
    channels::{Channel, Member, Status},
};

/// Channel ids, in the range of Telegram's newer ones (Bot API `-1002…`).
const FIRST_CHANNEL_ID: i64 = 2_000_000_001;
const MAX_TITLE_LENGTH: usize = 128;
const MAX_ABOUT_LENGTH: usize = 255;
const MAX_RANK_LENGTH: usize = 16;

/// The channel, if it exists and wasn't deleted.
pub fn get(ctx: &Ctx<'_>, id: ChannelId) -> RpcResult<Channel> {
    match store::channels::get(ctx.db, id)? {
        Some(found) if !found.deleted => Ok(found),
        _ => Err(RpcError::bad_request("CHANNEL_INVALID")),
    }
}

/// `user`'s status, if any.
pub fn member(ctx: &Ctx<'_>, channel: ChannelId, user: UserId) -> RpcResult<Option<Member>> {
    Ok(store::channels::member(ctx.db, channel, user)?)
}

/// `user` as a current member, or Telegram's error for why they can't act there.
pub fn require_member(ctx: &Ctx<'_>, channel: &Channel, user: UserId) -> RpcResult<Member> {
    match member(ctx, channel.id, user)? {
        Some(m) if m.status.is_member() => Ok(m),
        Some(m) if m.status == Status::Banned => Err(RpcError::forbidden("CHANNEL_PRIVATE")),
        _ if channel.username.is_some() => Err(RpcError::forbidden("CHAT_WRITE_FORBIDDEN")),
        _ => Err(RpcError::forbidden("CHANNEL_PRIVATE")),
    }
}

/// Whether `user` may read the channel: members, and anyone for public channels unless
/// banned. Returns their status, if any.
pub fn require_reader(ctx: &Ctx<'_>, channel: &Channel, user: UserId) -> RpcResult<Option<Member>> {
    let found = member(ctx, channel.id, user)?;
    let allowed = match &found {
        Some(m) if m.status == Status::Banned => false,
        Some(m) if m.status.is_member() => true,
        _ => channel.username.is_some(),
    };
    if !allowed {
        return Err(RpcError::forbidden("CHANNEL_PRIVATE"));
    }
    Ok(found)
}

/// `actor` as an admin holding `right`, or `CHAT_ADMIN_REQUIRED`.
fn require_right(
    ctx: &Ctx<'_>,
    channel: &Channel,
    actor: UserId,
    right: impl Fn(&types::ChatAdminRights) -> bool,
) -> RpcResult<Member> {
    let found = require_member(ctx, channel, actor)?;
    if !rights::can(&found, right) {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    Ok(found)
}

/// Changing the title, photo or description: admins with `change_info`, and in
/// supergroups every member the default permissions allow.
fn require_change_info(ctx: &Ctx<'_>, channel: &Channel, actor: UserId) -> RpcResult<()> {
    let found = require_member(ctx, channel, actor)?;
    let allowed = rights::can(&found, |r| r.change_info)
        || (!channel.broadcast
            && !found.status.is_admin()
            && !rights::restricted(ctx, channel, &found, |r| r.change_info));
    if !allowed {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    Ok(())
}

pub fn create(
    ctx: &mut Ctx<'_>,
    creator: UserId,
    broadcast: bool,
    title: &str,
    about: &str,
) -> RpcResult<ChannelId> {
    if ctx.account(creator)?.is_bot {
        return Err(RpcError::bad_request("BOT_METHOD_INVALID"));
    }
    let title = check_title(title)?;
    check_about(about)?;
    let id = insert(
        ctx,
        creator,
        broadcast,
        &title,
        about,
        serde_json::json!({}),
    )?;
    updates::push_plain(
        ctx,
        creator,
        types::UpdateChannel { channel_id: id.0 }.into(),
        [],
    );
    ctx.out.mention_channel(creator, id);
    let channel = get(ctx, id)?;
    post_service(
        ctx,
        &channel,
        creator,
        types::MessageActionChannelCreate { title }.into(),
        &[],
    )?;
    Ok(id)
}

/// A new channel row with `creator` as its creator, and nothing posted yet.
pub(crate) fn insert(
    ctx: &mut Ctx<'_>,
    creator: UserId,
    broadcast: bool,
    title: &str,
    about: &str,
    extra: serde_json::Value,
) -> RpcResult<ChannelId> {
    let id = ChannelId(store::meta::next(ctx.db, "channel", FIRST_CHANNEL_ID)?);
    let date = ctx.now();
    store::channels::insert(
        ctx.db,
        &Channel {
            id,
            broadcast,
            title: title.to_owned(),
            username: None,
            about: about.to_owned(),
            creator,
            date,
            pts: 0,
            deleted: false,
            extra,
        },
    )?;
    store::channels::put_member(
        ctx.db,
        id,
        &Member::new(creator, Status::Creator, creator, date),
    )?;
    Ok(id)
}

/// `actor` adds `users`. In supergroups members may invite unless the default
/// permissions forbid it; in channels only admins with `invite_users`.
pub fn invite(ctx: &mut Ctx<'_>, actor: UserId, id: ChannelId, users: &[UserId]) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let inviter = require_member(ctx, &channel, actor)?;
    let may_invite = rights::can(&inviter, |r| r.invite_users)
        || (!channel.broadcast
            && !inviter.status.is_admin()
            && !rights::restricted(ctx, &channel, &inviter, |r| r.invite_users));
    if !may_invite {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let mut added = Vec::new();
    for &user in users {
        let account = ctx.account(user)?;
        if account.is_bot && channel.broadcast {
            return Err(RpcError::bad_request("USER_BOT"));
        }
        chats::check_can_join(ctx, user)?;
        let before = member(ctx, id, user)?;
        match &before {
            Some(m) if m.status.is_member() => {
                if users.len() == 1 {
                    return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
                }
                continue;
            }
            Some(m) if m.status == Status::Banned && !rights::can(&inviter, |r| r.ban_users) => {
                return Err(RpcError::bad_request("USER_KICKED"));
            }
            _ => {}
        }
        let now = ctx.now();
        store::channels::put_member(ctx.db, id, &Member::new(user, Status::Member, actor, now))?;
        added.push((user, before));
    }
    if added.is_empty() {
        return Ok(());
    }
    if !channel.broadcast {
        let action = types::MessageActionChatAddUser {
            users: added.iter().map(|(u, _)| u.0).collect(),
        };
        post_service(ctx, &channel, actor, action.into(), &[])?;
    }
    for (user, before) in added {
        membership_changed(ctx, actor, &channel, user, before, None)?;
    }
    Ok(())
}

/// Join a public supergroup or channel.
pub fn join(ctx: &mut Ctx<'_>, user: UserId, id: ChannelId) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let before = member(ctx, id, user)?;
    match &before {
        Some(m) if m.status.is_member() => {
            return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
        }
        Some(m) if m.status == Status::Banned => {
            return Err(RpcError::forbidden("CHANNEL_PRIVATE"));
        }
        _ => {}
    }
    if channel.username.is_none() {
        return Err(RpcError::forbidden("CHANNEL_PRIVATE"));
    }
    if ctx.account(user)?.is_bot {
        return Err(RpcError::bad_request("BOT_METHOD_INVALID"));
    }
    let now = ctx.now();
    store::channels::put_member(ctx.db, id, &Member::new(user, Status::Member, user, now))?;
    if !channel.broadcast {
        let action = types::MessageActionChatAddUser {
            users: vec![user.0],
        };
        post_service(ctx, &channel, user, action.into(), &[])?;
    }
    membership_changed(ctx, user, &channel, user, before, None)
}

/// `user` joins through an invite link created by `inviter` (or, `via_request`, after
/// an admin approved their request).
pub fn join_by_link(
    ctx: &mut Ctx<'_>,
    user: UserId,
    id: ChannelId,
    inviter: UserId,
    invite: enums::ExportedChatInvite,
    via_request: bool,
) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let before = member(ctx, id, user)?;
    match &before {
        Some(m) if m.status.is_member() => {
            return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
        }
        Some(m) if m.status == Status::Banned => {
            return Err(RpcError::forbidden("CHANNEL_PRIVATE"));
        }
        _ => {}
    }
    let now = ctx.now();
    store::channels::put_member(ctx.db, id, &Member::new(user, Status::Member, inviter, now))?;
    if !channel.broadcast {
        let action: enums::MessageAction = if via_request {
            types::MessageActionChatJoinedByRequest {}.into()
        } else {
            types::MessageActionChatJoinedByLink {
                inviter_id: inviter.0,
            }
            .into()
        };
        post_service(ctx, &channel, user, action, &[])?;
    }
    membership_changed(ctx, user, &channel, user, before, Some(invite))
}

/// Leave. (The creator can't leave the group they own here; Telegram lets them leave
/// while staying its owner, which Simula doesn't model yet.)
pub fn leave(ctx: &mut Ctx<'_>, user: UserId, id: ChannelId) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let before = match member(ctx, id, user)? {
        Some(m) if m.status.is_member() => m,
        _ => return Err(RpcError::bad_request("USER_NOT_PARTICIPANT")),
    };
    if before.status == Status::Creator {
        return Err(RpcError::bad_request("USER_CREATOR"));
    }
    if !channel.broadcast {
        let action = types::MessageActionChatDeleteUser { user_id: user.0 };
        post_service(ctx, &channel, user, action.into(), &[])?;
    }
    let now = ctx.now();
    store::channels::put_member(
        ctx.db,
        id,
        &Member {
            status: Status::Left,
            admin_rights: None,
            banned_rights: None,
            rank: None,
            date: now,
            ..before.clone()
        },
    )?;
    membership_changed(ctx, user, &channel, user, Some(before), None)
}

/// Promote, edit or demote an admin (empty rights demote). Bots that aren't members
/// join as admins, the way bots are added to channels.
pub fn edit_admin(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    user: UserId,
    wanted: types::ChatAdminRights,
    rank: Option<String>,
) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let promoter = require_right(ctx, &channel, actor, |r| r.add_admins)?;
    if rank
        .as_deref()
        .is_some_and(|r| r.chars().count() > MAX_RANK_LENGTH)
    {
        return Err(RpcError::bad_request("ADMIN_RANK_INVALID"));
    }
    let held = rights::admin_rights(&channel, &promoter).unwrap_or_default();
    if promoter.status != Status::Creator && !rights::is_subset(&wanted, &held) {
        return Err(RpcError::bad_request("RIGHT_FORBIDDEN"));
    }
    let before = member(ctx, id, user)?;
    let is_bot = ctx.account(user)?.is_bot;
    let current = match &before {
        Some(m) if m.status == Status::Creator => {
            return Err(RpcError::bad_request("USER_CREATOR"));
        }
        Some(m) if m.status.is_member() => Some(m.clone()),
        Some(m) if m.status == Status::Banned => {
            return Err(RpcError::bad_request("USER_KICKED"));
        }
        _ if is_bot => {
            chats::check_can_join(ctx, user)?;
            None
        }
        _ => return Err(RpcError::bad_request("USER_NOT_PARTICIPANT")),
    };
    // Admins edit only the admins they promoted.
    if let Some(m) = &current
        && m.status == Status::Admin
        && promoter.status != Status::Creator
        && m.promoted_by != Some(actor)
    {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let now = ctx.now();
    let demote = rights::is_empty(&wanted);
    if demote && current.as_ref().is_none_or(|m| m.status != Status::Admin) {
        return Ok(());
    }
    let joined = current.is_none();
    let base = current.unwrap_or_else(|| Member::new(user, Status::Member, actor, now));
    let next = if demote {
        Member {
            status: Status::Member,
            promoted_by: None,
            admin_rights: None,
            rank: None,
            ..base
        }
    } else {
        Member {
            status: Status::Admin,
            promoted_by: Some(actor),
            admin_rights: Some(wanted),
            banned_rights: None,
            // No rank keeps the current one; an empty one removes it.
            rank: match rank {
                Some(r) => Some(r).filter(|r| !r.is_empty()),
                None => base.rank.clone(),
            },
            ..base
        }
    };
    store::channels::put_member(ctx.db, id, &next)?;
    if joined && !channel.broadcast {
        let action = types::MessageActionChatAddUser {
            users: vec![user.0],
        };
        post_service(ctx, &channel, actor, action.into(), &[])?;
    }
    membership_changed(ctx, actor, &channel, user, before, None)
}

/// Restrict, ban (`view_messages`) or lift restrictions (no rights taken).
pub fn edit_banned(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    user: UserId,
    mut banned: types::ChatBannedRights,
) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let admin = require_right(ctx, &channel, actor, |r| r.ban_users)?;
    let before = member(ctx, id, user)?;
    if let Some(m) = &before {
        let protected = m.status == Status::Creator
            || (m.status == Status::Admin
                && admin.status != Status::Creator
                && m.promoted_by != Some(actor));
        if protected {
            return Err(RpcError::bad_request("USER_ADMIN_INVALID"));
        }
    }
    banned.until_date = rights::normalize_until(ctx, banned.until_date);
    let now = ctx.now();
    let was_member = before.as_ref().is_some_and(|m| m.status.is_member());
    let base = before
        .clone()
        .unwrap_or_else(|| Member::new(user, Status::Left, actor, now));
    let next = if banned.view_messages {
        Member {
            status: Status::Banned,
            kicked_by: Some(actor),
            banned_rights: Some(banned),
            admin_rights: None,
            promoted_by: None,
            rank: None,
            date: now,
            ..base
        }
    } else if rights::takes_nothing(&banned) {
        match base.status {
            Status::Banned => Member {
                status: Status::Left,
                kicked_by: None,
                banned_rights: None,
                ..base
            },
            Status::Restricted => Member {
                status: Status::Member,
                kicked_by: None,
                banned_rights: None,
                ..base
            },
            _ => return Ok(()),
        }
    } else {
        if !was_member {
            return Err(RpcError::bad_request("USER_NOT_PARTICIPANT"));
        }
        Member {
            status: Status::Restricted,
            kicked_by: Some(actor),
            banned_rights: Some(banned),
            admin_rights: None,
            promoted_by: None,
            rank: None,
            ..base
        }
    };
    // The removed member still sees the service message about it.
    if was_member && next.status == Status::Banned && !channel.broadcast {
        let action = types::MessageActionChatDeleteUser { user_id: user.0 };
        post_service(ctx, &channel, actor, action.into(), &[])?;
    }
    store::channels::put_member(ctx.db, id, &next)?;
    membership_changed(ctx, actor, &channel, user, before, None)
}

pub fn rename(ctx: &mut Ctx<'_>, actor: UserId, id: ChannelId, title: &str) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    require_change_info(ctx, &channel, actor)?;
    let title = check_title(title)?;
    if title == channel.title {
        return Err(RpcError::bad_request("CHAT_NOT_MODIFIED"));
    }
    store::channels::set_title(ctx.db, id, &title)?;
    let channel = get(ctx, id)?;
    post_service(
        ctx,
        &channel,
        actor,
        types::MessageActionChatEditTitle { title }.into(),
        &[],
    )
}

pub fn set_about(ctx: &mut Ctx<'_>, actor: UserId, id: ChannelId, about: &str) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    require_change_info(ctx, &channel, actor)?;
    check_about(about)?;
    if about == channel.about {
        return Err(RpcError::bad_request("CHAT_ABOUT_NOT_MODIFIED"));
    }
    store::channels::set_about(ctx.db, id, about)?;
    Ok(())
}

/// The supergroup's default permissions.
pub fn set_default_banned(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    mut banned: types::ChatBannedRights,
) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    if channel.broadcast {
        return Err(RpcError::bad_request("CHANNEL_INVALID"));
    }
    require_right(ctx, &channel, actor, |r| r.ban_users)?;
    banned.until_date = i32::MAX;
    if banned == rights::default_banned(&channel) {
        return Err(RpcError::bad_request("CHAT_NOT_MODIFIED"));
    }
    let mut extra = channel.extra.clone();
    extra["default_banned_rights"] = serde_json::to_value(&banned).map_err(RpcError::internal)?;
    store::channels::set_extra(ctx.db, id, &extra)?;
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

/// Set or remove (`""`) the public username. Only the creator may.
pub fn set_username(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    id: ChannelId,
    username: &str,
) -> RpcResult<()> {
    let channel = get(ctx, id)?;
    let found = require_member(ctx, &channel, actor)?;
    if found.status != Status::Creator {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let username = username.trim_start_matches('@');
    let current = channel.username.as_deref().unwrap_or("");
    if username.eq_ignore_ascii_case(current) {
        return Err(RpcError::bad_request("USERNAME_NOT_MODIFIED"));
    }
    if username.is_empty() {
        store::channels::set_username(ctx.db, id, None)?;
    } else {
        accounts::check_username(ctx, username)?;
        store::channels::set_username(ctx.db, id, Some(username))?;
    }
    Ok(())
}

/// A service message in the channel's box, for every member (and `also`, such as a
/// member being removed).
pub fn post_service(
    ctx: &mut Ctx<'_>,
    channel: &Channel,
    actor: UserId,
    action: enums::MessageAction,
    also: &[UserId],
) -> RpcResult<()> {
    post_service_replying(ctx, channel, actor, action, also, None)
}

/// [`post_service`] replying to message `reply_to` (a pinned message's notice).
pub fn post_service_replying(
    ctx: &mut Ctx<'_>,
    channel: &Channel,
    actor: UserId,
    action: enums::MessageAction,
    also: &[UserId],
    reply_to: Option<i32>,
) -> RpcResult<()> {
    let mut recipients: Vec<UserId> = store::channels::current_members(ctx.db, channel.id)?
        .into_iter()
        .map(|m| m.user)
        .collect();
    for &user in also {
        if !recipients.contains(&user) {
            recipients.push(user);
        }
    }
    let mut mentions = vec![actor];
    mentions.extend(chats::action_users(&action));
    let message: enums::Message = types::MessageService {
        post: channel.broadcast,
        id: store::channels::next_message_id(ctx.db, channel.id)?,
        from_id: (!channel.broadcast).then(|| Peer::User(actor).to_tl()),
        peer_id: Peer::Channel(channel.id).to_tl(),
        reply_to: reply_to.map(|id| {
            types::MessageReplyHeader {
                reply_to_msg_id: Some(id),
                ..Default::default()
            }
            .into()
        }),
        date: ctx.now(),
        action,
        ..Default::default()
    }
    .into();
    store::channels::insert_message(ctx.db, channel.id, actor, None, &message)?;
    updates::push_channel(
        ctx,
        channel.id,
        1,
        &recipients,
        &mentions,
        |viewer, pts, pts_count| {
            types::UpdateNewChannelMessage {
                message: channel_messages::as_seen_by(&message, actor, viewer),
                pts,
                pts_count,
            }
            .into()
        },
    )?;
    Ok(())
}

/// After `user`'s status changed: their clients get `updateChannel`; the bot itself
/// (if `user` is one) and admin bots get `updateChannelParticipant`.
fn membership_changed(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    channel: &Channel,
    user: UserId,
    before: Option<Member>,
    invite: Option<enums::ExportedChatInvite>,
) -> RpcResult<()> {
    let after = member(ctx, channel.id, user)?;
    updates::push_plain(
        ctx,
        user,
        types::UpdateChannel {
            channel_id: channel.id.0,
        }
        .into(),
        [],
    );
    ctx.out.mention_channel(user, channel.id);
    let mut bots: Vec<UserId> = Vec::new();
    if ctx.account(user)?.is_bot {
        bots.push(user);
    }
    for m in store::channels::current_members(ctx.db, channel.id)? {
        if m.user != user && m.status.is_admin() && ctx.account(m.user)?.is_bot {
            bots.push(m.user);
        }
    }
    let date = ctx.now();
    for bot in bots {
        let prev = before.as_ref().and_then(|m| participant(channel, bot, m));
        let new = after.as_ref().and_then(|m| participant(channel, bot, m));
        ctx.out.mention_channel(bot, channel.id);
        updates::push_qts(ctx, bot, [actor, user], |qts| {
            types::UpdateChannelParticipant {
                via_chatlist: false,
                channel_id: channel.id.0,
                date,
                actor_id: actor.0,
                user_id: user.0,
                prev_participant: prev,
                new_participant: new,
                invite: invite.clone(),
                qts,
            }
            .into()
        })?;
    }
    Ok(())
}

/// The TL form of `member` as `viewer` sees it; `None` for who left.
pub fn participant(
    channel: &Channel,
    viewer: UserId,
    member: &Member,
) -> Option<enums::ChannelParticipant> {
    let is_self = member.user == viewer;
    Some(match member.status {
        Status::Creator => types::ChannelParticipantCreator {
            user_id: member.user.0,
            admin_rights: rights::creator_rights(channel.broadcast).into(),
            rank: member.rank.clone(),
        }
        .into(),
        Status::Admin => types::ChannelParticipantAdmin {
            can_edit: member.promoted_by == Some(viewer) || channel.creator == viewer,
            self_: is_self,
            user_id: member.user.0,
            inviter_id: is_self.then_some(member.inviter.0),
            promoted_by: member.promoted_by.unwrap_or(channel.creator).0,
            date: member.date,
            admin_rights: member.admin_rights.clone().unwrap_or_default().into(),
            rank: member.rank.clone(),
        }
        .into(),
        Status::Member if is_self => types::ChannelParticipantSelf {
            via_request: false,
            user_id: member.user.0,
            inviter_id: member.inviter.0,
            date: member.date,
            subscription_until_date: None,
            rank: member.rank.clone(),
        }
        .into(),
        Status::Member => types::ChannelParticipant {
            user_id: member.user.0,
            date: member.date,
            subscription_until_date: None,
            rank: member.rank.clone(),
        }
        .into(),
        Status::Restricted | Status::Banned => types::ChannelParticipantBanned {
            left: member.status == Status::Banned,
            peer: Peer::User(member.user).to_tl(),
            kicked_by: member.kicked_by.unwrap_or(channel.creator).0,
            date: member.date,
            banned_rights: member.banned_rights.clone().unwrap_or_default().into(),
            rank: None,
        }
        .into(),
        Status::Left => return None,
    })
}

fn check_title(title: &str) -> RpcResult<String> {
    let title = title.trim();
    if title.is_empty() {
        return Err(RpcError::bad_request("CHAT_TITLE_EMPTY"));
    }
    Ok(title.chars().take(MAX_TITLE_LENGTH).collect())
}

fn check_about(about: &str) -> RpcResult<()> {
    if about.chars().count() > MAX_ABOUT_LENGTH {
        return Err(RpcError::bad_request("CHAT_ABOUT_TOO_LONG"));
    }
    Ok(())
}
