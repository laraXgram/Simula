//! Invite links (`https://t.me/+…`) and join requests for basic groups, supergroups
//! and channels. Admins with the right to invite create links: an admin's primary link
//! (`permanent`) and extra ones with an expiry, a usage limit, a title, or requiring
//! approval. A link that needs approval files a join request; admin bots learn of it
//! through `updateBotChatInviteRequester` (qts), admin users through
//! `updatePendingJoinRequests`, and the bot may then write to the requester.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::render;
use crate::services::{channels, chats, rights, updates};
use crate::store::{self, chats::Role, invites::Invite};

const LINK_PREFIX: &str = "https://t.me/+";
const MAX_USAGE_LIMIT: i32 = 99_999;
const MAX_TITLE_LENGTH: usize = 32;
/// How long a bot may write to someone who asked to join (Bot API: 5 minutes).
pub const REQUESTER_CONTACT_WINDOW: i32 = 5 * 60;

#[derive(Debug, Default)]
pub struct NewInvite {
    /// Replace `actor`'s primary link with a new one.
    pub replace_permanent: bool,
    pub request_needed: bool,
    pub expire_date: Option<i32>,
    pub usage_limit: Option<i32>,
    pub title: Option<String>,
}

#[derive(Debug, Default)]
pub struct InviteChange {
    pub revoked: bool,
    pub expire_date: Option<i32>,
    pub usage_limit: Option<i32>,
    pub request_needed: Option<bool>,
    pub title: Option<String>,
}

/// The hash in a link, or the hash itself.
pub fn hash_of(link: &str) -> &str {
    let link = link.trim();
    ["https://", "http://", ""]
        .iter()
        .find_map(|scheme| link.strip_prefix(scheme))
        .and_then(|rest| {
            [
                "t.me/+",
                "t.me/joinchat/",
                "telegram.me/+",
                "telegram.me/joinchat/",
            ]
            .iter()
            .find_map(|prefix| rest.strip_prefix(prefix))
        })
        .unwrap_or(link)
}

pub fn export(ctx: &mut Ctx<'_>, actor: UserId, peer: Peer, new: NewInvite) -> RpcResult<Invite> {
    require_invite_right(ctx, actor, peer)?;
    check_limits(ctx, new.expire_date, new.usage_limit, new.title.as_deref())?;
    if new.replace_permanent
        && let Some(mut old) = store::invites::permanent(ctx.db, peer, actor)?
    {
        old.revoked = true;
        store::invites::put(ctx.db, &old)?;
    }
    let invite = Invite {
        hash: new_hash(),
        peer,
        admin: actor,
        date: ctx.now(),
        expire_date: new.expire_date.filter(|&d| d > 0),
        usage_limit: new.usage_limit.filter(|&l| l > 0),
        usage: 0,
        request_needed: new.request_needed,
        revoked: false,
        permanent: new.replace_permanent,
        title: new.title.filter(|t| !t.is_empty()),
    };
    store::invites::put(ctx.db, &invite)?;
    Ok(invite)
}

/// Edit or revoke one of the chat's links. Revoking a primary link replaces it; the
/// replacement comes back second.
pub fn edit(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    peer: Peer,
    link: &str,
    change: InviteChange,
) -> RpcResult<(Invite, Option<Invite>)> {
    require_invite_right(ctx, actor, peer)?;
    let mut invite = store::invites::get(ctx.db, hash_of(link))?
        .filter(|i| i.peer == peer)
        .ok_or_else(|| RpcError::bad_request("INVITE_HASH_INVALID"))?;
    if invite.admin != actor && !is_owner(ctx, actor, peer)? {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    if change.revoked {
        invite.revoked = true;
        store::invites::put(ctx.db, &invite)?;
        let replacement = if invite.permanent {
            Some(export(
                ctx,
                invite.admin,
                peer,
                NewInvite {
                    replace_permanent: true,
                    ..Default::default()
                },
            )?)
        } else {
            None
        };
        return Ok((invite, replacement));
    }
    if invite.revoked {
        return Err(RpcError::bad_request("INVITE_REVOKED_MISSING"));
    }
    check_limits(
        ctx,
        change.expire_date,
        change.usage_limit,
        change.title.as_deref(),
    )?;
    if let Some(date) = change.expire_date {
        invite.expire_date = Some(date).filter(|&d| d > 0);
    }
    if let Some(limit) = change.usage_limit {
        invite.usage_limit = Some(limit).filter(|&l| l > 0);
    }
    if let Some(needed) = change.request_needed {
        invite.request_needed = needed;
    }
    if let Some(title) = change.title {
        invite.title = Some(title).filter(|t| !t.is_empty());
    }
    store::invites::put(ctx.db, &invite)?;
    Ok((invite, None))
}

/// What a link leads to, for someone about to use it.
pub fn check(ctx: &Ctx<'_>, user: UserId, hash: &str) -> RpcResult<enums::ChatInvite> {
    let invite = usable(ctx, hash)?;
    if is_member(ctx, invite.peer, user)? {
        return Ok(types::ChatInviteAlready {
            chat: chat_object(ctx, user, invite.peer)?,
        }
        .into());
    }
    let (title, about, count, channel, broadcast, public) = match invite.peer {
        Peer::Chat(chat) => {
            let found = chats::get(ctx, chat)?;
            let about = found
                .extra
                .get("about")
                .and_then(|v| v.as_str())
                .unwrap_or_default()
                .to_owned();
            let count = store::chats::members(ctx.db, chat)?.len() as i32;
            (found.title, about, count, false, false, false)
        }
        Peer::Channel(id) => {
            let found = channels::get(ctx, id)?;
            let count = store::channels::current_members(ctx.db, id)?.len() as i32;
            (
                found.title,
                found.about,
                count,
                true,
                found.broadcast,
                found.username.is_some(),
            )
        }
        Peer::User(_) => return Err(RpcError::bad_request("INVITE_HASH_INVALID")),
    };
    Ok(types::ChatInvite {
        channel,
        broadcast,
        public,
        megagroup: channel && !broadcast,
        request_needed: invite.request_needed,
        title,
        about: Some(about).filter(|a| !a.is_empty()),
        photo: enums::Photo::Empty(Box::default()),
        participants_count: count,
        ..Default::default()
    }
    .into())
}

/// Use a link: join, or file a join request (kept, but answered `INVITE_REQUEST_SENT`).
pub fn import(ctx: &mut Ctx<'_>, user: UserId, hash: &str) -> RpcResult<()> {
    if ctx.account(user)?.is_bot {
        return Err(RpcError::bad_request("BOT_METHOD_INVALID"));
    }
    let mut invite = usable(ctx, hash)?;
    if is_member(ctx, invite.peer, user)? {
        return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
    }
    if invite.request_needed {
        let date = ctx.now();
        store::invites::add_request(ctx.db, invite.peer, user, date, &invite.hash)?;
        for admin in admins(ctx, invite.peer)? {
            if ctx.account(admin)?.is_bot {
                mention(ctx, admin, invite.peer);
                let peer = invite.peer;
                let rendered = self::render(ctx, Some(admin), &invite)?;
                updates::push_qts(ctx, admin, [user, invite.admin], |qts| {
                    types::UpdateBotChatInviteRequester {
                        peer: peer.to_tl(),
                        date,
                        user_id: user.0,
                        about: String::new(),
                        invite: rendered,
                        qts,
                        query_id: None,
                    }
                    .into()
                })?;
            }
        }
        pending_changed(ctx, invite.peer)?;
        ctx.fail_after_commit(RpcError::bad_request("INVITE_REQUEST_SENT"));
        return Ok(());
    }
    invite.usage += 1;
    store::invites::put(ctx.db, &invite)?;
    join(ctx, user, &invite, false)
}

/// Approve or decline `user`'s request.
pub fn hide_request(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    peer: Peer,
    user: UserId,
    approved: bool,
) -> RpcResult<()> {
    require_invite_right(ctx, actor, peer)?;
    let Some((_, hash, _)) = store::invites::request(ctx.db, peer, user)? else {
        return Err(RpcError::bad_request("HIDE_REQUESTER_MISSING"));
    };
    store::invites::remove_request(ctx.db, peer, user)?;
    if approved {
        let mut invite = store::invites::get(ctx.db, &hash)?
            .ok_or_else(|| RpcError::internal("join request without its link"))?;
        invite.usage += 1;
        store::invites::put(ctx.db, &invite)?;
        join(ctx, user, &invite, true)?;
    }
    pending_changed(ctx, peer)
}

/// Whether `bot` may write to `user` first: they asked to join a chat where the bot
/// handles join requests, within the last five minutes.
pub fn may_contact(ctx: &Ctx<'_>, bot: UserId, user: UserId) -> RpcResult<bool> {
    let now = ctx.now();
    for (peer, date) in store::invites::requests_of(ctx.db, user)? {
        if now - date <= REQUESTER_CONTACT_WINDOW && admins(ctx, peer)?.contains(&bot) {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The TL form of a link for `viewer` (`None`: as stored). Links of other admins
/// show only their beginning, unless the viewer owns the chat.
pub fn render(
    ctx: &Ctx<'_>,
    viewer: Option<UserId>,
    invite: &Invite,
) -> RpcResult<enums::ExportedChatInvite> {
    let full = match viewer {
        None => true,
        Some(viewer) => viewer == invite.admin || is_owner(ctx, viewer, invite.peer)?,
    };
    let link = if full {
        format!("{LINK_PREFIX}{}", invite.hash)
    } else {
        format!("{LINK_PREFIX}{}…", &invite.hash[..invite.hash.len().min(6)])
    };
    let requested = store::invites::requests_via(ctx.db, &invite.hash)?;
    Ok(types::ChatInviteExported {
        revoked: invite.revoked,
        permanent: invite.permanent,
        request_needed: invite.request_needed,
        link,
        admin_id: invite.admin.0,
        date: invite.date,
        expire_date: invite.expire_date,
        usage_limit: invite.usage_limit,
        usage: Some(invite.usage).filter(|&u| u > 0),
        requested: Some(requested).filter(|&r| r > 0),
        title: invite.title.clone(),
        ..Default::default()
    }
    .into())
}

/// `actor`'s primary link, if they have one (for full chat objects).
pub fn primary(
    ctx: &Ctx<'_>,
    actor: UserId,
    peer: Peer,
) -> RpcResult<Option<enums::ExportedChatInvite>> {
    match store::invites::permanent(ctx.db, peer, actor)? {
        Some(invite) => Ok(Some(render(ctx, Some(actor), &invite)?)),
        None => Ok(None),
    }
}

fn join(ctx: &mut Ctx<'_>, user: UserId, invite: &Invite, via_request: bool) -> RpcResult<()> {
    let rendered = render(ctx, None, invite)?;
    match invite.peer {
        Peer::Chat(chat) => {
            chats::join_by_link(ctx, user, chat, invite.admin, rendered, via_request)
        }
        Peer::Channel(id) => {
            channels::join_by_link(ctx, user, id, invite.admin, rendered, via_request)
        }
        Peer::User(_) => Err(RpcError::bad_request("INVITE_HASH_INVALID")),
    }
}

/// A live link, or why it can't be used.
fn usable(ctx: &Ctx<'_>, hash: &str) -> RpcResult<Invite> {
    let invite = store::invites::get(ctx.db, hash_of(hash))?
        .ok_or_else(|| RpcError::bad_request("INVITE_HASH_INVALID"))?;
    let expired = invite.revoked
        || invite.expire_date.is_some_and(|d| d <= ctx.now())
        || invite.usage_limit.is_some_and(|l| invite.usage >= l);
    if expired {
        return Err(RpcError::bad_request("INVITE_HASH_EXPIRED"));
    }
    Ok(invite)
}

/// Basic groups: their admins. Supergroups and channels: admins with `invite_users`.
fn require_invite_right(ctx: &Ctx<'_>, actor: UserId, peer: Peer) -> RpcResult<()> {
    let allowed = match peer {
        Peer::Chat(chat) => {
            chats::get(ctx, chat)?;
            chats::require_member(ctx, chat, actor)?.role != Role::Member
        }
        Peer::Channel(id) => {
            let channel = channels::get(ctx, id)?;
            let member = channels::require_member(ctx, &channel, actor)?;
            rights::can(&member, |r| r.invite_users)
        }
        Peer::User(_) => return Err(RpcError::peer_id_invalid()),
    };
    if !allowed {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    Ok(())
}

/// Who handles the chat's join requests.
fn admins(ctx: &Ctx<'_>, peer: Peer) -> RpcResult<Vec<UserId>> {
    Ok(match peer {
        Peer::Chat(chat) => store::chats::members(ctx.db, chat)?
            .into_iter()
            .filter(|m| m.role != Role::Member)
            .map(|m| m.user)
            .collect(),
        Peer::Channel(id) => store::channels::current_members(ctx.db, id)?
            .into_iter()
            .filter(|m| rights::can(m, |r| r.invite_users))
            .map(|m| m.user)
            .collect(),
        Peer::User(_) => Vec::new(),
    })
}

fn is_owner(ctx: &Ctx<'_>, user: UserId, peer: Peer) -> RpcResult<bool> {
    Ok(match peer {
        Peer::Chat(chat) => chats::get(ctx, chat)?.creator == user,
        Peer::Channel(id) => channels::get(ctx, id)?.creator == user,
        Peer::User(_) => false,
    })
}

fn is_member(ctx: &Ctx<'_>, peer: Peer, user: UserId) -> RpcResult<bool> {
    Ok(match peer {
        Peer::Chat(chat) => store::chats::member(ctx.db, chat, user)?.is_some(),
        Peer::Channel(id) => {
            store::channels::member(ctx.db, id, user)?.is_some_and(|m| m.status.is_member())
        }
        Peer::User(_) => false,
    })
}

fn chat_object(ctx: &Ctx<'_>, viewer: UserId, peer: Peer) -> RpcResult<enums::Chat> {
    match peer {
        Peer::Chat(chat) => render::chat(ctx, viewer, &chats::get(ctx, chat)?),
        Peer::Channel(id) => render::channel(ctx, viewer, &channels::get(ctx, id)?),
        Peer::User(_) => Err(RpcError::peer_id_invalid()),
    }
}

fn mention(ctx: &mut Ctx<'_>, account: UserId, peer: Peer) {
    match peer {
        Peer::Chat(chat) => ctx.out.mention_chat(account, chat),
        Peer::Channel(id) => ctx.out.mention_channel(account, id),
        Peer::User(_) => {}
    }
}

/// Admin users see how many requests wait (`updatePendingJoinRequests`).
fn pending_changed(ctx: &mut Ctx<'_>, peer: Peer) -> RpcResult<()> {
    let requesters = store::invites::requesters(ctx.db, peer)?;
    let recent: Vec<i64> = requesters.iter().take(3).map(|u| u.0).collect();
    for admin in admins(ctx, peer)? {
        if ctx.account(admin)?.is_bot {
            continue;
        }
        mention(ctx, admin, peer);
        updates::push_plain(
            ctx,
            admin,
            types::UpdatePendingJoinRequests {
                peer: peer.to_tl(),
                requests_pending: requesters.len() as i32,
                recent_requesters: recent.clone(),
            }
            .into(),
            requesters.iter().take(3).copied(),
        );
    }
    Ok(())
}

fn check_limits(
    ctx: &Ctx<'_>,
    expire_date: Option<i32>,
    usage_limit: Option<i32>,
    title: Option<&str>,
) -> RpcResult<()> {
    if expire_date.is_some_and(|d| d > 0 && d <= ctx.now()) {
        return Err(RpcError::bad_request("EXPIRE_DATE_INVALID"));
    }
    if usage_limit.is_some_and(|l| !(0..=MAX_USAGE_LIMIT).contains(&l)) {
        return Err(RpcError::bad_request("USAGE_LIMIT_INVALID"));
    }
    if title.is_some_and(|t| t.chars().count() > MAX_TITLE_LENGTH) {
        return Err(RpcError::bad_request("INVITE_TITLE_INVALID"));
    }
    Ok(())
}

/// 16 URL-safe characters, like Telegram's link hashes.
fn new_hash() -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-";
    (0..16)
        .map(|_| ALPHABET[rand::random_range(0..ALPHABET.len())] as char)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::hash_of;

    #[test]
    fn links_and_hashes() {
        assert_eq!(hash_of("https://t.me/+AbC_d-12"), "AbC_d-12");
        assert_eq!(hash_of("t.me/joinchat/AbC"), "AbC");
        assert_eq!(hash_of("AbC"), "AbC");
    }
}
