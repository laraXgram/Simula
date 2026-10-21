//! Admin rights and restrictions in supergroups and channels: what the creator holds,
//! what an admin was given, the group's default permissions, and a member's own
//! restrictions (which apply until their `until_date`).

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::store::channels::{Channel, Member, Status};

/// Restrictions shorter than 30 seconds or longer than 366 days are forever, as on
/// Telegram; they are stored with `until_date` 0.
pub fn normalize_until(ctx: &Ctx<'_>, until: i32) -> i32 {
    let left = i64::from(until) - i64::from(ctx.now());
    if until <= 0 || !(30..=366 * 86400).contains(&left) {
        0
    } else {
        until
    }
}

pub fn is_forever(until: i32) -> bool {
    until == 0 || until == i32::MAX
}

/// Whether a restriction ending at `until` still applies.
pub fn in_force(ctx: &Ctx<'_>, until: i32) -> bool {
    is_forever(until) || until > ctx.now()
}

/// Everything the creator may do.
pub fn creator_rights(broadcast: bool) -> types::ChatAdminRights {
    types::ChatAdminRights {
        change_info: true,
        post_messages: broadcast,
        edit_messages: broadcast,
        delete_messages: true,
        ban_users: true,
        invite_users: true,
        pin_messages: true,
        add_admins: true,
        manage_call: true,
        other: true,
        manage_topics: !broadcast,
        post_stories: true,
        edit_stories: true,
        delete_stories: true,
        manage_direct_messages: broadcast,
        ..Default::default()
    }
}

/// The rights `member` holds as an admin, if they are one.
pub fn admin_rights(channel: &Channel, member: &Member) -> Option<types::ChatAdminRights> {
    match member.status {
        Status::Creator => Some(creator_rights(channel.broadcast)),
        Status::Admin => Some(member.admin_rights.clone().unwrap_or_default()),
        _ => None,
    }
}

/// Whether `member` is an admin holding `right` (the creator holds every right).
pub fn can(member: &Member, right: impl Fn(&types::ChatAdminRights) -> bool) -> bool {
    match member.status {
        Status::Creator => true,
        Status::Admin => member.admin_rights.as_ref().is_some_and(right),
        _ => false,
    }
}

/// What new supergroups forbid their members: changing the group's info, pinning,
/// managing topics.
pub fn new_group_defaults() -> types::ChatBannedRights {
    types::ChatBannedRights {
        change_info: true,
        pin_messages: true,
        manage_topics: true,
        until_date: i32::MAX,
        ..Default::default()
    }
}

/// The supergroup's default permissions, as rights its members lack.
pub fn default_banned(channel: &Channel) -> types::ChatBannedRights {
    channel
        .extra
        .get("default_banned_rights")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or_else(new_group_defaults)
}

/// Whether a non-admin `member` of a supergroup lacks a permission, by their own
/// restrictions or the group's defaults. Admins lack nothing.
pub fn restricted(
    ctx: &Ctx<'_>,
    channel: &Channel,
    member: &Member,
    right: impl Fn(&types::ChatBannedRights) -> bool,
) -> bool {
    if member.status.is_admin() {
        return false;
    }
    let own = member.status == Status::Restricted
        && member
            .banned_rights
            .as_ref()
            .is_some_and(|r| in_force(ctx, r.until_date) && right(r));
    own || (!channel.broadcast && right(&default_banned(channel)))
}

/// Telegram's checks before `member` posts a text message or a medium (captions need
/// no right of their own).
pub fn check_can_send(
    ctx: &Ctx<'_>,
    channel: &Channel,
    member: &Member,
    media: Option<&enums::MessageMedia>,
) -> RpcResult<()> {
    if channel.broadcast {
        if !can(member, |r| r.post_messages) {
            return Err(RpcError::forbidden("CHAT_ADMIN_REQUIRED"));
        }
        return Ok(());
    }
    let (banned, error): (fn(&types::ChatBannedRights) -> bool, &str) = match media {
        None | Some(enums::MessageMedia::WebPage(_)) => (
            |r| r.send_messages || r.send_plain,
            "CHAT_SEND_PLAIN_FORBIDDEN",
        ),
        Some(enums::MessageMedia::Photo(_)) => (|r| r.send_photos, "CHAT_SEND_PHOTOS_FORBIDDEN"),
        Some(enums::MessageMedia::Document(m)) => {
            let attributes = match &m.document {
                Some(enums::Document::Document(d)) => d.attributes.as_slice(),
                _ => &[],
            };
            let has = |f: fn(&enums::DocumentAttribute) -> bool| attributes.iter().any(f);
            if has(|a| matches!(a, enums::DocumentAttribute::Animated)) {
                (|r| r.send_gifs, "CHAT_SEND_GIFS_FORBIDDEN")
            } else if m.round {
                (|r| r.send_roundvideos, "CHAT_SEND_ROUNDVIDEOS_FORBIDDEN")
            } else if m.voice {
                (|r| r.send_voices, "CHAT_SEND_VOICES_FORBIDDEN")
            } else if m.video {
                (|r| r.send_videos, "CHAT_SEND_VIDEOS_FORBIDDEN")
            } else if has(|a| matches!(a, enums::DocumentAttribute::Audio(_))) {
                (|r| r.send_audios, "CHAT_SEND_AUDIOS_FORBIDDEN")
            } else {
                (|r| r.send_docs, "CHAT_SEND_DOCS_FORBIDDEN")
            }
        }
        Some(_) => (|r| r.send_media, "CHAT_SEND_MEDIA_FORBIDDEN"),
    };
    // Media also need the general right to send media.
    let media_banned = media.is_some_and(|m| !matches!(m, enums::MessageMedia::WebPage(_)))
        && restricted(ctx, channel, member, |r| r.send_messages || r.send_media);
    if media_banned {
        return Err(RpcError::forbidden("CHAT_SEND_MEDIA_FORBIDDEN"));
    }
    if restricted(ctx, channel, member, banned) {
        return Err(RpcError::forbidden(error));
    }
    Ok(())
}

/// Whether every right in `wanted` is also in `held` (admins only grant what they have).
pub fn is_subset(wanted: &types::ChatAdminRights, held: &types::ChatAdminRights) -> bool {
    let pairs = [
        (wanted.change_info, held.change_info),
        (wanted.post_messages, held.post_messages),
        (wanted.edit_messages, held.edit_messages),
        (wanted.delete_messages, held.delete_messages),
        (wanted.ban_users, held.ban_users),
        (wanted.invite_users, held.invite_users),
        (wanted.pin_messages, held.pin_messages),
        (wanted.add_admins, held.add_admins),
        (wanted.anonymous, held.anonymous),
        (wanted.manage_call, held.manage_call),
        (wanted.other, held.other),
        (wanted.manage_topics, held.manage_topics),
        (wanted.post_stories, held.post_stories),
        (wanted.edit_stories, held.edit_stories),
        (wanted.delete_stories, held.delete_stories),
        (wanted.manage_direct_messages, held.manage_direct_messages),
    ];
    pairs.iter().all(|&(w, h)| !w || h)
}

/// Whether `rights` grant nothing (demotes to a plain member).
pub fn is_empty(rights: &types::ChatAdminRights) -> bool {
    is_subset(rights, &types::ChatAdminRights::default())
}

/// Whether `rights` take nothing away.
pub fn takes_nothing(rights: &types::ChatBannedRights) -> bool {
    *rights
        == types::ChatBannedRights {
            until_date: rights.until_date,
            ..Default::default()
        }
}
