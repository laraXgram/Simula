//! Basic groups: creating them, adding and removing members, renaming. Every change
//! posts a service message to the members' boxes; bots whose membership changes (and
//! admin bots, for others) get `updateChatParticipant`, the source of the Bot API's
//! `my_chat_member` / `chat_member`.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChatId, Peer, UserId};
use crate::services::{service_messages, updates};
use crate::store::{self, chats::Member, chats::Role};

/// Basic group ids, like Telegram's newer ones.
const FIRST_CHAT_ID: i64 = 4_000_000_001;
const MAX_TITLE_LENGTH: usize = 128;

/// The chat, if it exists and isn't deactivated.
pub fn get(ctx: &Ctx<'_>, chat: ChatId) -> RpcResult<store::chats::Chat> {
    match get_any(ctx, chat)? {
        found if !found.deactivated => Ok(found),
        _ => Err(RpcError::bad_request("CHAT_ID_INVALID")),
    }
}

/// The chat, deactivated (upgraded) or not: its history and info stay readable.
pub fn get_any(ctx: &Ctx<'_>, chat: ChatId) -> RpcResult<store::chats::Chat> {
    store::chats::get(ctx.db, chat)?.ok_or_else(|| RpcError::bad_request("CHAT_ID_INVALID"))
}

pub fn create(
    ctx: &mut Ctx<'_>,
    creator: UserId,
    title: &str,
    users: &[UserId],
) -> RpcResult<ChatId> {
    let title = check_title(title)?;
    if ctx.account(creator)?.is_bot {
        return Err(RpcError::bad_request("BOT_METHOD_INVALID"));
    }
    for &user in users {
        check_can_join(ctx, user)?;
    }
    let id = ChatId(store::meta::next(ctx.db, "chat", FIRST_CHAT_ID)?);
    let date = ctx.now();
    store::chats::insert(
        ctx.db,
        &store::chats::Chat {
            id,
            title: title.clone(),
            creator,
            date,
            version: 1,
            deactivated: false,
            extra: serde_json::json!({}),
        },
    )?;
    let mut all = vec![creator];
    all.extend(users.iter().copied().filter(|&u| u != creator));
    for &user in &all {
        let role = if user == creator {
            Role::Creator
        } else {
            Role::Member
        };
        store::chats::add_member(
            ctx.db,
            id,
            &Member {
                user,
                role,
                inviter: creator,
                date,
            },
        )?;
    }
    let action = types::MessageActionChatCreate {
        title,
        users: all.iter().map(|u| u.0).collect(),
    };
    post(ctx, creator, id, action.into(), &all)?;
    for &user in all.iter().filter(|&&u| u != creator) {
        membership_changed(ctx, creator, id, user, None, None)?;
    }
    Ok(id)
}

pub fn add_member(ctx: &mut Ctx<'_>, actor: UserId, chat: ChatId, user: UserId) -> RpcResult<()> {
    get(ctx, chat)?;
    require_member(ctx, chat, actor)?;
    if store::chats::member(ctx.db, chat, user)?.is_some() {
        return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
    }
    check_can_join(ctx, user)?;
    set_removed(ctx, chat, user, false)?;
    let date = ctx.now();
    store::chats::add_member(
        ctx.db,
        chat,
        &Member {
            user,
            role: Role::Member,
            inviter: actor,
            date,
        },
    )?;
    store::chats::bump_version(ctx.db, chat)?;
    let action = types::MessageActionChatAddUser {
        users: vec![user.0],
    };
    post(ctx, actor, chat, action.into(), &member_ids(ctx, chat)?)?;
    membership_changed(ctx, actor, chat, user, None, None)
}

/// `user` joins through an invite link created by `inviter` (or, `via_request`, after
/// an admin approved their request).
pub fn join_by_link(
    ctx: &mut Ctx<'_>,
    user: UserId,
    chat: ChatId,
    inviter: UserId,
    invite: enums::ExportedChatInvite,
    via_request: bool,
) -> RpcResult<()> {
    get(ctx, chat)?;
    if store::chats::member(ctx.db, chat, user)?.is_some() {
        return Err(RpcError::bad_request("USER_ALREADY_PARTICIPANT"));
    }
    set_removed(ctx, chat, user, false)?;
    let date = ctx.now();
    store::chats::add_member(
        ctx.db,
        chat,
        &Member {
            user,
            role: Role::Member,
            inviter,
            date,
        },
    )?;
    store::chats::bump_version(ctx.db, chat)?;
    let action: enums::MessageAction = if via_request {
        types::MessageActionChatJoinedByRequest {}.into()
    } else {
        types::MessageActionChatJoinedByLink {
            inviter_id: inviter.0,
        }
        .into()
    };
    post(ctx, user, chat, action, &member_ids(ctx, chat)?)?;
    membership_changed(ctx, user, chat, user, None, Some(invite))
}

/// Remove `user` (or leave, when `actor == user`). The removed user still gets the
/// service message, then no more.
pub fn remove_member(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    chat: ChatId,
    user: UserId,
) -> RpcResult<()> {
    let found = get(ctx, chat)?;
    let Some(removed) = store::chats::member(ctx.db, chat, user)? else {
        return Err(RpcError::bad_request("USER_NOT_PARTICIPANT"));
    };
    if actor != user {
        let actor_member = require_member(ctx, chat, actor)?;
        let may = actor_member.role != Role::Member || removed.inviter == actor;
        if !may || user == found.creator {
            return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
        }
    }
    let recipients = member_ids(ctx, chat)?;
    store::chats::remove_member(ctx.db, chat, user)?;
    // Telegram shows a removed member `chatForbidden`, one who left the chat (`left`).
    if actor != user {
        set_removed(ctx, chat, user, true)?;
    }
    store::chats::bump_version(ctx.db, chat)?;
    let action = types::MessageActionChatDeleteUser { user_id: user.0 };
    post(ctx, actor, chat, action.into(), &recipients)?;
    membership_changed(ctx, actor, chat, user, Some(removed), None)
}

pub fn rename(ctx: &mut Ctx<'_>, actor: UserId, chat: ChatId, title: &str) -> RpcResult<()> {
    let found = get(ctx, chat)?;
    require_member(ctx, chat, actor)?;
    let title = check_title(title)?;
    if title == found.title {
        return Err(RpcError::bad_request("CHAT_NOT_MODIFIED"));
    }
    store::chats::set_title(ctx.db, chat, &title)?;
    let action = types::MessageActionChatEditTitle { title };
    post(ctx, actor, chat, action.into(), &member_ids(ctx, chat)?)
}

/// Make a member an admin of the group or a member again. Only the creator may.
pub fn set_admin(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    chat: ChatId,
    user: UserId,
    is_admin: bool,
) -> RpcResult<()> {
    let found = get(ctx, chat)?;
    if require_member(ctx, chat, actor)?.role != Role::Creator {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let Some(before) = store::chats::member(ctx.db, chat, user)? else {
        return Err(RpcError::bad_request("USER_NOT_PARTICIPANT"));
    };
    if user == found.creator {
        return Err(RpcError::bad_request("USER_CREATOR"));
    }
    let role = if is_admin { Role::Admin } else { Role::Member };
    if before.role == role {
        return Ok(());
    }
    store::chats::set_role(ctx.db, chat, user, role)?;
    let version = store::chats::bump_version(ctx.db, chat)?;
    for member in member_ids(ctx, chat)? {
        ctx.out.mention_chat(member, chat);
        updates::push_plain(
            ctx,
            member,
            types::UpdateChatParticipantAdmin {
                chat_id: chat.0,
                user_id: user.0,
                is_admin,
                version,
            }
            .into(),
            [user],
        );
    }
    membership_changed(ctx, actor, chat, user, Some(before), None)
}

/// The group's description: admins, or members when the defaults let them change info.
pub fn set_about(ctx: &mut Ctx<'_>, actor: UserId, chat: ChatId, about: &str) -> RpcResult<()> {
    let found = get(ctx, chat)?;
    let member = require_member(ctx, chat, actor)?;
    if member.role == Role::Member && default_banned(&found).change_info {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    if about.chars().count() > 255 {
        return Err(RpcError::bad_request("CHAT_ABOUT_TOO_LONG"));
    }
    if found
        .extra
        .get("about")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        == about
    {
        return Err(RpcError::bad_request("CHAT_ABOUT_NOT_MODIFIED"));
    }
    let mut extra = found.extra;
    extra["about"] = about.into();
    store::chats::set_extra(ctx.db, chat, &extra)?;
    Ok(())
}

/// What members may do by default, as the rights they lack (basic groups start with
/// nothing taken away).
pub fn default_banned(chat: &store::chats::Chat) -> types::ChatBannedRights {
    chat.extra
        .get("default_banned_rights")
        .and_then(|v| serde_json::from_value(v.clone()).ok())
        .unwrap_or(types::ChatBannedRights {
            until_date: i32::MAX,
            ..Default::default()
        })
}

pub fn set_default_banned(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    chat: ChatId,
    mut banned: types::ChatBannedRights,
) -> RpcResult<()> {
    let found = get(ctx, chat)?;
    if require_member(ctx, chat, actor)?.role == Role::Member {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    banned.until_date = i32::MAX;
    if banned == default_banned(&found) {
        return Err(RpcError::bad_request("CHAT_NOT_MODIFIED"));
    }
    let mut extra = found.extra;
    extra["default_banned_rights"] = serde_json::to_value(&banned).map_err(RpcError::internal)?;
    store::chats::set_extra(ctx.db, chat, &extra)?;
    let version = store::chats::bump_version(ctx.db, chat)?;
    for user in member_ids(ctx, chat)? {
        ctx.out.mention_chat(user, chat);
        crate::services::updates::push_plain(
            ctx,
            user,
            types::UpdateChatDefaultBannedRights {
                peer: Peer::Chat(chat).to_tl(),
                default_banned_rights: banned.clone().into(),
                version,
            }
            .into(),
            [],
        );
    }
    Ok(())
}

pub fn require_member(ctx: &Ctx<'_>, chat: ChatId, user: UserId) -> RpcResult<Member> {
    store::chats::member(ctx.db, chat, user)?
        .ok_or_else(|| RpcError::forbidden("CHAT_WRITE_FORBIDDEN"))
}

pub fn member_ids(ctx: &Ctx<'_>, chat: ChatId) -> RpcResult<Vec<UserId>> {
    Ok(store::chats::members(ctx.db, chat)?
        .into_iter()
        .map(|m| m.user)
        .collect())
}

/// The TL form of a member.
pub fn participant(member: &Member) -> enums::ChatParticipant {
    match member.role {
        Role::Creator => types::ChatParticipantCreator {
            user_id: member.user.0,
            rank: None,
        }
        .into(),
        Role::Admin => types::ChatParticipantAdmin {
            user_id: member.user.0,
            inviter_id: member.inviter.0,
            date: member.date,
            rank: None,
        }
        .into(),
        Role::Member => types::ChatParticipant {
            user_id: member.user.0,
            inviter_id: member.inviter.0,
            date: member.date,
            rank: None,
        }
        .into(),
    }
}

fn check_title(title: &str) -> RpcResult<String> {
    let title = title.trim();
    if title.is_empty() {
        return Err(RpcError::bad_request("CHAT_TITLE_EMPTY"));
    }
    Ok(title.chars().take(MAX_TITLE_LENGTH).collect())
}

/// Bots can be kept out of groups by their owner (BotFather's "Allow Groups?").
pub(crate) fn check_can_join(ctx: &Ctx<'_>, user: UserId) -> RpcResult<()> {
    let account = ctx.account(user)?;
    if account.is_deleted() {
        return Err(RpcError::bad_request("INPUT_USER_DEACTIVATED"));
    }
    let blocked = account.is_bot
        && account
            .extra
            .pointer("/bot/can_join_groups")
            .and_then(|v| v.as_bool())
            == Some(false);
    if blocked {
        return Err(RpcError::bad_request("BOT_GROUPS_BLOCKED"));
    }
    Ok(())
}

/// A service message in every recipient's box (own id, own pts).
fn post(
    ctx: &mut Ctx<'_>,
    from: UserId,
    chat: ChatId,
    action: enums::MessageAction,
    recipients: &[UserId],
) -> RpcResult<()> {
    let copies: Vec<(UserId, Peer)> = recipients.iter().map(|&r| (r, Peer::Chat(chat))).collect();
    service_messages::post(ctx, from, &copies, action, None)
}

/// Users a service action names.
pub fn action_users(action: &enums::MessageAction) -> Vec<UserId> {
    match action {
        enums::MessageAction::ChatCreate(a) => a.users.iter().map(|&u| UserId(u)).collect(),
        enums::MessageAction::ChatAddUser(a) => a.users.iter().map(|&u| UserId(u)).collect(),
        enums::MessageAction::ChatDeleteUser(a) => vec![UserId(a.user_id)],
        _ => Vec::new(),
    }
}

/// `updateChatParticipant` for the bot whose membership changed, and for admin bots.
fn membership_changed(
    ctx: &mut Ctx<'_>,
    actor: UserId,
    chat: ChatId,
    user: UserId,
    before: Option<Member>,
    invite: Option<enums::ExportedChatInvite>,
) -> RpcResult<()> {
    let after = store::chats::member(ctx.db, chat, user)?;
    let mut bots: Vec<UserId> = Vec::new();
    if ctx.account(user)?.is_bot {
        bots.push(user);
    }
    for member in store::chats::members(ctx.db, chat)? {
        if member.user != user && member.role != Role::Member && ctx.account(member.user)?.is_bot {
            bots.push(member.user);
        }
    }
    let date = ctx.now();
    for bot in bots {
        ctx.out.mention_chat(bot, chat);
        updates::push_qts(ctx, bot, [actor, user], |qts| {
            types::UpdateChatParticipant {
                chat_id: chat.0,
                date,
                actor_id: actor.0,
                user_id: user.0,
                prev_participant: before.as_ref().map(participant),
                new_participant: after.as_ref().map(participant),
                invite: invite.clone(),
                qts,
            }
            .into()
        })?;
    }
    Ok(())
}

/// Whether `user` was removed by someone else (and not added back).
pub fn was_removed(chat: &store::chats::Chat, user: UserId) -> bool {
    chat.extra
        .get("removed")
        .and_then(|v| v.as_array())
        .is_some_and(|list| list.iter().any(|v| v.as_i64() == Some(user.0)))
}

fn set_removed(ctx: &Ctx<'_>, chat: ChatId, user: UserId, removed: bool) -> RpcResult<()> {
    let Some(found) = store::chats::get(ctx.db, chat)? else {
        return Ok(());
    };
    let mut extra = found.extra;
    let mut list: Vec<i64> = extra
        .get("removed")
        .and_then(|v| v.as_array())
        .map(|l| l.iter().filter_map(|v| v.as_i64()).collect())
        .unwrap_or_default();
    list.retain(|&id| id != user.0);
    if removed {
        list.push(user.0);
    }
    extra["removed"] = list.into();
    store::chats::set_extra(ctx.db, chat, &extra)?;
    Ok(())
}
