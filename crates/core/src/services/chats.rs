//! Basic groups: creating them, adding and removing members, renaming. Every change
//! posts a service message to the members' boxes; bots whose membership changes (and
//! admin bots, for others) get `updateChatParticipant`, the source of the Bot API's
//! `my_chat_member` / `chat_member`.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChatId, Peer, UserId};
use crate::services::updates;
use crate::store::{self, chats::Member, chats::Role};

/// Basic group ids, like Telegram's newer ones.
const FIRST_CHAT_ID: i64 = 4_000_000_001;
const MAX_TITLE_LENGTH: usize = 128;

/// The chat, if it exists and isn't deactivated.
pub fn get(ctx: &Ctx<'_>, chat: ChatId) -> RpcResult<store::chats::Chat> {
    match store::chats::get(ctx.db, chat)? {
        Some(found) if !found.deactivated => Ok(found),
        _ => Err(RpcError::bad_request("CHAT_ID_INVALID")),
    }
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
        membership_changed(ctx, creator, id, user, None)?;
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
    membership_changed(ctx, actor, chat, user, None)
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
    store::chats::bump_version(ctx.db, chat)?;
    let action = types::MessageActionChatDeleteUser { user_id: user.0 };
    post(ctx, actor, chat, action.into(), &recipients)?;
    membership_changed(ctx, actor, chat, user, Some(removed))
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
fn check_can_join(ctx: &Ctx<'_>, user: UserId) -> RpcResult<()> {
    let account = ctx.account(user)?;
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
    let date = ctx.now();
    let global_id = store::meta::next(ctx.db, "global_message", 1)?;
    let peer = Peer::Chat(chat);
    let mut mentioned: Vec<UserId> = vec![from];
    mentioned.extend(action_users(&action));
    for &owner in recipients {
        let message = types::MessageService {
            out: owner == from,
            id: store::messages::next_id(ctx.db, owner)?,
            from_id: Some(Peer::User(from).to_tl()),
            peer_id: peer.to_tl(),
            date,
            action: action.clone(),
            ..Default::default()
        };
        store::messages::insert_service(ctx.db, owner, peer, global_id, &message)?;
        ctx.out.mention_chat(owner, chat);
        updates::push_pts(ctx, owner, mentioned.clone(), |pts, pts_count| {
            types::UpdateNewMessage {
                message: message.clone().into(),
                pts,
                pts_count,
            }
            .into()
        })?;
    }
    Ok(())
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
                invite: None,
                qts,
            }
            .into()
        })?;
    }
    Ok(())
}
