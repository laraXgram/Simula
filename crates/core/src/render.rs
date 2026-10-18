//! Domain rows → TL objects, as a specific viewer sees them (access hashes, `self`,
//! phone visibility, bot-only fields).

use std::collections::BTreeSet;

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::{ChannelId, ChatId, Peer, UserId};
use crate::services::rights;
use crate::store::{self, accounts::Account, channels::Status};

pub fn user(ctx: &Ctx<'_>, viewer: &Account, account: &Account) -> enums::User {
    let is_self = viewer.id == account.id;
    let bot_setting = |key: &str| {
        account
            .extra
            .pointer(&format!("/bot/{key}"))
            .and_then(|v| v.as_bool())
    };
    types::User {
        self_: is_self,
        bot: account.is_bot,
        premium: account.is_premium,
        // Privacy mode off = the bot sees all group messages.
        bot_chat_history: account.is_bot && bot_setting("privacy_mode") == Some(false),
        bot_nochats: account.is_bot && bot_setting("can_join_groups") == Some(false),
        id: account.id.0,
        access_hash: Some(ctx.access_hash(viewer.id, account.id.into())),
        first_name: Some(account.first_name.clone()),
        last_name: account.last_name.clone(),
        username: account.username.clone(),
        phone: if is_self { account.phone.clone() } else { None },
        bot_info_version: account.is_bot.then_some(1),
        // Bots learn their users' language; other users don't.
        lang_code: if viewer.is_bot {
            account.lang_code.clone()
        } else {
            None
        },
        ..Default::default()
    }
    .into()
}

/// Render several users for one viewer, skipping ids that no longer exist.
pub fn users(ctx: &Ctx<'_>, viewer: UserId, ids: &BTreeSet<UserId>) -> RpcResult<Vec<enums::User>> {
    let viewer = ctx.account(viewer)?;
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        if let Ok(account) = ctx.account(id) {
            out.push(user(ctx, &viewer, &account));
        }
    }
    Ok(out)
}

/// A basic group as `viewer` sees it: whether they created it or left it, their admin
/// rights, the member count.
pub fn chat(ctx: &Ctx<'_>, viewer: UserId, chat: &store::chats::Chat) -> RpcResult<enums::Chat> {
    let members = store::chats::members(ctx.db, chat.id)?;
    let me = members.iter().find(|m| m.user == viewer);
    let removed = crate::services::chats::was_removed(chat, viewer);
    if me.is_none() && (removed || (chat.creator != viewer && !was_member(ctx, viewer, chat.id)?)) {
        return Ok(types::ChatForbidden {
            id: chat.id.0,
            title: chat.title.clone(),
        }
        .into());
    }
    let admin = me.is_some_and(|m| m.role != store::chats::Role::Member);
    Ok(types::Chat {
        creator: chat.creator == viewer,
        left: me.is_none(),
        deactivated: chat.deactivated,
        id: chat.id.0,
        title: chat.title.clone(),
        photo: enums::ChatPhoto::Empty,
        participants_count: members.len() as i32,
        date: chat.date,
        version: chat.version,
        admin_rights: admin.then(|| full_admin_rights().into()),
        default_banned_rights: Some(crate::services::chats::default_banned(chat).into()),
        ..Default::default()
    }
    .into())
}

/// Several basic groups for one viewer, skipping ids that don't exist.
pub fn chats(ctx: &Ctx<'_>, viewer: UserId, ids: &BTreeSet<ChatId>) -> RpcResult<Vec<enums::Chat>> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        if let Some(found) = store::chats::get(ctx.db, id)? {
            out.push(chat(ctx, viewer, &found)?);
        }
    }
    Ok(out)
}

/// Whether `viewer` has the chat in its box (a former member still sees its history).
fn was_member(ctx: &Ctx<'_>, viewer: UserId, chat: ChatId) -> RpcResult<bool> {
    Ok(ctx.db.query_row(
        "SELECT EXISTS (SELECT 1 FROM messages WHERE owner_id = ?1 AND peer = ?2)",
        rusqlite::params![viewer.0, crate::ids::Peer::Chat(chat).bot_api_id()],
        |row| row.get(0),
    )?)
}

/// Basic group admins have every right that applies to basic groups.
pub fn full_admin_rights() -> types::ChatAdminRights {
    types::ChatAdminRights {
        change_info: true,
        delete_messages: true,
        ban_users: true,
        invite_users: true,
        pin_messages: true,
        manage_call: true,
        other: true,
        ..Default::default()
    }
}

/// A supergroup or channel as `viewer` sees it: their status and rights in it, the
/// access hash they use. Who was banned sees `channelForbidden`.
pub fn channel(
    ctx: &Ctx<'_>,
    viewer: UserId,
    channel: &store::channels::Channel,
) -> RpcResult<enums::Chat> {
    let me = store::channels::member(ctx.db, channel.id, viewer)?;
    let access_hash = ctx.access_hash(viewer, Peer::Channel(channel.id));
    let banned = me.as_ref().filter(|m| m.status == Status::Banned);
    if channel.deleted || banned.is_some() {
        let until = banned
            .and_then(|m| m.banned_rights.as_ref())
            .map(|r| r.until_date)
            .filter(|&until| !rights::is_forever(until));
        return Ok(types::ChannelForbidden {
            broadcast: channel.broadcast,
            megagroup: !channel.broadcast,
            monoforum: false,
            id: channel.id.0,
            access_hash,
            title: channel.title.clone(),
            until_date: until,
        }
        .into());
    }
    let status = me.as_ref().map(|m| m.status);
    let count = store::channels::current_members(ctx.db, channel.id)?.len() as i32;
    Ok(types::Channel {
        creator: status == Some(Status::Creator),
        left: !status.is_some_and(Status::is_member),
        broadcast: channel.broadcast,
        megagroup: !channel.broadcast,
        id: channel.id.0,
        access_hash: Some(access_hash),
        title: channel.title.clone(),
        username: channel.username.clone(),
        photo: enums::ChatPhoto::Empty,
        date: me.as_ref().map_or(channel.date, |m| m.date),
        admin_rights: me
            .as_ref()
            .and_then(|m| rights::admin_rights(channel, m))
            .map(Into::into),
        banned_rights: me
            .as_ref()
            .filter(|m| m.status == Status::Restricted)
            .and_then(|m| m.banned_rights.clone())
            .map(Into::into),
        default_banned_rights: (!channel.broadcast).then(|| rights::default_banned(channel).into()),
        participants_count: Some(count),
        ..Default::default()
    }
    .into())
}

/// Several channels for one viewer, skipping ids that don't exist.
pub fn channels(
    ctx: &Ctx<'_>,
    viewer: UserId,
    ids: &BTreeSet<ChannelId>,
) -> RpcResult<Vec<enums::Chat>> {
    let mut out = Vec::with_capacity(ids.len());
    for &id in ids {
        if let Some(found) = store::channels::get(ctx.db, id)? {
            out.push(channel(ctx, viewer, &found)?);
        }
    }
    Ok(out)
}

/// Basic groups and channels together, as `chats` vectors carry them.
pub fn all_chats(
    ctx: &Ctx<'_>,
    viewer: UserId,
    chat_ids: &BTreeSet<ChatId>,
    channel_ids: &BTreeSet<ChannelId>,
) -> RpcResult<Vec<enums::Chat>> {
    let mut out = chats(ctx, viewer, chat_ids)?;
    out.extend(channels(ctx, viewer, channel_ids)?);
    Ok(out)
}

/// The updates a call produced for `account`, as the call's `Updates` result.
pub fn own_updates(ctx: &Ctx<'_>, account: UserId) -> RpcResult<enums::Updates> {
    Ok(types::Updates {
        updates: ctx.out.updates_for(account),
        users: users(ctx, account, &ctx.out.users_for(account))?,
        chats: all_chats(
            ctx,
            account,
            &ctx.out.chats_for(account),
            &ctx.out.channels_for(account),
        )?,
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
