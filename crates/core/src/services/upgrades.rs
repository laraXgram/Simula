//! Upgrading a basic group to a supergroup (`messages.migrateChat`). The supergroup is
//! a new channel with the group's title, description, default permissions, members,
//! admins, invite links and join requests. The basic group is deactivated and points at
//! it (`chat.migrated_to`); its history stays in the members' boxes.
//!
//! Every member gets `messageActionChatMigrateTo` in the group, then
//! `messageActionChannelMigrateFrom` as the supergroup's first message — the Bot API's
//! `migrate_to_chat_id` and `migrate_from_chat_id`.

use simula_tl::{enums, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChannelId, ChatId, Peer, UserId};
use crate::render;
use crate::services::{channels, chats, service_messages, updates};
use crate::store::{
    self,
    channels::{Member, Status},
    chats::Role,
};

pub fn migrate(ctx: &mut Ctx<'_>, actor: UserId, chat: ChatId) -> RpcResult<ChannelId> {
    let found = chats::get(ctx, chat)?;
    if found.creator != actor || chats::require_member(ctx, chat, actor)?.role != Role::Creator {
        return Err(RpcError::bad_request("CHAT_ADMIN_REQUIRED"));
    }
    let members = store::chats::members(ctx.db, chat)?;
    let about = found
        .extra
        .get("about")
        .and_then(|v| v.as_str())
        .unwrap_or_default()
        .to_owned();
    let extra = serde_json::json!({
        "default_banned_rights": serde_json::to_value(chats::default_banned(&found))
            .map_err(RpcError::internal)?,
        "migrated_from_chat_id": chat.0,
    });
    let channel = channels::insert(ctx, actor, false, &found.title, &about, extra)?;

    // Basic-group admins become admins with the rights they held there (adding admins
    // stays the creator's).
    let date = ctx.now();
    for member in members.iter().filter(|m| m.role != Role::Creator) {
        let next = match member.role {
            Role::Admin => Member {
                status: Status::Admin,
                promoted_by: Some(actor),
                admin_rights: Some(render::full_admin_rights()),
                ..Member::new(member.user, Status::Admin, member.inviter, date)
            },
            _ => Member::new(member.user, Status::Member, member.inviter, date),
        };
        store::channels::put_member(ctx.db, channel, &next)?;
    }

    // Links and pending requests now lead to the supergroup.
    let (from, to) = (
        Peer::Chat(chat).bot_api_id(),
        Peer::Channel(channel).bot_api_id(),
    );
    ctx.db
        .execute("UPDATE invites SET peer = ?2 WHERE peer = ?1", [from, to])?;
    ctx.db.execute(
        "UPDATE join_requests SET peer = ?2 WHERE peer = ?1",
        [from, to],
    )?;

    let mut extra = found.extra.clone();
    extra["migrated_to"] = channel.0.into();
    store::chats::set_extra(ctx.db, chat, &extra)?;
    store::chats::deactivate(ctx.db, chat)?;

    let recipients: Vec<UserId> = members.iter().map(|m| m.user).collect();
    let copies: Vec<(UserId, Peer)> = recipients.iter().map(|&u| (u, Peer::Chat(chat))).collect();
    service_messages::post(
        ctx,
        actor,
        &copies,
        types::MessageActionChatMigrateTo {
            channel_id: channel.0,
        }
        .into(),
        None,
    )?;
    for &user in &recipients {
        ctx.out.mention_channel(user, channel);
        updates::push_plain(
            ctx,
            user,
            types::UpdateChannel {
                channel_id: channel.0,
            }
            .into(),
            [],
        );
    }

    let created = channels::get(ctx, channel)?;
    channels::post_service(
        ctx,
        &created,
        actor,
        types::MessageActionChannelMigrateFrom {
            title: found.title.clone(),
            chat_id: chat.0,
        }
        .into(),
        &[],
    )?;
    for &user in &recipients {
        ctx.out.mention_chat(user, chat);
    }
    Ok(channel)
}

/// The supergroup a basic group was upgraded to, if it was.
pub fn migrated_to(chat: &store::chats::Chat) -> Option<ChannelId> {
    chat.extra
        .get("migrated_to")
        .and_then(|v| v.as_i64())
        .map(ChannelId)
}

/// The basic group a supergroup was upgraded from, if it was.
pub fn migrated_from(channel: &store::channels::Channel) -> Option<ChatId> {
    channel
        .extra
        .get("migrated_from_chat_id")
        .and_then(|v| v.as_i64())
        .map(ChatId)
}

/// `chat.migrated_to` as `viewer` would address the supergroup.
pub fn input_channel(ctx: &Ctx<'_>, viewer: UserId, channel: ChannelId) -> enums::InputChannel {
    types::InputChannel {
        channel_id: channel.0,
        access_hash: ctx.access_hash(viewer, Peer::Channel(channel)),
    }
    .into()
}
