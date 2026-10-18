//! `channels.getFullChannel`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::Peer;
use crate::render;
use crate::services::{channels, commands, peers};
use crate::store::{self, channels::Status};

/// A supergroup or channel with its description, counts, the caller's read state and
/// its bots' commands for the caller.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::channels::GetFullChannel,
) -> RpcResult<enums::messages::ChatFull> {
    let caller = ctx.caller()?.account;
    let id = peers::resolve_input_channel(ctx, caller, &req.channel)?;
    let channel = channels::get(ctx, id)?;
    let me = channels::require_reader(ctx, &channel, caller)?;
    let admin = me.as_ref().is_some_and(|m| m.status.is_admin());
    let everyone = store::channels::members(ctx.db, id)?;
    let count = |status: Status| everyone.iter().filter(|m| m.status == status).count() as i32;
    let members: Vec<_> = everyone.iter().filter(|m| m.status.is_member()).collect();
    let mut users = BTreeSet::new();
    let mut bot_info = Vec::new();
    for member in &members {
        if member.status.is_admin() {
            users.insert(member.user);
        }
        if ctx.account(member.user)?.is_bot {
            users.insert(member.user);
            let commands =
                commands::for_group_chat(ctx, member.user, Peer::Channel(id), caller, admin)?;
            bot_info.push(
                types::BotInfo {
                    user_id: Some(member.user.0),
                    commands: Some(commands),
                    ..Default::default()
                }
                .into(),
            );
        }
    }
    let peer = Peer::Channel(id);
    let (read_inbox, read_outbox) = store::dialogs::read_state(ctx.db, caller, peer)?;
    let full = types::ChannelFull {
        can_view_participants: !channel.broadcast || admin,
        can_set_username: me.as_ref().is_some_and(|m| m.status == Status::Creator),
        id: id.0,
        about: channel.about.clone(),
        participants_count: Some(members.len() as i32),
        admins_count: Some(members.iter().filter(|m| m.status.is_admin()).count() as i32),
        kicked_count: Some(count(Status::Banned)),
        banned_count: Some(count(Status::Restricted)),
        read_inbox_max_id: read_inbox,
        read_outbox_max_id: read_outbox,
        unread_count: if me.as_ref().is_some_and(|m| m.status.is_member()) {
            store::channels::unread_count(ctx.db, id, caller, read_inbox)?
        } else {
            0
        },
        notify_settings: types::PeerNotifySettings::default().into(),
        bot_info,
        pts: channel.pts,
        pinned_msg_id: store::channels::pinned_ids(ctx.db, id)?.first().copied(),
        ..Default::default()
    };
    Ok(types::messages::ChatFull {
        full_chat: full.into(),
        chats: vec![render::channel(ctx, caller, &channel)?],
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
