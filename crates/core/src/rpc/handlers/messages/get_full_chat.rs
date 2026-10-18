//! `messages.getFullChat`

use std::collections::BTreeSet;

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{ChatId, Peer};
use crate::render;
use crate::services::{chats, commands};
use crate::store;

/// A basic group with its members and its bots' commands for the caller (only for
/// members, as on Telegram).
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetFullChat,
) -> RpcResult<enums::messages::ChatFull> {
    let caller = ctx.caller()?.account;
    let chat = chats::get(ctx, ChatId(req.chat_id))?;
    let members = store::chats::members(ctx.db, chat.id)?;
    if !members.iter().any(|m| m.user == caller) {
        return Err(RpcError::bad_request("CHAT_ID_INVALID"));
    }
    let users: BTreeSet<_> = members.iter().map(|m| m.user).collect();
    let admin = members
        .iter()
        .any(|m| m.user == caller && m.role != store::chats::Role::Member);
    let mut bot_info = Vec::new();
    for member in &members {
        if ctx.account(member.user)?.is_bot {
            let commands =
                commands::for_group_chat(ctx, member.user, Peer::Chat(chat.id), caller, admin)?;
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
    let full = types::ChatFull {
        id: chat.id.0,
        about: chat
            .extra
            .get("about")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_owned(),
        participants: types::ChatParticipants {
            chat_id: chat.id.0,
            participants: members.iter().map(chats::participant).collect(),
            version: chat.version,
        }
        .into(),
        notify_settings: types::PeerNotifySettings::default().into(),
        bot_info: Some(bot_info),
        exported_invite: crate::services::invites::primary(ctx, caller, Peer::Chat(chat.id))?,
        pinned_msg_id: store::messages::pinned_ids(ctx.db, caller, Peer::Chat(chat.id))?
            .first()
            .copied(),
        ..Default::default()
    };
    Ok(types::messages::ChatFull {
        full_chat: full.into(),
        chats: vec![render::chat(ctx, caller, &chat)?],
        users: render::users(ctx, caller, &users)?,
    }
    .into())
}
