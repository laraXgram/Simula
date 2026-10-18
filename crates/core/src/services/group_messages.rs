//! Messages in basic groups: one copy per member who gets it, each with its own id and
//! pts. Bots in privacy mode (the default) get only what Telegram sends them:
//! - commands meant for them (`/cmd@this_bot`), and general commands (`/cmd`) when they
//!   were the last bot to write in the group (or are its only bot, before any wrote);
//! - replies to their own messages;
//! - service messages (those go to everyone, see `services::chats`).
//!
//! Bots with privacy mode off, and admin bots, get every message.

use simula_tl::enums;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::ids::{ChatId, Peer, UserId};
use crate::services::chats;
use crate::services::messages::{self, Outgoing, Prepared, Sent};
use crate::store::{self, accounts::Account, chats::Role};

pub fn send(ctx: &mut Ctx<'_>, from: UserId, chat: ChatId, out: Outgoing) -> RpcResult<Sent> {
    let found = chats::get(ctx, chat)?;
    chats::require_member(ctx, chat, from)?;
    messages::check_text(&out.text)?;
    let members = store::chats::members(ctx.db, chat)?;
    let accounts: Vec<(Account, Role)> = members
        .iter()
        .map(|m| Ok((ctx.account(m.user)?, m.role)))
        .collect::<RpcResult<_>>()?;
    let has_bot = accounts.iter().any(|(a, _)| a.is_bot);
    let prepared = messages::prepare(ctx, from, Peer::Chat(chat), out, has_bot)?;
    let bots: Vec<UserId> = accounts
        .iter()
        .filter(|(a, _)| a.is_bot)
        .map(|(a, _)| a.id)
        .collect();
    // General commands go to the last bot that wrote, or to the only bot before any did.
    let last_bot = found
        .extra
        .get("last_bot")
        .and_then(|v| v.as_i64())
        .map(UserId)
        .filter(|bot| bots.contains(bot))
        .or_else(|| (bots.len() == 1).then(|| bots[0]));

    let copies: Vec<(UserId, Peer)> = accounts
        .iter()
        .filter(|(account, role)| {
            account.id == from
                || !account.is_bot
                || bot_gets(account, *role != Role::Member, &prepared, last_bot)
        })
        .map(|(account, _)| (account.id, Peer::Chat(chat)))
        .collect();
    let (sent, _) = messages::deliver(ctx, from, &prepared, &copies)?;

    if ctx.account(from)?.is_bot {
        let mut extra = found.extra;
        extra["last_bot"] = from.0.into();
        store::chats::set_extra(ctx.db, chat, &extra)?;
    }
    Ok(sent)
}

/// Whether a bot in a group gets a message: all of them with privacy mode off or as an
/// admin, otherwise only those meant for it.
pub(crate) fn bot_gets(
    bot: &Account,
    admin: bool,
    prepared: &Prepared,
    last_bot: Option<UserId>,
) -> bool {
    let privacy = bot
        .extra
        .pointer("/bot/privacy_mode")
        .and_then(|v| v.as_bool())
        != Some(false);
    !privacy || admin || is_meant_for(prepared, bot, last_bot)
}

/// Whether a message is "meant for" a bot in privacy mode.
fn is_meant_for(prepared: &Prepared, bot: &Account, last_bot: Option<UserId>) -> bool {
    if prepared
        .reply_to
        .as_ref()
        .is_some_and(|t| t.author() == Some(bot.id))
    {
        return true;
    }
    let Some(command) = leading_command(prepared) else {
        return false;
    };
    match command.split_once('@') {
        Some((_, target)) => bot
            .username
            .as_deref()
            .is_some_and(|name| name.eq_ignore_ascii_case(target)),
        None => last_bot == Some(bot.id),
    }
}

/// `/cmd` or `/cmd@bot` when the message starts with a bot command.
fn leading_command(prepared: &Prepared) -> Option<String> {
    let first = prepared.entities.iter().find(|e| e.offset() == 0)?;
    let enums::MessageEntity::BotCommand(command) = first else {
        return None;
    };
    let units: Vec<u16> = prepared.text().encode_utf16().collect();
    let end = (command.length.max(0) as usize).min(units.len());
    Some(String::from_utf16_lossy(&units[..end]))
}
