//! `messages.startBot`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{chats, group_messages, messages, peers};
use crate::store;

/// Longest deep-link `start` parameter Telegram accepts.
const MAX_START_PARAM: usize = 64;

/// A deep link (`t.me/<bot>?start=<param>`) opened in the private chat with the bot:
/// the user sends `/start <param>`, as Telegram does.
pub fn handle(ctx: &mut Ctx<'_>, req: functions::messages::StartBot) -> RpcResult<enums::Updates> {
    let caller = ctx.caller()?;
    let bot = peers::resolve_input_user(ctx, caller.account, &req.bot)?;
    if !ctx.account(bot)?.is_bot {
        return Err(RpcError::bad_request("BOT_INVALID"));
    }
    let peer = peers::resolve_input_peer(ctx, caller.account, &req.peer)?;
    let param = req.start_param;
    let valid = param.len() <= MAX_START_PARAM
        && param
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if !valid {
        return Err(RpcError::bad_request("START_PARAM_INVALID"));
    }
    // In a group the bot is added first and the command names it: `/start@bot <param>`.
    let command = match peer {
        Peer::Chat(chat) => {
            if store::chats::member(ctx.db, chat, bot)?.is_none() {
                chats::add_member(ctx, caller.account, chat, bot)?;
            }
            let username = ctx.account(bot)?.username.unwrap_or_default();
            format!("/start@{username}")
        }
        Peer::User(user) if user == bot => "/start".to_owned(),
        _ => return Err(RpcError::not_implemented("starting bots in channels")),
    };
    let text = if param.is_empty() {
        command
    } else {
        format!("{command} {param}")
    };
    let out = messages::Outgoing {
        text,
        random_id: Some(req.random_id),
        ..Default::default()
    };
    let sent = match peer {
        Peer::Chat(chat) => group_messages::send(ctx, caller.account, chat, out)?,
        _ => messages::send_private(ctx, caller.account, bot, out)?,
    };
    let mentioned = ctx.out.users_for(caller.account);
    Ok(types::Updates {
        updates: vec![
            types::UpdateMessageID {
                id: sent.message.id,
                random_id: req.random_id,
            }
            .into(),
            sent.update,
        ],
        users: render::users(ctx, caller.account, &mentioned)?,
        chats: render::chats(ctx, caller.account, &ctx.out.chats_for(caller.account))?,
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
