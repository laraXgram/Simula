//! `messages.startBot`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::Peer;
use crate::render;
use crate::services::{messages, peers};

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
    if peer != Peer::User(bot) {
        return Err(RpcError::not_implemented("starting bots in groups"));
    }
    let param = req.start_param;
    let valid = param.len() <= MAX_START_PARAM
        && param
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-');
    if !valid {
        return Err(RpcError::bad_request("START_PARAM_INVALID"));
    }
    let text = if param.is_empty() {
        "/start".to_owned()
    } else {
        format!("/start {param}")
    };
    let sent = messages::send_private(
        ctx,
        caller.account,
        bot,
        messages::Outgoing {
            text,
            random_id: Some(req.random_id),
            ..Default::default()
        },
    )?;
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
        chats: vec![],
        date: ctx.now(),
        seq: 0,
    }
    .into())
}
