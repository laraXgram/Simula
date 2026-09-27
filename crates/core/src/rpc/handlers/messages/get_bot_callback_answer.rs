//! `messages.getBotCallbackAnswer`

use simula_tl::{enums, functions, types};

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::services::{callbacks, peers};

/// Presses a callback button; the result is the bot's answer, which the call waits for.
pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::GetBotCallbackAnswer,
) -> RpcResult<enums::messages::BotCallbackAnswer> {
    if req.game || req.password.is_some() {
        return Err(RpcError::not_implemented(
            "game and password callback buttons",
        ));
    }
    let caller = ctx.caller()?.account;
    let peer = peers::resolve_input_peer(ctx, caller, &req.peer)?;
    let data = req
        .data
        .ok_or_else(|| RpcError::bad_request("DATA_INVALID"))?;
    callbacks::press(ctx, caller, peer, req.msg_id, &data)?;
    // Replaced by the bot's answer.
    Ok(types::messages::BotCallbackAnswer::default().into())
}
