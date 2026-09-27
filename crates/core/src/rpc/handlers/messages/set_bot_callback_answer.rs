//! `messages.setBotCallbackAnswer`

use simula_tl::functions;

use crate::ctx::Ctx;
use crate::error::RpcResult;
use crate::services::callbacks;

pub fn handle(
    ctx: &mut Ctx<'_>,
    req: functions::messages::SetBotCallbackAnswer,
) -> RpcResult<bool> {
    let bot = ctx.caller()?.account;
    callbacks::answer(
        ctx,
        bot,
        req.query_id,
        callbacks::Answer {
            alert: req.alert,
            message: req.message,
            url: req.url,
            cache_time: req.cache_time,
        },
    )?;
    Ok(true)
}
