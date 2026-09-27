//! `answerCallbackQuery`

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};

/// telegram-bot-api clamps `cache_time` to 30 days.
const MAX_CACHE_TIME: i64 = 30 * 24 * 60 * 60;

pub async fn handle(ctx: BotCtx, params: methods::AnswerCallbackQuery) -> BotResult<bool> {
    // `td::to_integer`: an id that doesn't parse is 0, which the server rejects.
    let query_id = params.callback_query_id.trim().parse().unwrap_or(0);
    let non_empty = |s: Option<String>| s.filter(|s| !s.is_empty());
    ctx.call(
        functions::messages::SetBotCallbackAnswer {
            alert: params.show_alert == Some(true),
            query_id,
            message: non_empty(params.text),
            url: non_empty(params.url),
            cache_time: params.cache_time.unwrap_or(0).clamp(0, MAX_CACHE_TIME) as i32,
        },
        ErrorContext::Plain,
    )
    .await?;
    Ok(true)
}
