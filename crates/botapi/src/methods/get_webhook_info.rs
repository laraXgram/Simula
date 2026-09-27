//! `getWebhookInfo`

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;

pub async fn handle(ctx: BotCtx, _params: methods::GetWebhookInfo) -> BotResult<bot::WebhookInfo> {
    let pending = ctx.state().queue.lock().expect("queue lock").len();
    Ok(bot::WebhookInfo {
        url: String::new(),
        pending_update_count: pending as i64,
        ..Default::default()
    })
}
