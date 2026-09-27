//! `deleteWebhook` (webhooks themselves arrive with M2; polling libraries call this first).

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;

pub async fn handle(ctx: BotCtx, params: methods::DeleteWebhook) -> BotResult<bool> {
    if params.drop_pending_updates.unwrap_or(false) {
        let state = ctx.state();
        let mut queue = state.queue.lock().expect("queue lock");
        queue.confirm(i64::MAX);
    }
    Ok(true)
}
