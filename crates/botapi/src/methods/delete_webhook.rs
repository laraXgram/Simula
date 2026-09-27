//! `deleteWebhook`: `setWebhook` with no URL, as in telegram-bot-api.

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::webhook::{self, Request};

pub async fn handle(ctx: BotCtx, params: methods::DeleteWebhook) -> BotResult<bool> {
    let description = webhook::set(
        &ctx,
        Request {
            drop_pending_updates: params.drop_pending_updates == Some(true),
            ..Request::default()
        },
    )
    .await?;
    ctx.describe(description);
    Ok(true)
}
