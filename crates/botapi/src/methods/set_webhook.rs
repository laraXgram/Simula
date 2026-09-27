//! `setWebhook`

use simula_botapi_types::methods;

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::webhook::{self, Request};

pub async fn handle(ctx: BotCtx, params: methods::SetWebhook) -> BotResult<bool> {
    let description = webhook::set(
        &ctx,
        Request {
            url: params.url,
            ip_address: params.ip_address,
            max_connections: params.max_connections,
            allowed_updates: params.allowed_updates,
            drop_pending_updates: params.drop_pending_updates == Some(true),
            secret_token: params.secret_token,
        },
    )
    .await?;
    ctx.describe(description);
    Ok(true)
}
