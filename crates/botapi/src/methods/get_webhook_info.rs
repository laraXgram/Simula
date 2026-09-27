//! `getWebhookInfo`

use simula_botapi_types::{methods, types as bot};

use crate::ctx::BotCtx;
use crate::error::BotResult;
use crate::webhook;

pub async fn handle(ctx: BotCtx, _params: methods::GetWebhookInfo) -> BotResult<bot::WebhookInfo> {
    Ok(webhook::info(&ctx))
}
