//! `deleteMyCommands`

use simula_botapi_types::methods;
use simula_tl::functions;

use crate::commands;
use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};

pub async fn handle(ctx: BotCtx, params: methods::DeleteMyCommands) -> BotResult<bool> {
    let scope = commands::scope(&ctx, params.scope.as_deref()).await?;
    let lang_code = commands::language_code(params.language_code.as_deref())?;
    ctx.call(
        functions::bots::ResetBotCommands { scope, lang_code },
        ErrorContext::Plain,
    )
    .await
}
