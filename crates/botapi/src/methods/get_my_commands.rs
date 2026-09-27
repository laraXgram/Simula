//! `getMyCommands`

use simula_botapi_types::{methods, types as bot};
use simula_tl::{enums, functions};

use crate::commands;
use crate::ctx::BotCtx;
use crate::error::{BotResult, ErrorContext};

pub async fn handle(
    ctx: BotCtx,
    params: methods::GetMyCommands,
) -> BotResult<Vec<bot::BotCommand>> {
    let scope = commands::scope(&ctx, params.scope.as_deref()).await?;
    let lang_code = commands::language_code(params.language_code.as_deref())?;
    let list = ctx
        .call(
            functions::bots::GetBotCommands { scope, lang_code },
            ErrorContext::Plain,
        )
        .await?;
    Ok(list
        .into_iter()
        .map(|enums::BotCommand::BotCommand(c)| bot::BotCommand {
            command: c.command,
            description: c.description,
            is_ephemeral: c.ephemeral.then_some(true),
        })
        .collect())
}
