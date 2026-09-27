//! `setMyCommands`

use simula_botapi_types::methods;
use simula_tl::{functions, types};

use crate::commands;
use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult, ErrorContext};

const MAX_COMMAND_LENGTH: usize = 32;
const MAX_DESCRIPTION_LENGTH: usize = 256;

pub async fn handle(ctx: BotCtx, params: methods::SetMyCommands) -> BotResult<bool> {
    let scope = commands::scope(&ctx, params.scope.as_deref()).await?;
    let lang_code = commands::language_code(params.language_code.as_deref())?;

    // tdlib's `set_commands`: trimmed, a leading "/" dropped, lengths checked. Which
    // characters are allowed is the server's call (BOT_COMMAND_INVALID).
    let mut list = Vec::with_capacity(params.commands.len());
    for command in params.commands {
        let name = command.command.trim();
        let name = name.strip_prefix('/').unwrap_or(name);
        if name.is_empty() {
            return Err(BotError::bad_request("command must be non-empty"));
        }
        if name.chars().count() > MAX_COMMAND_LENGTH {
            return Err(BotError::bad_request(format!(
                "command length must not exceed {MAX_COMMAND_LENGTH}"
            )));
        }
        let description = command.description.trim();
        if description.is_empty() {
            return Err(BotError::bad_request(
                "command description must be non-empty",
            ));
        }
        if description.chars().count() > MAX_DESCRIPTION_LENGTH {
            return Err(BotError::bad_request(format!(
                "command description length must not exceed {MAX_DESCRIPTION_LENGTH}"
            )));
        }
        list.push(
            types::BotCommand {
                ephemeral: command.is_ephemeral == Some(true),
                command: name.to_owned(),
                description: description.to_owned(),
            }
            .into(),
        );
    }
    ctx.call(
        functions::bots::SetBotCommands {
            scope,
            lang_code,
            commands: list,
        },
        ErrorContext::Plain,
    )
    .await
}
