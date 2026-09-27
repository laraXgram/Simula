//! Bot command lists: one per scope (everyone, private chats, groups, a chat, …) and
//! language. Only bots have them; Telegram checks every command and description.

use simula_tl::enums;

use crate::ctx::Ctx;
use crate::error::{RpcError, RpcResult};
use crate::ids::{Peer, UserId};
use crate::services::peers;
use crate::store;

const MAX_COMMANDS: usize = 100;
const MAX_COMMAND_LENGTH: usize = 32;
const MAX_DESCRIPTION_LENGTH: usize = 256;

/// The storage key of a scope, checking the chats and users it names.
pub fn scope_key(ctx: &Ctx<'_>, bot: UserId, scope: &enums::BotCommandScope) -> RpcResult<String> {
    use enums::BotCommandScope as S;
    let peer = |input: &enums::InputPeer| -> RpcResult<Peer> {
        peers::resolve_input_peer(ctx, bot, input)
    };
    Ok(match scope {
        S::Default => "default".into(),
        S::Users => "users".into(),
        S::Chats => "chats".into(),
        S::ChatAdmins => "chat_admins".into(),
        S::Peer(s) => format!("peer:{}", peer(&s.peer)?.bot_api_id()),
        S::PeerAdmins(s) => format!("peer_admins:{}", peer(&s.peer)?.bot_api_id()),
        S::PeerUser(s) => {
            let user = peers::resolve_input_user(ctx, bot, &s.user_id)?;
            format!("peer_user:{}:{}", peer(&s.peer)?.bot_api_id(), user.0)
        }
    })
}

pub fn set(
    ctx: &Ctx<'_>,
    bot: UserId,
    scope: &enums::BotCommandScope,
    lang_code: &str,
    commands: Vec<enums::BotCommand>,
) -> RpcResult<()> {
    require_bot(ctx, bot)?;
    check_lang_code(lang_code)?;
    if commands.len() > MAX_COMMANDS {
        return Err(RpcError::bad_request("BOT_COMMANDS_TOO_MUCH"));
    }
    for enums::BotCommand::BotCommand(command) in &commands {
        let valid = !command.command.is_empty()
            && command.command.len() <= MAX_COMMAND_LENGTH
            && command
                .command
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_');
        if !valid {
            return Err(RpcError::bad_request("BOT_COMMAND_INVALID"));
        }
        let length = command.description.chars().count();
        if length == 0 || length > MAX_DESCRIPTION_LENGTH {
            return Err(RpcError::bad_request("BOT_COMMAND_DESCRIPTION_INVALID"));
        }
    }
    let key = scope_key(ctx, bot, scope)?;
    store::bot_commands::set(ctx.db, bot, &key, lang_code, &commands)?;
    Ok(())
}

pub fn get(
    ctx: &Ctx<'_>,
    bot: UserId,
    scope: &enums::BotCommandScope,
    lang_code: &str,
) -> RpcResult<Vec<enums::BotCommand>> {
    require_bot(ctx, bot)?;
    check_lang_code(lang_code)?;
    let key = scope_key(ctx, bot, scope)?;
    Ok(store::bot_commands::get(ctx.db, bot, &key, lang_code)?)
}

pub fn reset(
    ctx: &Ctx<'_>,
    bot: UserId,
    scope: &enums::BotCommandScope,
    lang_code: &str,
) -> RpcResult<()> {
    set(ctx, bot, scope, lang_code, Vec::new())
}

fn require_bot(ctx: &Ctx<'_>, account: UserId) -> RpcResult<()> {
    if ctx.account(account)?.is_bot {
        Ok(())
    } else {
        Err(RpcError::bad_request("USER_BOT_REQUIRED"))
    }
}

/// "" or a two-letter ISO 639-1 code.
fn check_lang_code(code: &str) -> RpcResult<()> {
    let valid =
        code.is_empty() || (code.len() == 2 && code.bytes().all(|b| b.is_ascii_lowercase()));
    if valid {
        Ok(())
    } else {
        Err(RpcError::bad_request("LANG_CODE_INVALID"))
    }
}
