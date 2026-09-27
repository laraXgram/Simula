//! What the `*MyCommands` methods share: the scope (telegram-bot-api's
//! `get_bot_command_scope` + `check_bot_command_scope`, then tdlib's
//! `BotCommandScope::get_bot_command_scope`) and tdlib's language code check.

use simula_botapi_types::types as bot;
use simula_core::Peer;
use simula_tl::{enums, types};

use crate::ctx::BotCtx;
use crate::error::{BotError, BotResult};
use crate::resolve;

pub async fn scope(
    ctx: &BotCtx,
    scope: Option<&bot::BotCommandScope>,
) -> BotResult<enums::BotCommandScope> {
    use bot::BotCommandScope as S;
    let Some(scope) = scope else {
        return Ok(enums::BotCommandScope::Default);
    };
    let (chat_id, kind) = match scope {
        S::Default(_) => return Ok(enums::BotCommandScope::Default),
        S::AllPrivateChats(_) => return Ok(enums::BotCommandScope::Users),
        S::AllGroupChats(_) => return Ok(enums::BotCommandScope::Chats),
        S::AllChatAdministrators(_) => return Ok(enums::BotCommandScope::ChatAdmins),
        S::Chat(s) => (&s.chat_id, Kind::Chat),
        S::ChatAdministrators(s) => (&s.chat_id, Kind::Admins),
        S::ChatMember(s) => {
            if s.user_id <= 0 {
                return Err(BotError::bad_request(
                    "can't parse BotCommandScope: Invalid user_id specified",
                ));
            }
            (&s.chat_id, Kind::Member(s.user_id))
        }
    };
    let chat = resolve::chat(ctx, chat_id).await?;
    if matches!(chat, Peer::User(_)) && !matches!(kind, Kind::Chat) {
        return Err(BotError::bad_request(
            "can't use specified scope in private chats",
        ));
    }
    let peer = ctx.input_peer(chat);
    Ok(match kind {
        Kind::Chat => types::BotCommandScopePeer { peer }.into(),
        Kind::Admins => types::BotCommandScopePeerAdmins { peer }.into(),
        Kind::Member(user_id) => types::BotCommandScopePeerUser {
            peer,
            user_id: ctx.input_user(user_id),
        }
        .into(),
    })
}

enum Kind {
    Chat,
    Admins,
    Member(i64),
}

/// tdlib's `validate_bot_language_code`.
pub fn language_code(code: Option<&str>) -> BotResult<String> {
    let code = code.unwrap_or("");
    let valid =
        code.is_empty() || (code.len() == 2 && code.bytes().all(|b| b.is_ascii_lowercase()));
    if !valid {
        return Err(BotError::bad_request("invalid language code specified"));
    }
    Ok(code.to_owned())
}
